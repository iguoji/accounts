/**
 * Redis 数据访问层。
 *
 * 数据结构映射：
 *  - proxy:{ip}:{port}          Hash   单个代理的状态（status / consecutive_fail / checked_at）
 *  - check_queue                ZSet   调度队列，score = next_check_at（毫秒时间戳）
 *  - available:{type}           Set    按协议分组的可用代理集合（type = 1/2/3/4）
 *  - known_proxies              Set    所有已采集入库的代理（用于采集去重）
 *  - dead_pool                  Set    已软删的代理（连续失败达上限）
 *  - stats                      Hash   统计计数器（total / available / dead / checked / unchecked）
 *  - meta:last_collect          String 最近一次采集完成时间
 *  - meta:collect_count         String 累计采集次数
 *  - meta:last_check            String 最近一次测活时间
 *
 * 时间存储约定：
 *  - check_queue 的 score 与 checked_at 使用毫秒级整数（Date.now()），便于与"当前时间"直接比较
 *  - meta:last_collect / meta:last_check 为 "YYYY-MM-DD HH:MM:SS" 文本（仅供展示）
 */
import { createClient, type RedisClientType } from 'redis';
import { logger } from './logger.js';
import { parseAddrKey, TYPE_TO_NAME } from './types.js';
import type { ProxyAddr } from './types.js';
import type { AppConfig } from './config.js';

/** "YYYY-MM-DD HH:MM:SS"（本地时间）文本格式。 */
export function fmtTime(d: Date = new Date()): string {
  const p = (n: number) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
}

export interface AvailableItem {
  ip: string;
  port: number;
  protocols: string[];
  /** 可选的认证用户名（源行含 username:password@ 时存在） */
  username?: string;
  /** 可选的认证密码 */
  password?: string;
}

export interface StatsItem {
  // 数据库分段（可加法对账）：proxy_total = proxy_available + proxy_unchecked + proxy_cooldown
  proxy_total: number;
  proxy_available: number;
  proxy_unchecked: number;
  proxy_cooldown: number;
  // 已软删（不计入上面等式，单独看）
  proxy_dead: number;
  // 运行时读数
  proxy_checking: number;
  // 时间与累计计数
  last_collect_at: string | null;
  last_check_at: string | null;
  collect_count: number;
  check_count: number;
}

const KNOWN = 'known_proxies';
const QUEUE = 'check_queue';
const DEAD = 'dead_pool';
const STATS = 'stats';
const ALL_AVAIL_KEYS = ['available:1', 'available:2', 'available:3', 'available:4'];
const UPSERT_BATCH_SIZE = 1000;
const STATS_SCAN_BATCH_SIZE = 1000;

/**
 * 原子写入一个采集地址，并同步维护统计计数。
 * 返回 1=新增、2=复活、0=已存在且无需处理。
 */
const UPSERT_ADDRESS_LUA = `
  local addr = ARGV[1]
  local proxy_key = ARGV[2]
  local username = ARGV[3]
  local password = ARGV[4]
  local ip = ARGV[5]
  local port = ARGV[6]
  local has_auth = ARGV[7]

  if redis.call('SISMEMBER', KEYS[1], addr) == 1 then
    -- 历史数据可能没有结构化地址字段。每次重新采集命中时顺便补齐，
    -- 使调度和 API 不再依赖对 member 字符串进行有歧义的拆分。
    redis.call('HSET', proxy_key, 'ip', ip, 'port', port)
    if has_auth == '1' then
      redis.call('HSET', proxy_key, 'username', username, 'password', password)
    else
      redis.call('HDEL', proxy_key, 'username', 'password')
    end
    if redis.call('SISMEMBER', KEYS[2], addr) == 0 then
      return 0
    end

    redis.call('SREM', KEYS[2], addr)
    redis.call('HSET', proxy_key,
      'status', '0',
      'consecutive_fail', '0',
      'checked_at', '',
      'ip', ip,
      'port', port)
    redis.call('ZADD', KEYS[3], 0, addr)
    redis.call('HINCRBY', KEYS[4], 'dead', -1)
    redis.call('HINCRBY', KEYS[4], 'unchecked', 1)
    redis.call('HINCRBY', KEYS[4], 'total', 1)
    return 2
  end

  redis.call('SADD', KEYS[1], addr)
  redis.call('HSET', proxy_key,
    'status', '0',
    'consecutive_fail', '0',
    'checked_at', '',
    'ip', ip,
    'port', port)
  if has_auth == '1' then
    redis.call('HSET', proxy_key, 'username', username, 'password', password)
  end
  redis.call('ZADD', KEYS[3], 0, addr)
  redis.call('HINCRBY', KEYS[4], 'unchecked', 1)
  redis.call('HINCRBY', KEYS[4], 'total', 1)
  return 1
`;

/** 原子写回测活结果，并根据旧状态同步迁移统计计数。 */
const FINALIZE_PROBE_LUA = `
  local addr = ARGV[1]
  local proxy_key = ARGV[2]
  local ok = ARGV[3] == '1'
  local consecutive_fail = tonumber(ARGV[4])
  local max_fail = tonumber(ARGV[5])
  local now = ARGV[6]
  local next_check = ARGV[7]
  local last_check = ARGV[8]

  local was_dead = redis.call('SISMEMBER', KEYS[2], addr) == 1
  local old_status = redis.call('HGET', proxy_key, 'status') or '0'
  local old_checked_at = redis.call('HGET', proxy_key, 'checked_at') or ''
  local old_bucket
  if was_dead then
    old_bucket = 'dead'
  elseif old_status == '1' then
    old_bucket = 'available'
  elseif old_checked_at == '' or old_checked_at == '0' then
    old_bucket = 'unchecked'
  else
    old_bucket = 'cooldown'
  end

  for i = 1, 4 do
    if ARGV[8 + i] == '1' then
      redis.call('SADD', KEYS[3 + i], addr)
    else
      redis.call('SREM', KEYS[3 + i], addr)
    end
  end

  local new_bucket
  if ok then
    redis.call('HSET', proxy_key, 'status', '1', 'consecutive_fail', '0', 'checked_at', now)
    redis.call('ZADD', KEYS[1], next_check, addr)
    redis.call('SREM', KEYS[2], addr)
    new_bucket = 'available'
  else
    redis.call('HSET', proxy_key, 'status', '0', 'consecutive_fail', consecutive_fail, 'checked_at', now)
    if consecutive_fail >= max_fail then
      redis.call('SADD', KEYS[2], addr)
      redis.call('ZREM', KEYS[1], addr)
      for i = 1, 4 do redis.call('SREM', KEYS[3 + i], addr) end
      new_bucket = 'dead'
    else
      redis.call('ZADD', KEYS[1], next_check, addr)
      new_bucket = 'cooldown'
    end
  end

  if old_bucket ~= new_bucket then
    redis.call('HINCRBY', KEYS[3], old_bucket, -1)
    redis.call('HINCRBY', KEYS[3], new_bucket, 1)
    if old_bucket == 'dead' then redis.call('HINCRBY', KEYS[3], 'total', 1) end
    if new_bucket == 'dead' then redis.call('HINCRBY', KEYS[3], 'total', -1) end
  end
  redis.call('SET', KEYS[8], last_check)
  redis.call('INCRBY', KEYS[9], 1)
  return new_bucket
`;

/** 原子取出到期候选：ZRANGEBYSCORE + ZREM，保证不重复取。 */
const POP_DUE_LUA = `
  local result = redis.call('ZRANGEBYSCORE', KEYS[1], 0, ARGV[1], 'LIMIT', 0, ARGV[2])
  for i, addr in ipairs(result) do
    redis.call('ZREM', KEYS[1], addr)
  end
  return result
`;

/**
 * 在 Redis 内完成协议集合并集、字典序排序和分页。
 * 只把当前页传回 Node.js，避免大集合在网络上传输后再由应用层全量排序。
 */
const PAGE_AVAILABLE_LUA = `
  local members = redis.call('SUNION', unpack(KEYS))
  table.sort(members)

  local start_index = tonumber(ARGV[1]) + 1
  local end_index = math.min(start_index + tonumber(ARGV[2]) - 1, #members)
  local page = {}
  for i = start_index, end_index do
    page[#page + 1] = members[i]
  end
  return page
`;

/** 在 Redis 内从协议集合并集中随机选择一个地址。 */
const RANDOM_AVAILABLE_LUA = `
  local members = redis.call('SUNION', unpack(KEYS))
  if #members == 0 then
    return false
  end
  return members[math.random(#members)]
`;

export class Database {
  private redis: RedisClientType;
  private connected = false;

  constructor(cfg: AppConfig) {
    this.redis = createClient({
      socket: { host: cfg.redisHost, port: cfg.redisPort },
    });
    this.redis.on('error', (e) => logger.error(`Redis 错误: ${String(e)}`));
  }

  /** 连接 Redis。 */
  async connect(): Promise<void> {
    if (this.connected) return;
    await this.redis.connect();
    this.connected = true;
  }

  /**
   * 等待 Redis 就绪：能正常响应命令。
   * 容器启动时 Redis 可能已开放 TCP 端口但还在从磁盘加载数据（返回 LOADING），
   * 此时发业务命令会失败。这里轮询 PING 直到返回 PONG，确保 Redis 真正可用。
   */
  async waitReady(maxAttempts = 30, intervalMs = 1000): Promise<void> {
    for (let i = 0; i < maxAttempts; i++) {
      try {
        const pong = await this.redis.ping();
        if (pong === 'PONG') return;
      } catch {
        // LOADING 或其他瞬时错误，继续重试
      }
      await new Promise((r) => setTimeout(r, intervalMs));
    }
    throw new Error(`Redis 在 ${maxAttempts} 次探测后仍未就绪`);
  }

  async close(): Promise<void> {
    if (!this.connected) return;
    await this.redis.quit();
    this.connected = false;
  }

  /** 从 addrKey（如 "ip:port" 或 "ip:port:user:pass"）还原 Redis Hash key。 */
  private proxyKeyFromAddr(addrKey: string): string {
    return `proxy:${addrKey}`;
  }

  // -------------------------------------------------------------------------
  // 采集：批量入库
  // -------------------------------------------------------------------------

  /**
   * 批量入库新地址。只处理 known_proxies 中不存在的（新增），已存在的跳过。
   * 新增的代理加入 check_queue（score=0，立即可测）和 known_proxies。
   * 已软删（在 dead_pool 里）的代理被重新采集时复活：移出 dead_pool，重置状态。
   * 同一 ip:port 但不同认证视为不同代理。
   */
  async upsertAddresses(addrs: Map<string, ProxyAddr>): Promise<{ newCount: number; revivedCount: number }> {
    if (addrs.size === 0) return { newCount: 0, revivedCount: 0 };

    let newCount = 0;
    let revivedCount = 0;

    const entries = [...addrs.entries()];
    for (let offset = 0; offset < entries.length; offset += UPSERT_BATCH_SIZE) {
      const batch = entries.slice(offset, offset + UPSERT_BATCH_SIZE);
      const pipeline = this.redis.multi();
      for (const [addrKey, proxy] of batch) {
        pipeline.eval(UPSERT_ADDRESS_LUA, {
          keys: [KNOWN, DEAD, QUEUE, STATS],
          arguments: [
            addrKey,
            this.proxyKeyFromAddr(addrKey),
            proxy.username ?? '',
            proxy.password ?? '',
            proxy.ip,
            String(proxy.port),
            proxy.username === undefined ? '0' : '1',
          ],
        });
      }
      const results = await pipeline.execAsPipeline();
      for (const result of results) {
        if (Number(result) === 1) newCount++;
        if (Number(result) === 2) revivedCount++;
      }
    }
    return { newCount, revivedCount };
  }

  /** 记录采集完成，更新 meta 与采集计数。 */
  async markCollectCompleted(): Promise<void> {
    const now = fmtTime(new Date());
    await this.redis.set('meta:last_collect', now);
    await this.redis.incrBy('meta:collect_count', 1);
  }

  // -------------------------------------------------------------------------
  // 测活调度
  // -------------------------------------------------------------------------

  /**
   * 原子取出到期候选（score <= nowMs），取出的代理从队列移除。
   * 测活完成后再由 finalizeProbe 放回（带新的 score = next_check_at）。
   */
  async getProxiesToCheck(nowMs: number, limit: number): Promise<string[]> {
    return (await this.redis.eval(POP_DUE_LUA, {
      keys: [QUEUE],
      arguments: [String(nowMs), String(limit)],
    })) as string[];
  }

  /**
   * 将已从调度队列取出、但暂时无法构造连接信息的候选重新入队。
   * 延迟重试可以避免异常数据形成无间隔的调度热循环，同时防止候选永久漏检。
   */
  async requeueProbeCandidates(addrKeys: string[], nextCheckAt: number): Promise<void> {
    if (addrKeys.length === 0) return;
    await this.redis.zAdd(
      QUEUE,
      addrKeys.map((value) => ({ score: nextCheckAt, value })),
    );
  }

  /** 读取单个代理的连续失败次数。 */
  async getConsecutiveFail(addrKey: string): Promise<number> {
    const val = (await this.redis.hGet(this.proxyKeyFromAddr(addrKey), 'consecutive_fail')) ?? '0';
    return parseInt(val, 10) || 0;
  }

  // -------------------------------------------------------------------------
  // 测活结果回写
  // -------------------------------------------------------------------------

  /**
   * 一次性写入一个代理的全部协议结果、整体状态、调度时间和测活元数据。
   *
   * 原实现会为四种协议分别往返 Redis，再单独更新代理状态、时间和计数。
   * 这里把同一次测活产生的所有写操作合并到一个 pipeline，减少网络往返和
   * Redis 命令调度开销，同时保证同一轮结果按顺序一次提交。
   */
  async finalizeProbe(
    addrKey: string,
    protocolResults: ReadonlyArray<{ type: number; ok: boolean }>,
    ok: boolean,
    consecutiveFail: number,
    cfg: AppConfig,
  ): Promise<void> {
    const now = Date.now();
    const backoff = ok
      ? cfg.interval * 1000
      : Math.max(cfg.interval, Math.pow(cfg.intervalBase, consecutiveFail) * cfg.interval) * 1000;
    const resultByType = new Map(protocolResults.map((result) => [result.type, result.ok]));
    await this.redis.eval(FINALIZE_PROBE_LUA, {
      keys: [QUEUE, DEAD, STATS, ...ALL_AVAIL_KEYS, 'meta:last_check', 'meta:check_count'],
      arguments: [
        addrKey,
        this.proxyKeyFromAddr(addrKey),
        ok ? '1' : '0',
        String(consecutiveFail),
        String(cfg.maxConsecutiveFail),
        String(now),
        String(now + backoff),
        fmtTime(new Date(now)),
        ...[1, 2, 3, 4].map((type) => (resultByType.get(type) ? '1' : '0')),
      ],
    });
  }

  // -------------------------------------------------------------------------
  // 对外 API 查询
  // -------------------------------------------------------------------------

  /**
   * 获取可用代理列表（基于 available:{type} 集合）。
   * types 为协议类型集合（空表示全部，取所有协议的并集）。
   * 结果按代理地址排序，分页返回。
   */
  async listAvailable(types: number[], page: number, count: number): Promise<AvailableItem[]> {
    const keys = types.length === 0 ? ALL_AVAIL_KEYS : types.map((t) => `available:${t}`);
    const start = (page - 1) * count;
    const pageItems = (await this.redis.eval(PAGE_AVAILABLE_LUA, {
      keys,
      arguments: [String(start), String(count)],
    })) as string[];
    return this.buildAvailableItems(pageItems);
  }

  /** 随机返回一个可用代理。types 为协议类型集合（空表示全部）。 */
  async randomAvailable(types: number[]): Promise<AvailableItem | null> {
    const keys = types.length === 0 ? ALL_AVAIL_KEYS : types.map((t) => `available:${t}`);
    const addrKey = (await this.redis.eval(RANDOM_AVAILABLE_LUA, {
      keys,
      arguments: [],
    })) as string | null;
    if (!addrKey) return null;
    return (await this.buildAvailableItems([addrKey]))[0] ?? null;
  }

  /**
   * 批量构建接口返回项。
   * 认证信息通过一个 pipeline 读取，协议状态通过四次 SMISMEMBER 批量查询，
   * Redis 往返次数不会随分页条数线性增长。
   */
  private async buildAvailableItems(addrKeys: string[]): Promise<AvailableItem[]> {
    if (addrKeys.length === 0) return [];

    const authPipeline = this.redis.multi();
    for (const addrKey of addrKeys) {
      authPipeline.hmGet(this.proxyKeyFromAddr(addrKey), ['ip', 'port', 'username', 'password']);
    }

    const [authRows, protocolFlags] = await Promise.all([
      authPipeline.execAsPipeline() as Promise<Array<[string | null, string | null, string | null, string | null]>>,
      Promise.all(
        ALL_AVAIL_KEYS.map(
          (key) => this.redis.sendCommand(['SMISMEMBER', key, ...addrKeys]) as Promise<number[]>,
        ),
      ),
    ]);

    return addrKeys.map((addrKey, index) => {
      const [storedIp, storedPort, username, password] = authRows[index] ?? [];
      const parsed = storedIp && storedPort
        ? { ip: storedIp, port: Number(storedPort) }
        : parseAddrKey(addrKey);
      if (!parsed) return null;
      const item: AvailableItem = {
        ip: parsed.ip,
        port: parsed.port,
        protocols: [],
      };

      if (username !== null) item.username = username;
      if (password !== null) item.password = password;

      for (let protocolIndex = 0; protocolIndex < protocolFlags.length; protocolIndex++) {
        if (protocolFlags[protocolIndex][index]) {
          const type = protocolIndex + 1;
          item.protocols.push(TYPE_TO_NAME[type] ?? 'http');
        }
      }
      return item;
    }).filter((item): item is AvailableItem => item !== null);
  }

  /** 批量读取调度候选的结构化连接信息。 */
  async getProxyAddresses(addrKeys: string[]): Promise<Array<{ addrKey: string; proxy: ProxyAddr }>> {
    if (addrKeys.length === 0) return [];
    const pipeline = this.redis.multi();
    for (const addrKey of addrKeys) {
      pipeline.hmGet(this.proxyKeyFromAddr(addrKey), ['ip', 'port', 'username', 'password']);
    }
    const rows = await pipeline.execAsPipeline() as Array<[string | null, string | null, string | null, string | null]>;

    const result: Array<{ addrKey: string; proxy: ProxyAddr }> = [];
    for (let index = 0; index < addrKeys.length; index++) {
      const [storedIp, storedPort, username, password] = rows[index] ?? [];
      const parsed = storedIp && storedPort
        ? { ip: storedIp, port: Number(storedPort) } as ProxyAddr
        : parseAddrKey(addrKeys[index]);
      if (!parsed || !Number.isInteger(parsed.port)) continue;
      if (username !== null) parsed.username = username;
      if (password !== null) parsed.password = password;
      result.push({ addrKey: addrKeys[index], proxy: parsed });
    }
    return result;
  }

  /**
   * 启动业务前分批扫描并重建统计值。
   * SSCAN 和批量 HMGET 避免一段长 Lua 独占 Redis，同时可修复历史版本留下的漂移。
   */
  async reconcileStats(): Promise<void> {
    let cursor = '0';
    let available = 0;
    let unchecked = 0;
    let cooldown = 0;
    let dead = 0;

    do {
      const reply = (await this.redis.sendCommand([
        'SSCAN', KNOWN, cursor, 'COUNT', String(STATS_SCAN_BATCH_SIZE),
      ])) as [string, string[]];
      cursor = reply[0];
      const addrKeys = reply[1];
      if (addrKeys.length === 0) continue;

      const pipeline = this.redis.multi();
      for (const addrKey of addrKeys) {
        pipeline.hmGet(this.proxyKeyFromAddr(addrKey), ['status', 'checked_at']);
      }
      const [deadFlags, states] = await Promise.all([
        this.redis.sendCommand(['SMISMEMBER', DEAD, ...addrKeys]) as Promise<number[]>,
        pipeline.execAsPipeline() as Promise<Array<[string | null, string | null]>>,
      ]);

      for (let i = 0; i < addrKeys.length; i++) {
        if (deadFlags[i]) {
          dead++;
          continue;
        }
        const [status, checkedAt] = states[i] ?? [];
        if (status === '1') available++;
        else if (!checkedAt || checkedAt === '0') unchecked++;
        else cooldown++;
      }
    } while (cursor !== '0');

    await this.redis.hSet(STATS, {
      total: String(available + unchecked + cooldown),
      available: String(available),
      unchecked: String(unchecked),
      cooldown: String(cooldown),
      dead: String(dead),
    });
  }

  /**
   * 启动业务前修复调度队列。
   *
   * 测活候选取出时会暂时从 check_queue 删除。如果进程恰好在测活完成前退出，
   * 这些代理来不及重新入队，重启后将永久漏检。这里分批扫描代理全集，把所有
   * 未软删且不在队列中的代理重新加入队列，并立即安排测活；同时移除死亡代理
   * 可能残留的队列成员。
   */
  async reconcileCheckQueue(): Promise<{ restored: number; removedDead: number }> {
    let cursor = '0';
    let restored = 0;
    let removedDead = 0;

    do {
      const reply = (await this.redis.sendCommand([
        'SSCAN', KNOWN, cursor, 'COUNT', String(STATS_SCAN_BATCH_SIZE),
      ])) as [string, string[]];
      cursor = reply[0];
      const addrKeys = reply[1];
      if (addrKeys.length === 0) continue;

      const [deadFlags, queueScores] = await Promise.all([
        this.redis.sendCommand(['SMISMEMBER', DEAD, ...addrKeys]) as Promise<number[]>,
        this.redis.sendCommand(['ZMSCORE', QUEUE, ...addrKeys]) as Promise<Array<string | null>>,
      ]);
      const pipeline = this.redis.multi();
      let batchCommands = 0;

      for (let i = 0; i < addrKeys.length; i++) {
        const inQueue = queueScores[i] !== null;
        if (deadFlags[i]) {
          if (inQueue) {
            pipeline.zRem(QUEUE, addrKeys[i]);
            removedDead++;
            batchCommands++;
          }
        } else if (!inQueue) {
          pipeline.zAdd(QUEUE, { score: 0, value: addrKeys[i] });
          restored++;
          batchCommands++;
        }
      }

      if (batchCommands > 0) await pipeline.execAsPipeline();
    } while (cursor !== '0');

    return { restored, removedDead };
  }

  /**
   * 获取系统运行状态。请求只读取常量大小的统计 Hash 和元数据：
   *   proxy_total = proxy_available + proxy_unchecked + proxy_cooldown
   *   proxy_dead 单独计数（不计入上面等式）
   * 状态迁移由 Lua 原子维护，服务启动时再通过分批扫描进行自愈校准。
   */
  async getStats(): Promise<StatsItem> {
    const [lastCollect, lastCheck, collectCountRaw, checkCountRaw, counts] = await Promise.all([
      this.redis.get('meta:last_collect'),
      this.redis.get('meta:last_check'),
      this.redis.get('meta:collect_count'),
      this.redis.get('meta:check_count'),
      this.redis.hmGet(STATS, ['total', 'available', 'unchecked', 'cooldown', 'dead']),
    ]);
    const [total, available, unchecked, cooldown, dead] = counts.map((n) => Number(n) || 0);

    return {
      proxy_total: total,
      proxy_available: available,
      proxy_unchecked: unchecked,
      proxy_cooldown: cooldown,
      proxy_dead: dead,
      proxy_checking: 0, // 占位，由 API 层用运行时数据覆盖
      last_collect_at: lastCollect ?? null,
      last_check_at: lastCheck ?? null,
      collect_count: parseInt(collectCountRaw ?? '0', 10) || 0,
      check_count: parseInt(checkCountRaw ?? '0', 10) || 0,
    };
  }

}
