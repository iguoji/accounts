/**
 * Redis 数据访问层。
 *
 * 数据结构映射：
 *  - proxy:{ip}:{port}          Hash   单个代理的状态（status / consecutive_fail / checked_at）
 *  - check_queue:available      ZSet   可用代理复查队列，score = next_check_at
 *  - check_queue:unchecked      ZSet   新入库未检测队列，score = next_check_at
 *  - check_queue:cooldown       ZSet   失败代理复查队列，score = next_check_at
 *  - available:{type}           Set    按协议分组的可用代理集合（type = 1/2/3/4）
 *  - known_proxies              Set    所有已采集入库的代理（用于采集去重）
 *  - dead_pool                  Set    已软删的代理（连续失败达上限）
 *  - stats                      Hash   统计计数器（total / available / dead / checked / unchecked）
 *  - meta:last_collect          String 最近一次采集完成时间
 *  - meta:collect_count         String 累计采集次数
 *  - meta:last_check            String 最近一次测活时间
 *
 * 时间存储约定：
 *  - 三个 check_queue:* 的 score 与 checked_at 使用毫秒级整数（Date.now()），便于与"当前时间"直接比较
 *  - meta:last_collect / meta:last_check 为 "YYYY-MM-DD HH:MM:SS" 文本（仅供展示）
 */
import { createClient, type RedisClientType } from 'redis';
import { logger } from './logger.js';
import { parseAddrKey, TYPE_TO_NAME } from './types.js';
import type { ProxyAddr } from './types.js';
import type { AppConfig } from './config.js';

export interface SourceState {
  id: string;
  url: string;
  enabled: boolean;
  contentHash: string;
  proxySetHash: string;
  etag: string;
  lastModified: string;
  lastCheckedAt: number;
  lastChangedAt: number;
  nextCheckAt: number;
  consecutiveUnchanged: number;
  consecutiveFail: number;
  lastHttpStatus: number;
  changeIntervals: number[];
}

export interface SourceCheckLog {
  checkedAt: number;
  result: string;
  httpStatus: number;
  contentChanged: boolean;
  proxySetChanged: boolean;
  contentHash: string;
  proxySetHash: string;
  validProxyCount: number;
  addedProxyCount: number;
  revivedProxyCount: number;
  elapsedMs: number;
  nextCheckAt: number;
  scheduleReason: string;
  error: string;
}

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
const LEGACY_QUEUE = 'check_queue';
const AVAILABLE_QUEUE = 'check_queue:available';
const UNCHECKED_QUEUE = 'check_queue:unchecked';
const COOLDOWN_QUEUE = 'check_queue:cooldown';
const PROBE_QUEUES = [AVAILABLE_QUEUE, UNCHECKED_QUEUE, COOLDOWN_QUEUE] as const;
export type ProbeQueueBucket = 'available' | 'unchecked' | 'cooldown';
export interface ProbeCandidate {
  addrKey: string;
  bucket: ProbeQueueBucket;
}
const DEAD = 'dead_pool';
const STATS = 'stats';
const ALL_AVAIL_KEYS = ['available:1', 'available:2', 'available:3', 'available:4'];
const UPSERT_BATCH_SIZE = 1000;
const STATS_SCAN_BATCH_SIZE = 1000;
const SOURCE_QUEUE = 'source_probe_queue';
const SOURCE_IDS = 'source_ids';
const SOURCE_SITE_COOLDOWN_PREFIX = 'source_site_cooldown:';
const DOMAIN_BLOCK_PREFIX = 'domain_block:';

/** 原子写入业务域名封禁，已有截止时间更晚时保持原值。 */
const BLOCK_FOR_DOMAIN_LUA = `
  local current = tonumber(redis.call('ZSCORE', KEYS[1], ARGV[1]) or '0')
  local requested = tonumber(ARGV[2])
  local final = math.max(current, requested)
  redis.call('ZADD', KEYS[1], final, ARGV[1])
  return final
`;

const POP_DUE_SOURCE_LUA = `
  local result = redis.call('ZRANGEBYSCORE', KEYS[1], 0, ARGV[1], 'LIMIT', 0, 1)
  if #result == 0 then return false end
  redis.call('ZREM', KEYS[1], result[1])
  return result[1]
`;

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
  local now = tonumber(ARGV[8])
  local revive_after = tonumber(ARGV[9])

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

    local dead_at = tonumber(redis.call('HGET', proxy_key, 'dead_at') or '') or 0
    if dead_at == 0 then
      -- 兼容旧版本软删除数据：首次重新采集时才开始计算等待期。
      redis.call('HSET', proxy_key, 'dead_at', now)
      return 0
    end
    if now - dead_at < revive_after then
      return 0
    end

    redis.call('SREM', KEYS[2], addr)
    redis.call('HSET', proxy_key,
      'status', '0',
      'consecutive_fail', '0',
      'checked_at', '',
      'dead_at', '',
      'ip', ip,
      'port', port)
    redis.call('ZREM', KEYS[3], addr)
    redis.call('ZREM', KEYS[4], addr)
    redis.call('ZADD', KEYS[5], 0, addr)
    redis.call('HINCRBY', KEYS[6], 'dead', -1)
    redis.call('HINCRBY', KEYS[6], 'unchecked', 1)
    redis.call('HINCRBY', KEYS[6], 'total', 1)
    return 2
  end

  redis.call('SADD', KEYS[1], addr)
  redis.call('HSET', proxy_key,
    'status', '0',
    'consecutive_fail', '0',
    'checked_at', '',
    'dead_at', '',
    'ip', ip,
    'port', port)
  if has_auth == '1' then
    redis.call('HSET', proxy_key, 'username', username, 'password', password)
  end
  redis.call('ZADD', KEYS[5], 0, addr)
  redis.call('HINCRBY', KEYS[6], 'unchecked', 1)
  redis.call('HINCRBY', KEYS[6], 'total', 1)
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

  local was_dead = redis.call('SISMEMBER', KEYS[4], addr) == 1
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
      redis.call('SADD', KEYS[5 + i], addr)
    else
      redis.call('SREM', KEYS[5 + i], addr)
    end
  end

  local new_bucket
  redis.call('ZREM', KEYS[1], addr)
  redis.call('ZREM', KEYS[2], addr)
  redis.call('ZREM', KEYS[3], addr)
  if ok then
    redis.call('HSET', proxy_key, 'status', '1', 'consecutive_fail', '0', 'checked_at', now, 'dead_at', '')
    redis.call('ZADD', KEYS[1], next_check, addr)
    redis.call('SREM', KEYS[4], addr)
    new_bucket = 'available'
  else
    redis.call('HSET', proxy_key, 'status', '0', 'consecutive_fail', consecutive_fail, 'checked_at', now)
    if consecutive_fail >= max_fail then
      redis.call('SADD', KEYS[4], addr)
      redis.call('HSET', proxy_key, 'dead_at', now)
      for i = 1, 4 do redis.call('SREM', KEYS[5 + i], addr) end
      new_bucket = 'dead'
    else
      redis.call('ZADD', KEYS[3], next_check, addr)
      new_bucket = 'cooldown'
    end
  end

  if old_bucket ~= new_bucket then
    redis.call('HINCRBY', KEYS[5], old_bucket, -1)
    redis.call('HINCRBY', KEYS[5], new_bucket, 1)
    if old_bucket == 'dead' then redis.call('HINCRBY', KEYS[5], 'total', 1) end
    if new_bucket == 'dead' then redis.call('HINCRBY', KEYS[5], 'total', -1) end
  end
  redis.call('SET', KEYS[10], last_check)
  redis.call('INCRBY', KEYS[11], 1)
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
 * 在 Redis 内完成协议集合并集、字典序排序、分页和返回字段组装。
 * 整个请求只往返 Redis 一次，避免取到地址后再次查询 Hash 和协议集合。
 */
const PAGE_AVAILABLE_LUA = `
  local members = redis.call('SUNION', unpack(KEYS))
  table.sort(members)

  local start_offset = tonumber(ARGV[1])
  local limit = tonumber(ARGV[2])
  local domain_key = ARGV[3]
  local now = tonumber(ARGV[4])
  local result = {}
  local accepted = 0
  for i = 1, #members do
    local addr = members[i]
    local values = redis.call('HMGET', 'proxy:' .. addr, 'ip', 'port', 'username', 'password')
    local blocked = false
    if domain_key ~= '' and values[1] then
      local blocked_until = tonumber(redis.call('ZSCORE', domain_key, values[1]) or '0')
      if blocked_until > now then
        blocked = true
      elseif blocked_until > 0 then
        redis.call('ZREM', domain_key, values[1])
      end
    end
    if not blocked then
      if accepted >= start_offset and #result < limit * 6 then
        local protocols = {}
        for protocol_index = 1, 4 do
          if redis.call('SISMEMBER', 'available:' .. protocol_index, addr) == 1 then
            protocols[#protocols + 1] = protocol_index
          end
        end
        result[#result + 1] = addr
        result[#result + 1] = values[1] or false
        result[#result + 1] = values[2] or false
        result[#result + 1] = values[3] or false
        result[#result + 1] = values[4] or false
        result[#result + 1] = protocols
      end
      accepted = accepted + 1
    end
  end
  return result
`;

/** 从协议集合的唯一并集中均匀随机选择一个地址，并同时组装返回字段。 */
const RANDOM_AVAILABLE_LUA = `
  local members = redis.call('SUNION', unpack(KEYS))
  if #members == 0 then return false end
  local domain_key = ARGV[1]
  local now = tonumber(ARGV[2])
  local candidates = {}
  for i = 1, #members do
    local values = redis.call('HMGET', 'proxy:' .. members[i], 'ip', 'port', 'username', 'password')
    local blocked = false
    if domain_key ~= '' and values[1] then
      local blocked_until = tonumber(redis.call('ZSCORE', domain_key, values[1]) or '0')
      if blocked_until > now then
        blocked = true
      elseif blocked_until > 0 then
        redis.call('ZREM', domain_key, values[1])
      end
    end
    if not blocked then candidates[#candidates + 1] = members[i] end
  end
  if #candidates == 0 then return false end
  local addr = candidates[math.random(#candidates)]
  local values = redis.call('HMGET', 'proxy:' .. addr, 'ip', 'port', 'username', 'password')
  local protocols = {}
  for protocol_index = 1, 4 do
    if redis.call('SISMEMBER', 'available:' .. protocol_index, addr) == 1 then
      protocols[#protocols + 1] = protocol_index
    end
  end
  return {addr, values[1] or false, values[2] or false, values[3] or false, values[4] or false, protocols}
`;

/** 一次读取统计 Hash 与元数据，每次调用仍返回 Redis 中的实时值。 */
const GET_STATS_LUA = `
  local counts = redis.call('HMGET', KEYS[1], 'total', 'available', 'unchecked', 'cooldown', 'dead')
  return {
    counts[1] or false,
    counts[2] or false,
    counts[3] or false,
    counts[4] or false,
    counts[5] or false,
    redis.call('GET', KEYS[2]) or false,
    redis.call('GET', KEYS[3]) or false,
    redis.call('GET', KEYS[4]) or false,
    redis.call('GET', KEYS[5]) or false
  }
`;

export class Database {
  private redis: RedisClientType;
  private connected = false;
  private deadReviveAfterMs: number;
  /** 跨调度轮次保存 5:3:2 周期位置，避免少量空闲槽位长期偏向某一类。 */
  private probeQuotaCursor = 0;

  constructor(cfg: AppConfig) {
    this.deadReviveAfterMs = cfg.deadReviveAfter * 1000;
    this.redis = createClient({
      socket: { host: cfg.redisHost, port: cfg.redisPort },
      password: cfg.redisPassword,
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
   * 新增的代理加入 check_queue:unchecked（score=0，立即可测）和 known_proxies。
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
          keys: [KNOWN, DEAD, AVAILABLE_QUEUE, COOLDOWN_QUEUE, UNCHECKED_QUEUE, STATS],
          arguments: [
            addrKey,
            this.proxyKeyFromAddr(addrKey),
            proxy.username ?? '',
            proxy.password ?? '',
            proxy.ip,
            String(proxy.port),
            proxy.username === undefined ? '0' : '1',
            String(Date.now()),
            String(this.deadReviveAfterMs),
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

  private sourceKey(sourceId: string): string {
    return `source:${sourceId}`;
  }

  async syncSources(sources: Array<{ id: string; url: string }>, initialNextCheckAt: number): Promise<number> {
    if (sources.length === 0) return 0;
    const pipeline = this.redis.multi();
    for (const source of sources) {
      const key = this.sourceKey(source.id);
      pipeline.sAdd(SOURCE_IDS, source.id);
      pipeline.hSetNX(key, 'url', source.url);
      pipeline.hSetNX(key, 'enabled', '1');
      pipeline.hSetNX(key, 'status', 'pending');
      pipeline.hSetNX(key, 'content_hash', '');
      pipeline.hSetNX(key, 'proxy_set_hash', '');
      pipeline.hSetNX(key, 'etag', '');
      pipeline.hSetNX(key, 'last_modified', '');
      pipeline.hSetNX(key, 'last_checked_at', '0');
      pipeline.hSetNX(key, 'last_changed_at', '0');
      pipeline.hSetNX(key, 'next_check_at', String(initialNextCheckAt));
      pipeline.hSetNX(key, 'consecutive_unchanged', '0');
      pipeline.hSetNX(key, 'consecutive_fail', '0');
      pipeline.hSetNX(key, 'last_http_status', '0');
      pipeline.hSetNX(key, 'last_error', '');
      pipeline.hSetNX(key, 'change_intervals', '[]');
      pipeline.zAdd(
        SOURCE_QUEUE,
        { score: initialNextCheckAt, value: source.id },
        { NX: true },
      );
    }
    await pipeline.execAsPipeline();
    return sources.length;
  }

  async popDueSource(nowMs: number): Promise<string | null> {
    const result = await this.redis.eval(POP_DUE_SOURCE_LUA, {
      keys: [SOURCE_QUEUE],
      arguments: [String(nowMs)],
    });
    return result === null ? null : String(result);
  }

  /** 将异常中断或历史版本遗漏的启用源头补回调度队列。 */
  async repairSourceQueue(nowMs: number): Promise<number> {
    const sourceIds = await this.redis.sMembers(SOURCE_IDS);
    let restored = 0;
    for (const sourceId of sourceIds) {
      const source = await this.getSource(sourceId);
      if (!source || !source.enabled) continue;
      const score = source.nextCheckAt > 0 ? source.nextCheckAt : nowMs;
      const added = await this.redis.zAdd(
        SOURCE_QUEUE,
        { score, value: sourceId },
        { NX: true },
      );
      restored += Number(added) || 0;
    }
    return restored;
  }

  async requeueSource(sourceId: string, nextCheckAt: number): Promise<void> {
    await this.redis.zAdd(SOURCE_QUEUE, { score: nextCheckAt, value: sourceId });
  }

  async getSource(sourceId: string): Promise<SourceState | null> {
    const raw = await this.redis.hGetAll(this.sourceKey(sourceId));
    if (!raw.url) return null;
    let changeIntervals: number[] = [];
    try {
      const parsed = JSON.parse(raw.change_intervals || '[]');
      if (Array.isArray(parsed)) changeIntervals = parsed.map(Number).filter(Number.isFinite);
    } catch {
      changeIntervals = [];
    }
    return {
      id: sourceId,
      url: raw.url,
      enabled: raw.enabled !== '0',
      contentHash: raw.content_hash || '',
      proxySetHash: raw.proxy_set_hash || '',
      etag: raw.etag || '',
      lastModified: raw.last_modified || '',
      lastCheckedAt: Number(raw.last_checked_at) || 0,
      lastChangedAt: Number(raw.last_changed_at) || 0,
      nextCheckAt: Number(raw.next_check_at) || 0,
      consecutiveUnchanged: Number(raw.consecutive_unchanged) || 0,
      consecutiveFail: Number(raw.consecutive_fail) || 0,
      lastHttpStatus: Number(raw.last_http_status) || 0,
      changeIntervals,
    };
  }

  async saveSource(source: SourceState, status: string, error = ''): Promise<void> {
    await this.redis.hSet(this.sourceKey(source.id), {
      url: source.url,
      enabled: source.enabled ? '1' : '0',
      status,
      content_hash: source.contentHash,
      proxy_set_hash: source.proxySetHash,
      etag: source.etag,
      last_modified: source.lastModified,
      last_checked_at: String(source.lastCheckedAt),
      last_changed_at: String(source.lastChangedAt),
      next_check_at: String(source.nextCheckAt),
      consecutive_unchanged: String(source.consecutiveUnchanged),
      consecutive_fail: String(source.consecutiveFail),
      last_http_status: String(source.lastHttpStatus),
      last_error: error,
      change_intervals: JSON.stringify(source.changeIntervals.slice(-10)),
    });
    if (source.enabled) {
      await this.redis.zAdd(SOURCE_QUEUE, { score: source.nextCheckAt, value: source.id });
    }
  }

  async appendSourceLog(sourceId: string, entry: SourceCheckLog, maxLength: number): Promise<void> {
    await this.redis.sendCommand([
      'XADD', `source_log:${sourceId}`, 'MAXLEN', '~', String(maxLength), '*',
      'checked_at', String(entry.checkedAt),
      'result', entry.result,
      'http_status', String(entry.httpStatus),
      'content_changed', entry.contentChanged ? '1' : '0',
      'proxy_set_changed', entry.proxySetChanged ? '1' : '0',
      'content_hash', entry.contentHash,
      'proxy_set_hash', entry.proxySetHash,
      'valid_proxy_count', String(entry.validProxyCount),
      'added_proxy_count', String(entry.addedProxyCount),
      'revived_proxy_count', String(entry.revivedProxyCount),
      'elapsed_ms', String(entry.elapsedMs),
      'next_check_at', String(entry.nextCheckAt),
      'schedule_reason', entry.scheduleReason,
      'error', entry.error,
    ]);
  }

  async getSiteCooldown(hostname: string): Promise<number> {
    return Number(await this.redis.get(`${SOURCE_SITE_COOLDOWN_PREFIX}${hostname}`)) || 0;
  }

  async setSiteCooldown(hostname: string, untilMs: number): Promise<void> {
    await this.redis.set(`${SOURCE_SITE_COOLDOWN_PREFIX}${hostname}`, String(untilMs));
  }

  // -------------------------------------------------------------------------
  // 测活调度
  // -------------------------------------------------------------------------

  /**
   * 原子取出到期候选（score <= nowMs），取出的代理从队列移除。
   * 测活完成后再由 finalizeProbe 放回（带新的 score = next_check_at）。
   */
  async getProxiesToCheck(nowMs: number, limit: number): Promise<ProbeCandidate[]> {
    if (limit <= 0) return [];
    const buckets: Array<{ bucket: ProbeQueueBucket; queue: string }> = [
      { bucket: 'available', queue: AVAILABLE_QUEUE },
      { bucket: 'unchecked', queue: UNCHECKED_QUEUE },
      { bucket: 'cooldown', queue: COOLDOWN_QUEUE },
    ];
    // 每十个槽位严格分配为 5:3:2，并将三类任务交错排列。
    // 游标跨调用保存，即使每轮只腾出一个槽位，长期比例仍保持 5:3:2。
    const quotaCycle: ProbeQueueBucket[] = [
      'available', 'unchecked', 'available', 'cooldown', 'available',
      'unchecked', 'available', 'cooldown', 'available', 'unchecked',
    ];
    const quotas = buckets.map(() => 0);
    for (let i = 0; i < limit; i++) {
      const bucket = quotaCycle[(this.probeQuotaCursor + i) % quotaCycle.length];
      const index = buckets.findIndex((item) => item.bucket === bucket);
      quotas[index]++;
    }
    this.probeQuotaCursor = (this.probeQuotaCursor + limit) % quotaCycle.length;

    const candidates: ProbeCandidate[] = [];
    for (let i = 0; i < buckets.length; i++) {
      if (quotas[i] === 0) continue;
      const rows = (await this.redis.eval(POP_DUE_LUA, {
        keys: [buckets[i].queue],
        arguments: [String(nowMs), String(quotas[i])],
      })) as string[];
      candidates.push(...rows.map((addrKey) => ({ addrKey, bucket: buckets[i].bucket })));
    }

    let remaining = limit - candidates.length;
    while (remaining > 0) {
      let added = 0;
      for (const item of buckets) {
        if (remaining === 0) break;
        const rows = (await this.redis.eval(POP_DUE_LUA, {
          keys: [item.queue],
          arguments: [String(nowMs), String(remaining)],
        })) as string[];
        candidates.push(...rows.map((addrKey) => ({ addrKey, bucket: item.bucket })));
        remaining -= rows.length;
        added += rows.length;
      }
      if (added === 0) break;
    }
    return candidates;
  }

  /**
   * 将已从调度队列取出、但暂时无法构造连接信息的候选重新入队。
   * 延迟重试可以避免异常数据形成无间隔的调度热循环，同时防止候选永久漏检。
   */
  async requeueProbeCandidates(candidates: ProbeCandidate[], nextCheckAt: number): Promise<void> {
    if (candidates.length === 0) return;
    const pipeline = this.redis.multi();
    for (const bucket of ['available', 'unchecked', 'cooldown'] as const) {
      const values = candidates
        .filter((candidate) => candidate.bucket === bucket)
        .map(({ addrKey: value }) => ({ score: nextCheckAt, value }));
      if (values.length > 0) {
        const queue = bucket === 'available' ? AVAILABLE_QUEUE : bucket === 'unchecked' ? UNCHECKED_QUEUE : COOLDOWN_QUEUE;
        pipeline.zAdd(queue, values);
      }
    }
    await pipeline.execAsPipeline();
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
   * 原实现会为各协议分别往返 Redis，再单独更新代理状态、时间和计数。
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
      keys: [AVAILABLE_QUEUE, UNCHECKED_QUEUE, COOLDOWN_QUEUE, DEAD, STATS, ...ALL_AVAIL_KEYS, 'meta:last_check', 'meta:check_count'],
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
  async listAvailable(types: number[], page: number, count: number, domain?: string): Promise<AvailableItem[]> {
    const keys = types.length === 0 ? ALL_AVAIL_KEYS : types.map((t) => `available:${t}`);
    const start = (page - 1) * count;
    const rows = (await this.redis.eval(PAGE_AVAILABLE_LUA, {
      keys,
      arguments: [String(start), String(count), domain ? `${DOMAIN_BLOCK_PREFIX}${domain}` : '', String(Date.now())],
    })) as unknown[];
    return this.parseAvailableRows(rows);
  }

  /** 随机返回一个可用代理。types 为协议类型集合（空表示全部）。 */
  async randomAvailable(types: number[], domain?: string): Promise<AvailableItem | null> {
    const keys = types.length === 0 ? ALL_AVAIL_KEYS : types.map((t) => `available:${t}`);
    const row = (await this.redis.eval(RANDOM_AVAILABLE_LUA, {
      keys,
      arguments: [domain ? `${DOMAIN_BLOCK_PREFIX}${domain}` : '', String(Date.now())],
    })) as unknown[] | null;
    if (!row) return null;
    return this.parseAvailableRows(row)[0] ?? null;
  }

  /** 确认反馈的 IP 和端口属于代理池中的代理。认证信息和协议不参与确认。 */
  async hasProxyEndpoint(ip: string, port: number): Promise<boolean> {
    let cursor = '0';
    do {
      const reply = (await this.redis.sendCommand([
        'SSCAN', KNOWN, cursor, 'COUNT', String(STATS_SCAN_BATCH_SIZE),
      ])) as [string, string[]];
      cursor = reply[0];
      if (reply[1].length === 0) continue;
      const pipeline = this.redis.multi();
      for (const addrKey of reply[1]) {
        pipeline.hmGet(this.proxyKeyFromAddr(addrKey), ['ip', 'port']);
      }
      const rows = await pipeline.execAsPipeline() as Array<[string | null, string | null]>;
      if (rows.some(([storedIp, storedPort]) => storedIp === ip && Number(storedPort) === port)) return true;
    } while (cursor !== '0');
    return false;
  }

  /** 记录业务域名封禁。同一 IP 和域名的重复反馈只能延长，不能缩短。 */
  async blockForDomain(ip: string, domain: string, blockedSeconds: number): Promise<number> {
    const requestedUntil = Date.now() + blockedSeconds * 1000;
    const key = `${DOMAIN_BLOCK_PREFIX}${domain}`;
    return Number(await this.redis.eval(BLOCK_FOR_DOMAIN_LUA, {
      keys: [key],
      arguments: [ip, String(requestedUntil)],
    }));
  }

  /** 将 Lua 返回的扁平行数据转换为接口对象，每行固定六项。 */
  private parseAvailableRows(rows: unknown[]): AvailableItem[] {
    const items: AvailableItem[] = [];
    for (let offset = 0; offset + 5 < rows.length; offset += 6) {
      const addrKey = String(rows[offset]);
      const storedIp = rows[offset + 1] === null ? null : String(rows[offset + 1]);
      const storedPort = rows[offset + 2] === null ? null : String(rows[offset + 2]);
      const username = rows[offset + 3] === null ? null : String(rows[offset + 3]);
      const password = rows[offset + 4] === null ? null : String(rows[offset + 4]);
      const protocolTypes = rows[offset + 5] as number[];
      const parsed = storedIp && storedPort
        ? { ip: storedIp, port: Number(storedPort) }
        : parseAddrKey(addrKey);
      if (!parsed || !Number.isInteger(parsed.port)) continue;

      const item: AvailableItem = {
        ip: parsed.ip,
        port: parsed.port,
        protocols: protocolTypes
          .map((type) => TYPE_TO_NAME[type])
          .filter((name): name is string => name !== undefined),
      };
      if (username !== null) item.username = username;
      if (password !== null) item.password = password;
      items.push(item);
    }
    return items;
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
   * 测活候选取出时会暂时从分类队列删除。如果进程恰好在测活完成前退出，
   * 这些代理来不及重新入队，重启后将永久漏检。这里分批扫描代理全集，把所有
   * 未软删且不在队列中的代理重新加入队列，并立即安排测活；同时移除死亡代理
   * 可能残留的队列成员。
   */
  async reconcileCheckQueue(): Promise<{ restored: number; migrated: number; removedDead: number }> {
    let cursor = '0';
    let restored = 0;
    let migrated = 0;
    let removedDead = 0;

    do {
      const reply = (await this.redis.sendCommand([
        'SSCAN', KNOWN, cursor, 'COUNT', String(STATS_SCAN_BATCH_SIZE),
      ])) as [string, string[]];
      cursor = reply[0];
      const addrKeys = reply[1];
      if (addrKeys.length === 0) continue;

      const statePipeline = this.redis.multi();
      for (const addrKey of addrKeys) statePipeline.hmGet(this.proxyKeyFromAddr(addrKey), ['status', 'checked_at']);
      const [deadFlags, legacyScores, availableScores, uncheckedScores, cooldownScores, states] = await Promise.all([
        this.redis.sendCommand(['SMISMEMBER', DEAD, ...addrKeys]) as Promise<number[]>,
        this.redis.sendCommand(['ZMSCORE', LEGACY_QUEUE, ...addrKeys]) as Promise<Array<string | null>>,
        this.redis.sendCommand(['ZMSCORE', AVAILABLE_QUEUE, ...addrKeys]) as Promise<Array<string | null>>,
        this.redis.sendCommand(['ZMSCORE', UNCHECKED_QUEUE, ...addrKeys]) as Promise<Array<string | null>>,
        this.redis.sendCommand(['ZMSCORE', COOLDOWN_QUEUE, ...addrKeys]) as Promise<Array<string | null>>,
        statePipeline.execAsPipeline() as Promise<Array<[string | null, string | null]>>,
      ]);
      const pipeline = this.redis.multi();
      let batchCommands = 0;

      for (let i = 0; i < addrKeys.length; i++) {
        const scores = [availableScores[i], uncheckedScores[i], cooldownScores[i]];
        const inAnyQueue = scores.some((score) => score !== null);
        if (deadFlags[i]) {
          if (legacyScores[i] !== null || inAnyQueue) {
            pipeline.zRem(LEGACY_QUEUE, addrKeys[i]);
            for (const queue of PROBE_QUEUES) pipeline.zRem(queue, addrKeys[i]);
            removedDead++;
            batchCommands += 4;
          }
        } else {
          const [status, checkedAt] = states[i] ?? [];
          const targetIndex = status === '1' ? 0 : (!checkedAt || checkedAt === '0' ? 1 : 2);
          const targetQueue = PROBE_QUEUES[targetIndex];
          const existingScore = scores[targetIndex];
          const legacyScore = legacyScores[i];
          const score = Number(existingScore ?? legacyScore ?? scores.find((value) => value !== null) ?? 0);
          const correctlyQueued = existingScore !== null && scores.every((value, index) => index === targetIndex || value === null);
          if (!correctlyQueued || legacyScore !== null) {
            for (const queue of PROBE_QUEUES) pipeline.zRem(queue, addrKeys[i]);
            pipeline.zAdd(targetQueue, { score, value: addrKeys[i] });
            pipeline.zRem(LEGACY_QUEUE, addrKeys[i]);
            if (!inAnyQueue && legacyScore === null) restored++;
            else migrated++;
            batchCommands += 5;
          }
        }
      }

      if (batchCommands > 0) await pipeline.execAsPipeline();
    } while (cursor !== '0');

    await this.redis.del(LEGACY_QUEUE);
    return { restored, migrated, removedDead };
  }

  /**
   * 获取系统运行状态。请求只读取常量大小的统计 Hash 和元数据：
   *   proxy_total = proxy_available + proxy_unchecked + proxy_cooldown
   *   proxy_dead 单独计数（不计入上面等式）
   * 状态迁移由 Lua 原子维护，服务启动时再通过分批扫描进行自愈校准。
   */
  async getStats(): Promise<StatsItem> {
    const row = (await this.redis.eval(GET_STATS_LUA, {
      keys: [STATS, 'meta:last_collect', 'meta:last_check', 'meta:collect_count', 'meta:check_count'],
      arguments: [],
    })) as Array<string | null>;
    const [total, available, unchecked, cooldown, dead] = row.slice(0, 5).map((n) => Number(n) || 0);
    const lastCollect = row[5];
    const lastCheck = row[6];
    const collectCountRaw = row[7];
    const checkCountRaw = row[8];

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
