/**
 * 测活：对单个代理分别以 HTTP(CONNECT) / SOCKS4 / SOCKS5 三个协议并发探测，
 * 成功标准为「通过代理访问测活渠道，返回 HTTP 200 且响应体含非空 IP 文本」。
 * 主备渠道都是全球知名网站，理论上永远可用；仅当主渠道返回 404（渠道自身页面问题）
 * 时才回退到备用渠道，其他失败（超时、连接错误等）一律判定为代理失效。
 *
 * 实现说明：
 * - 每个协议类型选对应的 http.Agent 子类（https-proxy-agent /
 *   socks-proxy-agent），再配合 Node 原生 https.request 发起请求。
 * - 这些都是 http.Agent 家族（agent-base），与 Node 原生网络栈直接配合，
 *   不通过 undici/fetch 的 dispatcher 方式。
 */
import * as http from 'node:http';
import * as https from 'node:https';
import { HttpsProxyAgent } from 'https-proxy-agent';
import { SocksProxyAgent } from 'socks-proxy-agent';
import { ALL_TYPES, typeToScheme } from './types.js';
import type { ProbeResult, ProtocolType } from './types.js';
import type { Database } from './db.js';
import type { AppConfig } from './config.js';
import { logger } from './logger.js';

/** 从响应体中提取非空 IP 文本（IPv4 或 IPv6 均可）。取不到返回空串，视为失败。 */
export function extractIp(body: string): string {
  const lines = body.split(/\r?\n/);
  for (const ln of lines) {
    const t = ln.trim();
    if (t.startsWith('ip=')) {
      const val = t.slice(3).trim();
      if (isIpish(val)) return val;
    }
  }
  const v4 = /\b(?:\d{1,3}\.){3}\d{1,3}\b/.exec(body);
  if (v4) return v4[0];
  const v6 = /(?:[0-9a-fA-F]{1,4}:){1,}[0-9a-fA-F:.]{1,}/.exec(body);
  return v6 ? v6[0] : '';
}

function isIpish(s: string): boolean {
  return /^[\d.:a-fA-F]+$/.test(s) && (s.includes('.') || s.includes(':'));
}

/** 根据协议类型构建对应的 http.Agent（可传给 https.request 的 agent 选项）。 */
function buildAgent(type: ProtocolType, ip: string, port: number, username?: string, password?: string): http.Agent {
  const auth = username !== undefined
    ? `${encodeURIComponent(username)}:${encodeURIComponent(password ?? '')}@`
    : '';
  const host = ip.includes(':') ? `[${ip}]` : ip;
  const proxyUri = `${typeToScheme(type)}://${auth}${host}:${port}`;
  let agent: http.Agent;
  switch (type) {
    case 1: // HTTP 代理：明文连接代理，通过 CONNECT 隧道访问 HTTPS 渠道
    case 2: // HTTPS 代理：代理端本身用 TLS
      agent = new HttpsProxyAgent(proxyUri) as unknown as http.Agent;
      break;
    case 3: // SOCKS4
    case 4: // SOCKS5
    default:
      agent = new SocksProxyAgent(proxyUri) as unknown as http.Agent;
      break;
  }

  // 代理不可用（对端断连、TLS 握手失败、超时等）时，agent 内部会抛出 error 事件。
  // 这些属于“代理自身的问题”，会由测活结果判定为失效，不应以未捕获错误刷系统日志。
  // 这里在 agent 实例上挂一个静默监听，把所有这类 error 吸收掉。
  if (typeof (agent as any).on === 'function') {
    (agent as any).on('error', () => {
      /* 静默：代理连通性错误处理完毕，不入系统日志 */
    });
  }

  return agent;
}

/** 超时错误：用于区分"代理握手卡死"与"渠道本身返回异常"两类失败。 */
class TimeoutError extends Error {}

/** 用给定 Agent 请求一个 HTTPS 渠道，返回 {status, body}。超时/失败则抛出。
 *  关键：硬超时直接 reject promise，绝不依赖 req.destroy 的副作用，
 *  保证任何情况下 promise 都会在 timeoutMs+1s 内 settle。 */
async function requestThrough(
  agent: http.Agent | undefined,
  url: string,
  timeoutMs: number,
  signal: AbortSignal,
  requireConnectTunnel: boolean,
): Promise<{ status: number; body: string; connectStatus: number | null }> {
  return new Promise((resolve, reject) => {
    let settled = false;
    let connectStatus: number | null = null;
    let hardTimer: NodeJS.Timeout | undefined;
    let req: http.ClientRequest | undefined;

    // 统一的收尾：保证只结算一次，并清理定时器与请求。
    const finish = (fn: () => void): void => {
      if (settled) return;
      settled = true;
      if (hardTimer) clearTimeout(hardTimer);
      fn();
      // 收尾后再尝试销毁请求，释放底层连接（即使已经 settle）
      try {
        req?.destroy();
      } catch {
        // 销毁失败忽略。
      }
    };

    req = https.request(
      url,
      {
        method: 'GET',
        agent,
        timeout: timeoutMs,
        signal,
        headers: { 'user-agent': 'accounts-proxies/0.1' },
      },
      (res) => {
        const chunks: Buffer[] = [];
        res.on('data', (c: Buffer) => chunks.push(c));
        res.on('error', (e) => finish(() => reject(e)));
        res.on('end', () =>
          finish(() => {
            if (requireConnectTunnel && connectStatus !== 200) {
              reject(new Error(`proxy-connect-status-${connectStatus ?? 'missing'}`));
              return;
            }
            resolve({
              status: res.statusCode ?? 0,
              body: Buffer.concat(chunks).toString('utf8'),
              connectStatus,
            });
          }),
        );
      },
    );

    // https-proxy-agent 会在代理返回 CONNECT 响应后触发该事件。
    // HTTP/HTTPS 代理必须明确返回 200，不能只凭最终响应正文判断隧道可用。
    req.once('proxyConnect', (info: { statusCode?: number }) => {
      connectStatus = info?.statusCode ?? null;
    });

    // 关键兜底：硬超时直接 reject promise，不依赖 req.destroy 的副作用。
    // 这样即使 req.destroy 后没有触发 error 事件（socket 已断、lookup 卡住等），
    // promise 也一定能 settle，绝不永久挂起。
    hardTimer = setTimeout(() => {
      finish(() => reject(new TimeoutError('hard-timeout')));
    }, timeoutMs + 1000);

    req.on('timeout', () => req.destroy(new TimeoutError('timeout')));
    req.on('error', (e) => finish(() => reject(e)));

    // 关键兜底：socket 连接层的错误吸附。
    // 坏代理在 TCP 连接 / TLS 握手 / SOCKS 握手阶段被对端断开时，
    // 底层 socket 会触发 'error' 事件；若此刻 socket 上没有监听者，
    // 该错误会一路冒泡到进程级 uncaughtException。
    req.on('socket', (sock) => {
      sock.on('error', () => {});
      if (typeof (sock as any).once === 'function') {
        (sock as any).once('error', () => {});
      }
    });

    req.end();
  });
}

let directEgressCache: { ip: string; expiresAt: number } | null = null;
let directEgressPending: Promise<string> | null = null;

/**
 * 获取当前服务容器的直连出口 IP，用于识别 Docker Desktop、VPN 或透明代理
 * 将任意目标地址接管后伪装成可用 CONNECT 代理的情况。
 */
async function getDirectEgressIp(cfg: AppConfig, timeoutMs: number, signal: AbortSignal): Promise<string> {
  const now = Date.now();
  if (directEgressCache && directEgressCache.expiresAt > now) return directEgressCache.ip;
  if (directEgressPending) return directEgressPending;

  directEgressPending = (async () => {
    for (const channel of [cfg.primaryChannel, cfg.backupChannel]) {
      try {
        const result = await requestThrough(undefined, channel, timeoutMs, signal, false);
        const ip = result.status === 200 ? extractIp(result.body) : '';
        if (ip !== '') {
          directEgressCache = { ip, expiresAt: Date.now() + 300_000 };
          return ip;
        }
      } catch {
        // 直连主渠道失败时尝试备用渠道；两个渠道都失败则按失败关闭。
      }
    }
    throw new Error('direct-egress-ip-unavailable');
  })();

  try {
    return await directEgressPending;
  } finally {
    directEgressPending = null;
  }
}

/**
 * 用给定代理探测渠道：先主渠道，仅当主渠道明确返回 404 时才回退备用渠道。
 *
 * 设计依据：主备渠道都是全球知名网站，理论上永远可用，所以请求结果反映的是
 * 代理本身的状态而非渠道状态：
 *  - 返回 200 且响应体含 IP → 代理可用
 *  - 返回 404               → 仅这种情况下是渠道自身的问题（页面被挪走等），
 *                             换备用渠道再试，避免误判代理失效
 *  - 超时 / 连接错误 / 其他非 200 → 都是代理不通，换备用渠道照样连不上，
 *                             直接判定失效，不浪费时间
 */
async function probeChannels(
  agent: http.Agent,
  cfg: AppConfig,
  timeoutMs: number,
  label: string,
  signal: AbortSignal,
  requireConnectTunnel: boolean,
  directEgressIp: string | null,
): Promise<{ ok: boolean; latencyMs: number; timedOut: boolean }> {
  const start = performance.now();
  const channels = [cfg.primaryChannel, cfg.backupChannel];
  let successfulChannels = 0;
  for (let i = 0; i < channels.length; i++) {
    try {
      const r = await requestThrough(agent, channels[i], timeoutMs, signal, requireConnectTunnel);
      const observedIp = extractIp(r.body);
      if (
        r.status === 200
        && observedIp !== ''
        && (!requireConnectTunnel || directEgressIp === null || observedIp !== directEgressIp)
      ) {
        successfulChannels += 1;
        // HTTP/HTTPS CONNECT 代理必须能访问两个独立 HTTPS 渠道。
        // 单一站点成功可能只是目标白名单或短暂放行，不能证明代理具备通用 HTTPS 能力。
        if (!requireConnectTunnel || successfulChannels === channels.length) {
          const ms = Math.round(performance.now() - start);
          return { ok: true, latencyMs: ms, timedOut: false };
        }
        continue;
      }
      if (requireConnectTunnel && observedIp !== '' && observedIp === directEgressIp) {
        logger.debug(`[测活异常样本] ${label} 代理出口与服务直连出口相同，判定为透明转发假阳性`);
        return { ok: false, latencyMs: Math.round(performance.now() - start), timedOut: false };
      }
      // SOCKS 保持原规则：主渠道仅在 404 时回退备用渠道。
      // HTTP/HTTPS 必须两个独立渠道均成功，任一渠道失败即判定本次失败。
      if (r.status !== 404 || requireConnectTunnel) {
        logger.debug(
          `[测活异常样本] ${label} 渠道${i + 1}失败: HTTP=${r.status} CONNECT=${r.connectStatus ?? 'missing'} 出口IP=${observedIp || 'missing'}`,
        );
        const ms = Math.round(performance.now() - start);
        return { ok: false, latencyMs: ms, timedOut: false };
      }
      logger.debug(`[测活异常样本] ${label} 渠道${i + 1}返回404，改用备用渠道`);
    } catch (e) {
      if (signal.aborted) throw signal.reason ?? e;
      // 超时 / 连接错误：代理不通，换备用渠道也连不上，直接判定失效
      const ms = Math.round(performance.now() - start);
      const isTimeout = e instanceof TimeoutError;
      logger.debug(
        `[测活异常样本] ${label} 渠道${i + 1}请求异常: ${isTimeout ? '超时' : String(e)}`,
      );
      return { ok: false, latencyMs: ms, timedOut: isTimeout };
    }
  }
  return { ok: false, latencyMs: Math.round(performance.now() - start), timedOut: false };
}

/** 对单个协议完整测活：按配置重试，直到成功或耗尽重试次数后销毁 agent。
 *  若某次判定为超时（代理握手卡死），说明代理大概率不通，立即跳出重试，不再浪费时间。 */
async function probeProtocol(
  ip: string,
  port: number,
  type: ProtocolType,
  cfg: AppConfig,
  timeoutMs: number,
  signal: AbortSignal,
  username?: string,
  password?: string,
): Promise<ProbeResult> {
  const typeName = typeToScheme(type);
  const label = `${ip}:${port}[${typeName}]`;
  const requireConnectTunnel = type === 1 || type === 2;
  // HTTP/HTTPS 的成功标准要求两条全新连接连续成功，因此即使通用重试配置为 1，
  // 也必须至少执行两次。否则稳定可用的 CONNECT 代理永远无法进入可用池。
  const maxAttempts = requireConnectTunnel ? Math.max(cfg.retry, 2) : cfg.retry;
  let directEgressIp: string | null = null;
  if (requireConnectTunnel) {
    try {
      directEgressIp = await getDirectEgressIp(cfg, timeoutMs, signal);
    } catch (e) {
      if (signal.aborted) throw signal.reason ?? e;
      logger.debug(`[测活异常样本] ${label} 无法确认服务直连出口，按失败处理: ${String(e)}`);
      return { ok: false, latencyMs: null };
    }
  }
  let consecutiveSuccesses = 0;
  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    const agent = buildAgent(type, ip, port, username, password);
    try {
      if (signal.aborted) throw signal.reason ?? new Error('probe-aborted');
      const r = await probeChannels(
        agent,
        cfg,
        timeoutMs,
        label,
        signal,
        requireConnectTunnel,
        directEgressIp,
      );
      if (r.ok) {
        // HTTP/HTTPS 代理必须使用两条全新连接连续成功，避免一次偶发成功的
        // 不稳定代理立刻进入可用池。SOCKS 保持一次成功即可。
        consecutiveSuccesses += 1;
        if (!requireConnectTunnel || consecutiveSuccesses >= 2) {
          return { ok: true, latencyMs: r.latencyMs };
        }
        continue;
      }
      consecutiveSuccesses = 0;
      // 超时即代理死了，重试无意义，立即返回失败
      if (r.timedOut) {
        return { ok: false, latencyMs: null };
      }
    } finally {
      if (typeof (agent as any).destroy === 'function') {
        try {
          (agent as any).destroy();
        } catch {
          // 释放失败可忽略，连接池最终会被回收。
        }
      }
    }
  }
  return { ok: false, latencyMs: null };
}

/**
 * 对单个代理测活：并发探测当前启用的三种协议，再一次性写入完整结果。
 * 单个协议即使出现未预期异常，也会转换为失败结果，保证全部协议索引
 * 都会被本轮结果覆盖，避免代理状态与 available 索引不一致。
 */
export async function probeProxy(args: {
  addrKey: string;
  ip: string;
  port: number;
  username?: string;
  password?: string;
  db: Database;
  cfg: AppConfig;
  signal: AbortSignal;
}): Promise<{ ok: boolean; protocols: string[]; elapsedMs: number; consecutiveFails: number }> {
  const { addrKey, ip, port, username, password, db, cfg, signal } = args;
  const timeoutMs = cfg.timeout * 1000;
  const startedAt = performance.now();

  if (signal.aborted) throw signal.reason ?? new Error('probe-aborted');
  const prevConsecFail = await db.getConsecutiveFail(addrKey);
  if (signal.aborted) throw signal.reason ?? new Error('probe-aborted');

  const results = await Promise.all(
    ALL_TYPES.map(async (type): Promise<{ type: ProtocolType; ok: boolean }> => {
      try {
        const r = await probeProtocol(ip, port, type, cfg, timeoutMs, signal, username, password);
        return { type, ok: r.ok };
      } catch (e) {
        if (signal.aborted) throw signal.reason ?? e;
        logger.debug(`[测活异常样本] ${addrKey}[${typeToScheme(type)}] 未预期异常: ${String(e)}`);
        return { type, ok: false };
      }
    }),
  );

  const anyOk = results.some((r) => r.ok);
  const consecFails = anyOk ? 0 : prevConsecFail + 1;
  // 任务级硬超时后禁止旧任务继续落库，避免过期结果覆盖后续测活结果。
  if (signal.aborted) throw signal.reason ?? new Error('probe-aborted');
  await db.finalizeProbe(addrKey, results, anyOk, consecFails, cfg);

  const elapsed = Math.round(performance.now() - startedAt);
  const okTypes = results
    .map((r) => (r.ok ? typeToScheme(r.type) : null))
    .filter(Boolean)
    .join(',');
  return {
    ok: anyOk,
    protocols: okTypes ? okTypes.split(',') : [],
    elapsedMs: elapsed,
    consecutiveFails: consecFails,
  };
}
