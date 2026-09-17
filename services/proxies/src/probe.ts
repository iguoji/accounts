/**
 * 测活：对单个代理分别以 HTTP / HTTPS / SOCKS4 / SOCKS5 四个协议并发探测，
 * 成功标准为「通过代理访问测活渠道，返回 HTTP 200 且响应体含非空 IP 文本」。
 * 主渠道失败（含 404）后回退到备用渠道。
 *
 * 实现说明：
 * - 每个协议类型选对应的 http.Agent 子类（http-proxy-agent / https-proxy-agent /
 *   socks-proxy-agent），再配合 Node 原生 https.request 发起请求。
 * - 这些都是 http.Agent 家族（agent-base），与 Node 原生网络栈直接配合，
 *   不通过 undici/fetch 的 dispatcher 方式。
 */
import * as http from 'node:http';
import * as https from 'node:https';
import { HttpsProxyAgent } from 'https-proxy-agent';
import { SocksProxyAgent } from 'socks-proxy-agent';
import { ALL_TYPES, typeToScheme } from './types.js';
import type { ProbeResult, ProtocolType, ProxyRecord } from './types.js';
import type { Database } from './db.js';
import { fmtTime } from './db.js';
import type { AppConfig } from './config.js';

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
function buildAgent(type: ProtocolType, ip: string, port: number): http.Agent {
  const proxyUri = `${typeToScheme(type)}://${ip}:${port}`;
  let agent: http.Agent;
  switch (type) {
    case 1: // HTTP 代理：对 https 渠道走 CONNECT 隧道
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

/** 用给定 Agent 请求一个 HTTPS 渠道，返回 {status, body}。超时/失败则抛出。 */
async function requestThrough(
  agent: http.Agent,
  url: string,
  timeoutMs: number,
): Promise<{ status: number; body: string }> {
  return new Promise((resolve, reject) => {
    const req = https.request(
      url,
      {
        method: 'GET',
        agent,
        timeout: timeoutMs,
        headers: { 'user-agent': 'accounts-proxies/0.1' },
      },
      (res) => {
        const chunks: Buffer[] = [];
        res.on('data', (c: Buffer) => chunks.push(c));
        res.on('error', reject);
        res.on('end', () => resolve({ status: res.statusCode ?? 0, body: Buffer.concat(chunks).toString('utf8') }));
      },
    );
    req.on('timeout', () => req.destroy(new Error('timeout')));
    req.on('error', reject);

    // 关键兜底：当代理连接的底层 socket 在 TLS 握手前就被对端断开时，
    // 它会在没有监听者的情况下触发未捕获的 'error' 事件，直接导致进程崩溃。
    // 这里为 socket 预先挂上一个 no-op 监听，把这个错误吸附住不往外冒。
    req.on('socket', (sock) => {
      // 关键兜底：当代理连接的底层 socket 在 TLS 握手前就被对端断开时，
      // 它会在没有监听者的情况下触发未捕获的 'error' 事件，直接导致进程崩溃。
      // 这里为 socket 预先挂一个 no-op 监听，把这个错误吸附住不往外冒。
      sock.on('error', () => {});
    });

    req.end();
  });
}

/**
 * 用给定代理探测渠道：先主渠道，非 200 或体无 IP 则回退备用。
 * 任一成功即判定可用并返回延迟。
 */
async function probeChannels(agent: http.Agent, cfg: AppConfig, timeoutMs: number): Promise<{ ok: boolean; latencyMs: number }> {
  const start = performance.now();
  for (const channel of [cfg.primaryChannel, cfg.backupChannel]) {
    try {
      const r = await requestThrough(agent, channel, timeoutMs);
      if (r.status === 200 && extractIp(r.body) !== '') {
        return { ok: true, latencyMs: Math.round(performance.now() - start) };
      }
      // 主渠道 404 等异常：继续尝试备用渠道
    } catch {
      // 渠道自身故障：继续尝试备用渠道
    }
  }
  return { ok: false, latencyMs: Math.round(performance.now() - start) };
}

/** 对单个协议完整测活：按配置重试，直到成功或耗尽重试次数后销毁 agent。 */
async function probeProtocol(
  proxy: ProxyRecord,
  type: ProtocolType,
  cfg: AppConfig,
  timeoutMs: number,
): Promise<ProbeResult> {
  const agent = buildAgent(type, proxy.ip, proxy.port);
  try {
    for (let attempt = 0; attempt < cfg.retry; attempt++) {
      const r = await probeChannels(agent, cfg, timeoutMs);
      if (r.ok) return { ok: true, latencyMs: r.latencyMs };
    }
    return { ok: false, latencyMs: null };
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

/** 对单个代理测活：并发探测四种协议，维护协议表并维护代理表。 */
export async function probeProxy(proxy: ProxyRecord, cfg: AppConfig, db: Database): Promise<void> {
  const timeoutMs = cfg.timeout * 1000;
  const started = Date.now();

  const results = await Promise.all(
    ALL_TYPES.map(async (type) => {
      const r = await probeProtocol(proxy, type, cfg, timeoutMs);
      db.updateProtocol(proxy.ip, proxy.port, type, r.ok, r.latencyMs);
      return r;
    }),
  );

  const anyOk = results.some((r) => r.ok);
  const consecFails = anyOk ? 0 : proxy.consecutiveFail + 1;
  // 失效时指数退避：max(INTERVAL, INTERVAL_BASE^连续失败次数 * INTERVAL)
  const nextIntervalMs = anyOk
    ? cfg.interval * 1000
    : Math.max(cfg.interval, Math.pow(cfg.intervalBase, consecFails) * cfg.interval) * 1000;
  const deletedAt = !anyOk && consecFails >= cfg.maxConsecutiveFail ? fmtTime(new Date()) : null;

  db.updateProxy(proxy.ip, proxy.port, anyOk ? 1 : 0, started, Date.now() + nextIntervalMs, consecFails, deletedAt);
  // 说明：测活结果（可用/失效）只入数据库，不写系统日志。
  // 系统日志只保留真正的运行时故障（见 README“日志保存的是系统相关信息，并非测活结果”）。
}