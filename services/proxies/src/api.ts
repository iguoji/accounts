/**
 * 对内 HTTP API（异步，对接 Redis 数据层）：
 *  - GET /proxies  返回所有可用代理分页列表
 *     参数: protocols(可空/可多次/逗号分隔), page(默认1), count(默认20)
 *     返回: JSON 数组 [{ip, port, protocols:[..]}]
 *  - GET /proxy    随机返回一个可用代理，无则返回空对象
 *     参数: protocols
 *     返回: JSON 对象 {ip, port, protocols:[..]} 或 {}
 *  - GET /stats    返回系统运行心跳信息
 *     proxy_total = proxy_available + proxy_unchecked + proxy_cooldown
 *     proxy_dead 为已软删的代理数（单独给出，不计入上面等式）
 *     proxy_checking 为正在测活中的代理数（运行时读数，与 DB 分段有重叠）
 *     另含最近采集/测活时间、累计采集次数与累计测活次数
 */
import { createServer } from 'node:http';
import type { Server, IncomingMessage, ServerResponse } from 'node:http';
import { NAME_TO_TYPE } from './types.js';
import type { Database } from './db.js';
import type { AppConfig } from './config.js';
import { logger } from './logger.js';

interface ApiQuery {
  protocols: number[]; // 空数组 = 全部
  page: number;
  count: number;
  domain?: string;
}

class InvalidQueryError extends Error {}

interface FeedbackBody {
  ip: string;
  port: number;
  domain: string;
  blocked_seconds: number;
  reason?: string;
}

function normalizeDomain(input: string): string | null {
  const value = input.trim();
  if (!value) return null;
  try {
    const url = new URL(value.includes('://') ? value : `http://${value}`);
    const hostname = url.hostname.toLowerCase().replace(/\.$/, '');
    return hostname || null;
  } catch {
    return null;
  }
}

/** 把整数型查询参数解析为 >= 1 的整数，空或非法用默认值。 */
function parsePositiveInt(v: string | null, def: number): number {
  if (v === null || v === '') return def;
  const n = Number(v);
  if (!Number.isInteger(n) || n <= 0) return def;
  return n;
}

function parseQuery(rawUrl: string): ApiQuery {
  const u = new URL(rawUrl, 'http://localhost');
  const names = u.searchParams
    .getAll('protocols')
    .flatMap((v) => v.split(/[,，]/))
    .map((s) => s.trim().toLowerCase())
    .filter(Boolean);

  const protocols: number[] = [];
  for (const n of names) {
    const t = NAME_TO_TYPE[n];
    if (t !== undefined) protocols.push(t);
  }
  if (names.length > 0 && protocols.length === 0) {
    throw new InvalidQueryError('invalid protocols');
  }

  const hasDomain = u.searchParams.has('domain');
  const domain = hasDomain ? normalizeDomain(u.searchParams.get('domain') ?? '') : null;
  if (hasDomain && !domain) throw new InvalidQueryError('invalid domain');

  return {
    protocols,
    page: parsePositiveInt(u.searchParams.get('page'), 1),
    count: parsePositiveInt(u.searchParams.get('count'), 20),
    ...(domain ? { domain } : {}),
  };
}

async function readJsonBody(req: IncomingMessage, maxBytes = 16 * 1024): Promise<unknown> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of req) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    size += buffer.length;
    if (size > maxBytes) throw new Error('request body too large');
    chunks.push(buffer);
  }
  return JSON.parse(Buffer.concat(chunks).toString('utf8'));
}

function parseFeedbackBody(raw: unknown): FeedbackBody | null {
  if (!raw || typeof raw !== 'object') return null;
  const body = raw as Record<string, unknown>;
  const ip = typeof body.ip === 'string' ? body.ip.trim() : '';
  const port = Number(body.port);
  const domain = typeof body.domain === 'string' ? normalizeDomain(body.domain) : null;
  const blockedSeconds = Number(body.blocked_seconds);
  const reason = typeof body.reason === 'string'
    ? body.reason.trim().replace(/[\r\n\t]+/g, ' ')
    : undefined;
  if (!ip || !domain || !Number.isInteger(port) || port < 1 || port > 65535) return null;
  if (!Number.isSafeInteger(blockedSeconds) || blockedSeconds <= 0) return null;
  if (reason !== undefined && reason.length > 1000) return null;
  return { ip, port, domain, blocked_seconds: blockedSeconds, ...(reason ? { reason } : {}) };
}

function writeJson(res: ServerResponse, code: number, body: unknown): void {
  const payload = JSON.stringify(body);
  res.writeHead(code, {
    'content-type': 'application/json; charset=utf-8',
    'content-length': Buffer.byteLength(payload),
  });
  res.end(payload);
}

export function createApiServer(db: Database, cfg: AppConfig, getChecking?: () => number): Server {
  const server = createServer(async (req: IncomingMessage, res: ServerResponse) => {
    const url = req.url ?? '/';
    const pathname = new URL(url, 'http://localhost').pathname;

    if (pathname === '/feedback' && req.method === 'POST') {
      try {
        const feedback = parseFeedbackBody(await readJsonBody(req));
        if (!feedback) {
          writeJson(res, 400, { error: 'invalid feedback' });
          return;
        }
        if (!(await db.hasProxyEndpoint(feedback.ip, feedback.port))) {
          writeJson(res, 404, { error: 'proxy not found' });
          return;
        }
        const blockedUntil = await db.blockForDomain(
          feedback.ip,
          feedback.domain,
          feedback.blocked_seconds,
        );
        logger.info(`收到业务封禁反馈: ${feedback.ip}:${feedback.port} domain=${feedback.domain} blocked_until=${blockedUntil}${feedback.reason ? ` reason=${feedback.reason}` : ''}`);
        writeJson(res, 200, {
          ip: feedback.ip,
          port: feedback.port,
          domain: feedback.domain,
          blocked_until: blockedUntil,
        });
      } catch (e) {
        logger.error(`反馈接口处理失败: ${String(e)}`);
        writeJson(res, 400, { error: 'invalid request body' });
      }
      return;
    }

    if (req.method !== 'GET') {
      writeJson(res, 405, { error: 'method not allowed' });
      return;
    }

    try {
      if (pathname === '/proxies') {
        const q = parseQuery(url);
        const items = await db.listAvailable(q.protocols, q.page, q.count, q.domain);
        writeJson(res, 200, items);
        return;
      }

      if (pathname === '/proxy') {
        const q = parseQuery(url);
        const pick = await db.randomAvailable(q.protocols, q.domain);
        writeJson(res, 200, pick ?? {});
        return;
      }

      if (pathname === '/stats') {
        const stats = await db.getStats();
        const checking = getChecking ? getChecking() : 0;
        writeJson(res, 200, { ...stats, proxy_checking: checking });
        return;
      }

      writeJson(res, 404, { error: 'not found' });
    } catch (e) {
      if (e instanceof InvalidQueryError) {
        writeJson(res, 400, { error: e.message });
        return;
      }
      logger.error(`API 处理失败(${url}): ${String(e)}`);
      writeJson(res, 500, { error: 'internal error' });
    }
  });

  server.listen(cfg.port, cfg.host, () => {
    logger.info(`HTTP API 已启动: http://${cfg.host}:${cfg.port} (GET /proxies, /proxy, /stats; POST /feedback)`);
  });
  return server;
}
