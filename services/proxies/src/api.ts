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

  return {
    protocols,
    page: parsePositiveInt(u.searchParams.get('page'), 1),
    count: parsePositiveInt(u.searchParams.get('count'), 20),
  };
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

    if (req.method !== 'GET') {
      writeJson(res, 405, { error: 'method not allowed' });
      return;
    }

    try {
      if (pathname === '/proxies') {
        const q = parseQuery(url);
        const items = await db.listAvailable(q.protocols, q.page, q.count);
        writeJson(res, 200, items);
        return;
      }

      if (pathname === '/proxy') {
        const q = parseQuery(url);
        const pick = await db.randomAvailable(q.protocols);
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
      logger.error(`API 处理失败(${url}): ${String(e)}`);
      writeJson(res, 500, { error: 'internal error' });
    }
  });

  server.listen(cfg.port, cfg.host, () => {
    logger.info(`HTTP API 已启动: http://${cfg.host}:${cfg.port} (GET /proxies, /proxy, /stats)`);
  });
  return server;
}
