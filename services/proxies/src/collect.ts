/**
 * 采集：定期从 source.yaml 中的源列表批量下载，逐行解析并去重，
 * 每个数据源独立流式解析，再按固定并发数下载；全部完成后统一合并并入库。
 * 采集失败不影响主流程，等待下一次采集。
 */
import { readFileSync } from 'node:fs';
import { Readable } from 'node:stream';
import { createInterface } from 'node:readline';
import { SOURCE_FILE } from './config.js';
import { logger } from './logger.js';
import type { Database } from './db.js';
import { formatAddrKey, type ProxyAddr } from './types.js';
import { parseProxyLine } from './parse.js';

/** 从 source.yaml 解析出 urls 列表（仅解析所需的极简 YAML 结构）。 */
export function loadUrls(yamlPath: string = SOURCE_FILE): string[] {
  const raw = readFileSync(yamlPath, 'utf8');
  const urls: string[] = [];
  for (const line of raw.split(/\r?\n/)) {
    const t = line.trim();
    if (!t || t.startsWith('#') || t.startsWith('--')) continue;
    // 识别 "- https://..." 形式
    const m = /^-\s+(-?\s*)?((?:https?:|socks4:|socks5:)\/\/\S+)/.exec(t);
    if (m) urls.push(m[2].replace(/['"]/g, ''));
  }
  return urls;
}

/** 从单个 URL 下载到本地临时文件并逐行解析合法公网地址，写入 addrs 集合。 */
async function collectFromUrl(
  url: string,
  timeoutMs: number,
  log: (s: string) => void,
): Promise<{ url: string; total: number; valid: number; addrs: Map<string, ProxyAddr> }> {
  const ctl = new AbortController();
  const timer = setTimeout(() => ctl.abort(), timeoutMs);
  const addrs = new Map<string, ProxyAddr>();
  let total = 0;
  let valid = 0;
  try {
    logger.debug(`[采集] 开始下载 ${url}`);
    const res = await fetch(url, { signal: ctl.signal });
    if (!res.ok || !res.body) {
      logger.debug(`[采集] ${url} 返回状态 ${res.status}，跳过`);
      log(`数据源 ${url} 返回状态 ${res.status}，跳过`);
      return { url, total, valid, addrs };
    }

    // 流式逐行处理，避免一次性加载巨型列表到内存。
    // 全局 fetch 的 res.body 是 Web ReadableStream，需先转成 Node Readable 才能交给 readline。
    const rl = createInterface({
      input: Readable.fromWeb(res.body as ReadableStream<Uint8Array>),
      crlfDelay: Infinity,
    });

    for await (const line of rl) {
      total++;
      const parsed = parseProxyLine(line);
      if (!parsed) continue;
      // 去重 key：同一 ip:port 但不同认证视为不同代理
      const key = formatAddrKey(parsed);
      addrs.set(key, parsed);
      valid++;
    }
    logger.debug(`[采集] ${url} 解析完成：读取 ${total} 行，合法 ${valid} 个`);
  } catch (e) {
    logger.debug(`[采集] 下载/解析 ${url} 失败: ${String(e)}`);
    log(`下载/解析 ${url} 失败: ${String(e)}`);
  } finally {
    clearTimeout(timer);
  }
  return { url, total, valid, addrs };
}

/** 按固定数量启动采集任务，避免一次并发请求全部数据源。 */
async function collectWithConcurrency(
  urls: string[],
  timeoutMs: number,
  concurrency: number,
  log: (s: string) => void,
): Promise<Array<{ url: string; total: number; valid: number; addrs: Map<string, ProxyAddr> }>> {
  const results = new Array<Awaited<ReturnType<typeof collectFromUrl>>>(urls.length);
  let nextIndex = 0;

  const worker = async (): Promise<void> => {
    while (nextIndex < urls.length) {
      const index = nextIndex++;
      results[index] = await collectFromUrl(urls[index], timeoutMs, log);
    }
  };

  const workerCount = Math.min(Math.max(concurrency, 1), urls.length);
  await Promise.all(Array.from({ length: workerCount }, () => worker()));
  return results;
}

/**
 * 执行一轮完整采集。跨 URL 去重（只对源列表内去重）。
 */
export async function runCollection(
  urls: string[],
  config: { fetchTimeout: number; fetchConcurrency: number },
  db: Database,
  log: (s: string) => void = logger.info,
): Promise<number> {
  const addrs = new Map<string, ProxyAddr>();
  const results = await collectWithConcurrency(
    urls,
    config.fetchTimeout * 1000,
    config.fetchConcurrency,
    log,
  );

  // 异步任务只写各自的 Map；此处按数据源原顺序单线程合并，保持跨来源去重语义稳定。
  for (const result of results) {
    for (const [key, addr] of result.addrs) addrs.set(key, addr);
    log(`源 ${result.url}: 读取 ${result.total} 行，合法公网地址 ${result.valid} 个`);
  }
  if (addrs.size === 0) {
    log('本轮未采集到任何地址，跳过入库');
    return 0;
  }
  const { newCount, revivedCount } = await db.upsertAddresses(addrs);
  await db.markCollectCompleted();
  log(`采集入库完成：新增 ${newCount} 个，复活 ${revivedCount} 个，总计 ${addrs.size} 个去重地址`);
  return addrs.size;
}
