/**
 * 采集：定期从 source.yaml 中的源列表批量下载，逐行解析并去重，
 * 得到 (ip, port) 集合后按序分批 upsert 到数据库。
 * 采集失败不影响主流程，整体回滚并等待下一次采集。
 */
import { readFileSync } from 'node:fs';
import { Readable } from 'node:stream';
import { createInterface } from 'node:readline';
import { SOURCE_FILE } from './config.js';
import { logger } from './logger.js';
import type { Database } from './db.js';
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
  addrs: Set<string>,
  log: (s: string) => void,
): Promise<{ total: number; valid: number }> {
  const ctl = new AbortController();
  const timer = setTimeout(() => ctl.abort(), timeoutMs);
  let total = 0;
  let valid = 0;
  try {
    const res = await fetch(url, { signal: ctl.signal });
    if (!res.ok || !res.body) {
      log(`数据源 ${url} 返回状态 ${res.status}，跳过`);
      return { total, valid };
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
      addrs.add(`${parsed.ip}:${parsed.port}`);
      valid++;
    }
  } catch (e) {
    log(`下载/解析 ${url} 失败: ${String(e)}`);
  } finally {
    clearTimeout(timer);
  }
  return { total, valid };
}

/**
 * 执行一轮完整采集。跨 URL 去重（只对源列表内去重）。
 */
export async function runCollection(
  urls: string[],
  config: { fetchTimeout: number },
  db: Database,
  log: (s: string) => void = logger.info,
): Promise<number> {
  const addrs = new Set<string>();
  for (const url of urls) {
    const r = await collectFromUrl(url, config.fetchTimeout * 1000, addrs, log);
    log(`源 ${url}: 读取 ${r.total} 行，合法公网地址 ${r.valid} 个`);
  }
  if (addrs.size === 0) {
    log('本轮未采集到任何地址，跳过入库');
    return 0;
  }
  db.upsertAddresses(addrs);
  log(`采集入库完成，共 ${addrs.size} 个去重地址`);
  return addrs.size;
}