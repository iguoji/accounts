import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { SOURCE_FILE, type AppConfig } from './config.js';
import { logger } from './logger.js';
import type { Database, SourceState } from './db.js';
import { formatAddrKey, type ProxyAddr } from './types.js';
import { parseProxyLine } from './parse.js';

const IDLE_WAIT_MS = 1000;

export function loadUrls(yamlPath: string = SOURCE_FILE): string[] {
  const raw = readFileSync(yamlPath, 'utf8');
  const urls = new Set<string>();
  for (const line of raw.split(/\r?\n/)) {
    const value = line.trim();
    if (!value || value.startsWith('#') || value.startsWith('--')) continue;
    const matched = /^-\s+(-?\s*)?((?:https?:|socks4:|socks5:)\/\/\S+)/.exec(value);
    if (matched) urls.add(matched[2].replace(/["']/g, ''));
  }
  return [...urls];
}

export function normalizeSourceUrl(rawUrl: string): string {
  const url = new URL(rawUrl);
  url.hash = '';
  url.hostname = url.hostname.toLowerCase();
  if ((url.protocol === 'https:' && url.port === '443') || (url.protocol === 'http:' && url.port === '80')) {
    url.port = '';
  }
  return url.toString();
}

export function sourceId(url: string): string {
  return createHash('sha256').update(normalizeSourceUrl(url)).digest('hex');
}

function hash(value: string): string {
  return createHash('sha256').update(value).digest('hex');
}

function parseAddresses(content: string): Map<string, ProxyAddr> {
  const addresses = new Map<string, ProxyAddr>();
  for (const line of content.split(/\r?\n/)) {
    const parsed = parseProxyLine(line);
    if (parsed) addresses.set(formatAddrKey(parsed), parsed);
  }
  return addresses;
}

function median(values: number[]): number {
  const sorted = [...values].sort((a, b) => a - b);
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0
    ? Math.round((sorted[middle - 1] + sorted[middle]) / 2)
    : sorted[middle];
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(Math.max(value, min), max);
}

function scheduleAfterSuccess(source: SourceState, changed: boolean, now: number, cfg: AppConfig) {
  const base = source.changeIntervals.length > 0
    ? median(source.changeIntervals)
    : cfg.sourceDefaultInterval * 1000;
  const delay = changed ? base : base * Math.pow(1.5, Math.min(source.consecutiveUnchanged, 6));
  return {
    at: now + clamp(Math.round(delay), cfg.sourceMinInterval * 1000, cfg.sourceMaxInterval * 1000),
    reason: changed
      ? (source.changeIntervals.length > 0 ? '变化间隔中位数' : '默认观察间隔')
      : '连续未变化退避',
  };
}

function scheduleAfterFailure(source: SourceState, now: number, cfg: AppConfig) {
  const delay = cfg.sourceFailureInterval * 1000 * Math.pow(2, Math.min(source.consecutiveFail - 1, 6));
  return {
    at: now + Math.min(delay, cfg.sourceMaxInterval * 1000),
    reason: '请求失败退避',
  };
}

async function processSource(db: Database, cfg: AppConfig, source: SourceState): Promise<void> {
  const startedAt = Date.now();
  const hostname = new URL(source.url).hostname.toLowerCase();
  const cooldownUntil = await db.getSiteCooldown(hostname);
  if (cooldownUntil > startedAt) {
    source.nextCheckAt = cooldownUntil;
    await db.saveSource(source, 'site_cooldown');
    await db.appendSourceLog(source.id, {
      checkedAt: startedAt,
      result: 'site_cooldown',
      httpStatus: 0,
      contentChanged: false,
      proxySetChanged: false,
      contentHash: source.contentHash,
      proxySetHash: source.proxySetHash,
      validProxyCount: 0,
      addedProxyCount: 0,
      revivedProxyCount: 0,
      elapsedMs: Date.now() - startedAt,
      nextCheckAt: source.nextCheckAt,
      scheduleReason: '同站点仍处于冷却期',
      error: '',
    }, cfg.sourceLogMaxLength);
    logger.debug(`[采集] host=${hostname} result=site_cooldown next=${new Date(source.nextCheckAt).toISOString()} reason=同站点仍处于冷却期`);
    return;
  }

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), cfg.fetchTimeout * 1000);
  let result = 'failed';
  let contentChanged = false;
  let proxySetChanged = false;
  let validProxyCount = 0;
  let addedProxyCount = 0;
  let revivedProxyCount = 0;
  let scheduleReason = '';
  let error = '';
  let currentHttpStatus = 0;

  try {
    const headers: Record<string, string> = {};
    if (source.etag) headers['If-None-Match'] = source.etag;
    if (source.lastModified) headers['If-Modified-Since'] = source.lastModified;
    const response = await fetch(source.url, { headers, signal: controller.signal });
    const now = Date.now();
    currentHttpStatus = response.status;
    source.lastCheckedAt = now;
    source.lastHttpStatus = currentHttpStatus;

    if (response.status === 304) {
      result = 'not_modified';
      source.consecutiveUnchanged++;
      source.consecutiveFail = 0;
      const next = scheduleAfterSuccess(source, false, now, cfg);
      source.nextCheckAt = next.at;
      scheduleReason = next.reason;
    } else if (response.ok) {
      const content = await response.text();
      const nextContentHash = hash(content);
      contentChanged = nextContentHash !== source.contentHash;
      const addresses = parseAddresses(content);
      validProxyCount = addresses.size;
      if (validProxyCount === 0) {
        throw new Error('响应成功，但没有解析到有效代理');
      }
      const nextProxySetHash = hash([...addresses.keys()].sort().join('\n'));
      proxySetChanged = nextProxySetHash !== source.proxySetHash;

      if (proxySetChanged) {
        const stored = await db.upsertAddresses(addresses);
        addedProxyCount = stored.newCount;
        revivedProxyCount = stored.revivedCount;
        await db.markCollectCompleted();
      }

      if (contentChanged) {
        if (source.lastChangedAt > 0) {
          source.changeIntervals = [...source.changeIntervals, now - source.lastChangedAt].slice(-10);
        }
        source.lastChangedAt = now;
        source.consecutiveUnchanged = 0;
      } else {
        source.consecutiveUnchanged++;
      }

      source.contentHash = nextContentHash;
      source.proxySetHash = nextProxySetHash;
      source.etag = response.headers.get('etag') || '';
      source.lastModified = response.headers.get('last-modified') || '';
      source.consecutiveFail = 0;
      result = proxySetChanged ? 'proxy_set_changed' : contentChanged ? 'content_changed' : 'unchanged';
      const next = scheduleAfterSuccess(source, contentChanged, now, cfg);
      source.nextCheckAt = next.at;
      scheduleReason = next.reason;
    } else {
      throw new Error(`HTTP ${response.status}`);
    }
  } catch (caught) {
    const now = Date.now();
    source.lastCheckedAt = now;
    source.consecutiveFail++;
    error = caught instanceof Error ? caught.message : String(caught);
    source.lastHttpStatus = currentHttpStatus;
    if (currentHttpStatus === 403 || currentHttpStatus === 429) {
      const until = now + cfg.sourceSiteCooldown * 1000;
      await db.setSiteCooldown(hostname, until);
      source.nextCheckAt = until;
      scheduleReason = `站点 HTTP ${currentHttpStatus} 冷却`;
    } else {
      const next = scheduleAfterFailure(source, now, cfg);
      source.nextCheckAt = next.at;
      scheduleReason = next.reason;
    }
  } finally {
    clearTimeout(timer);
  }

  await db.saveSource(source, result, error);
  await db.appendSourceLog(source.id, {
    checkedAt: source.lastCheckedAt,
    result,
    httpStatus: source.lastHttpStatus,
    contentChanged,
    proxySetChanged,
    contentHash: source.contentHash,
    proxySetHash: source.proxySetHash,
    validProxyCount,
    addedProxyCount,
    revivedProxyCount,
    elapsedMs: Date.now() - startedAt,
    nextCheckAt: source.nextCheckAt,
    scheduleReason,
    error,
  }, cfg.sourceLogMaxLength);
  const elapsedMs = Date.now() - startedAt;
  logger.info(`源头 ${source.url} 检查完成：${result}，新增 ${addedProxyCount}，复活 ${revivedProxyCount}`);
  logger.debug(`[采集] host=${hostname} result=${result} http=${source.lastHttpStatus} elapsed=${elapsedMs}ms valid=${validProxyCount} contentChanged=${contentChanged} proxySetChanged=${proxySetChanged} added=${addedProxyCount} revived=${revivedProxyCount} next=${new Date(source.nextCheckAt).toISOString()} reason=${scheduleReason}${error ? ` error=${error}` : ''}`);
}

export interface CollectionHandle {
  stop: () => void;
}

export async function startCollectionLoop(db: Database, cfg: AppConfig): Promise<CollectionHandle> {
  const urls = loadUrls().map(normalizeSourceUrl);
  await db.syncSources(urls.map((url) => ({ id: sourceId(url), url })), Date.now());
  const restored = await db.repairSourceQueue(Date.now());
  logger.info(`已同步 ${urls.length} 个兜底源头，恢复 ${restored} 个漏失调度，采集将按源头历史串行调度`);
  let stopped = false;

  const loop = async (): Promise<void> => {
    while (!stopped) {
      let currentSourceId: string | null = null;
      try {
        const id = await db.popDueSource(Date.now());
        if (!id) {
          await new Promise((resolve) => setTimeout(resolve, IDLE_WAIT_MS));
          continue;
        }
        currentSourceId = id;
        const source = await db.getSource(id);
        if (!source || !source.enabled) continue;
        await processSource(db, cfg, source);
      } catch (error) {
        logger.warn(`源头采集调度异常，1 秒后重试: ${String(error)}`);
        if (currentSourceId) {
          try {
            await db.requeueSource(
              currentSourceId,
              Date.now() + cfg.sourceFailureInterval * 1000,
            );
          } catch (requeueError) {
            logger.error(`源头 ${currentSourceId} 重新入队失败，将在服务重启时恢复: ${String(requeueError)}`);
          }
        }
        await new Promise((resolve) => setTimeout(resolve, IDLE_WAIT_MS));
      }
    }
  };

  void loop();
  return { stop: () => { stopped = true; } };
}
