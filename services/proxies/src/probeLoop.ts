/**
 * 测活调度主循环（README 伪代码 §测活 的实现）。
 * 有界并发队列维护「正在测活」的代理集合；每轮从库里取出满足条件（未软删、
 * next_check_at 已到期、不在队列中）的代理入队并行测活。
 */
import { BoundedQueue } from './pool.js';
import { probeProxy } from './probe.js';
import type { AppConfig } from './config.js';
import type { Database } from './db.js';
import { logger } from './logger.js';
import type { ProxyRecord } from './types.js';

const ONE_SECOND = 1000;

/** 启动测活调度循环。返回一个可调用的停止函数。 */
export function startProbeLoop(db: Database, cfg: AppConfig): () => void {
  const queue = new BoundedQueue<ProxyRecord>(cfg.probeConcurrency);
  let stopped = false;

  const loop = async (): Promise<void> => {
    while (!stopped) {
      if (queue.isFull()) {
        await queue.waitForSlot();
        continue;
      }

      const now = Date.now();
      // 排除正在测活（排队 + 执行中）的代理，key 为 "ip:port"。
      const exclude = new Set<string>();
      for (const p of queue.busy()) exclude.add(`${p.ip}:${p.port}`);
      const candidates = db.getProxiesToCheck(now, exclude, queue.idle());
      if (candidates.length === 0) {
        await new Promise((r) => setTimeout(r, ONE_SECOND));
        continue;
      }

      for (const proxy of candidates) {
        if (queue.isFull()) break;
        void queue.enqueue(proxy, async () => {
          try {
            await probeProxy(proxy, cfg, db);
          } catch (e) {
            logger.warn(`测活 ${proxy.ip}:${proxy.port} 发生异常: ${String(e)}`);
          }
        });
      }
    }
  };

  loop().catch((e) => logger.error(`测活循环终止: ${String(e)}`));

  return () => {
    stopped = true;
  };
}