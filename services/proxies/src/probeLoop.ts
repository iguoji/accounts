/**
 * 测活调度主循环。
 * 使用 Redis Sorted Set 作为调度队列：原子取出到期候选、测活后重新放回。
 * 因为取出即移除（Lua 原子操作），不需要额外的 exclude 集合来排除"正在测活"的代理。
 */
import { probeProxy } from './probe.js';
import type { AppConfig } from './config.js';
import type { Database } from './db.js';
import { logger } from './logger.js';

const ONE_SECOND = 1000;

export interface ProbeLoopHandle {
  /** 停止测活循环。 */
  stop: () => void;
  /** 读取当前正在测活的代理数，供 /stats 展示。 */
  getChecking: () => number;
}

/** 启动测活调度循环。 */
export function startProbeLoop(db: Database, cfg: AppConfig): ProbeLoopHandle {
  let stopped = false;
  let checking = 0;
  let dispatched = 0;
  let completed = 0;
  let succeeded = 0;
  let failed = 0;
  let timedOut = 0;
  let totalElapsedMs = 0;
  let maxElapsedMs = 0;
  let peakChecking = 0;
  let loopErrors = 0;
  const protocols = new Map<string, number>();
  const summaryTimer = setInterval(() => {
    if (!cfg.debug) return;
    const averageMs = completed > 0 ? Math.round(totalElapsedMs / completed) : 0;
    const protocolText = [...protocols.entries()].map(([name, count]) => `${name}=${count}`).join(',') || '无';
    logger.debug(`[测活周期汇总] 派发=${dispatched} 完成=${completed} 可用=${succeeded} 失效=${failed} 硬超时=${timedOut} 当前并发=${checking} 峰值并发=${peakChecking} 平均耗时=${averageMs}ms 最大耗时=${maxElapsedMs}ms 协议成功={${protocolText}} 调度异常=${loopErrors}`);
    dispatched = completed = succeeded = failed = timedOut = totalElapsedMs = maxElapsedMs = peakChecking = loopErrors = 0;
    protocols.clear();
  }, cfg.debugSummaryInterval * 1000);
  summaryTimer.unref();

  /** 等待至有空闲槽位（checking < probeConcurrency）。 */
  const waitForSlot = async (): Promise<void> => {
    while (checking >= cfg.probeConcurrency) {
      await new Promise((r) => setTimeout(r, 50));
      if (stopped) return;
    }
  };

  const loop = async (): Promise<void> => {
    while (!stopped) {
      try {
        // 并发已满：等待空位再继续
        if (checking >= cfg.probeConcurrency) {
          await waitForSlot();
          continue;
        }

        const now = Date.now();
        const limit = cfg.probeConcurrency - checking;
        const candidates = await db.getProxiesToCheck(now, limit);

        if (candidates.length === 0) {
          await new Promise((r) => setTimeout(r, ONE_SECOND));
          continue;
        }

        const candidateKeys = candidates.map(({ addrKey }) => addrKey);
        const scheduled = await db.getProxyAddresses(candidateKeys);
        if (scheduled.length !== candidates.length) {
          const scheduledKeys = new Set(scheduled.map(({ addrKey }) => addrKey));
          const rejected = candidates.filter(({ addrKey }) => !scheduledKeys.has(addrKey));
          await db.requeueProbeCandidates(rejected, Date.now() + 5000);
          logger.warn(
            `[调度] ${rejected.length} 个候选的地址信息无效，已延迟 5 秒重新入队，避免永久漏检`,
          );
        }
        for (const { addrKey, proxy } of scheduled) {
          const { ip, port, username, password } = proxy;

          checking++;
          dispatched++;
          peakChecking = Math.max(peakChecking, checking);
          // 单个代理最坏耗时兜底：
          // 3 个协议并发。HTTP CONNECT 严格测活最多包含：直连出口主备渠道各一次，
          // 再加两个独立代理渠道连续两轮，共 6 次顺序请求。每次请求都有
          // timeout+1 秒内部硬超时，因此任务级上限必须覆盖这条最长调用链。
          // 最后额外预留 10 秒给 Redis 读写、事件回调和 Agent 清理。
          // 超出此值说明底层任务出现异常卡顿，主动取消完整测活调用链。
          // 并发槽位在请求与 Agent 清理完成、任务真正退出后释放，避免后台残留请求突破并发上限。
          const maxProbeMs = (cfg.timeout * 1000 + 1000) * 6 + 10000;
          const controller = new AbortController();
          const timeoutId = setTimeout(() => {
            controller.abort(new Error('hard-timeout'));
          }, maxProbeMs);
          const task = probeProxy({
            addrKey,
            ip,
            port,
            username,
            password,
            db,
            cfg,
            signal: controller.signal,
          });
          void task
            .then((result) => {
              completed++;
              if (result.ok) succeeded++; else failed++;
              totalElapsedMs += result.elapsedMs;
              maxElapsedMs = Math.max(maxElapsedMs, result.elapsedMs);
              for (const protocol of result.protocols) protocols.set(protocol, (protocols.get(protocol) ?? 0) + 1);
            })
            .catch((e) => {
              completed++;
              failed++;
              if (String(e).includes('hard-timeout')) timedOut++;
              logger.debug(`[测活异常样本] ${addrKey} 任务异常: ${String(e)}`);
            })
            .finally(() => {
              clearTimeout(timeoutId);
              checking--;
            });
        }
      } catch (e) {
        loopErrors++;
        // 关键：单次迭代失败（如 Redis 还在加载数据、瞬时连接异常）绝不让循环退出。
        // 记录后等待数秒重试，保证循环具备自愈能力。
        // 这解决了"容器启动时 Redis 还在 LOADING 导致测活循环永久终止"的问题。
        logger.warn(`[调度] 循环迭代异常（${String(e)}），5 秒后重试`);
        await new Promise((r) => setTimeout(r, 5000));
      }
    }
  };

  // 循环本身的兜底：理论上 while(!stopped) 不会正常退出（除非收到停止信号），
  // 但若因未知原因退出，这里记录并重启，保证测活永不停止。
  const runForever = (): void => {
    loop().catch((e) => {
      logger.error(`测活循环意外退出（将重启）: ${String(e)}`);
      setTimeout(runForever, 5000);
    });
  };
  runForever();

  return {
    stop: () => {
      stopped = true;
      clearInterval(summaryTimer);
    },
    getChecking: () => checking,
  };
}
