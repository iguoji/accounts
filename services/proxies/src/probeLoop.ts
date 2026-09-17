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
          logger.debug(`[调度] 并发已满(${checking}/${cfg.probeConcurrency})，等待空位`);
          await waitForSlot();
          continue;
        }

        const now = Date.now();
        const limit = cfg.probeConcurrency - checking;
        const candidates = await db.getProxiesToCheck(now, limit);

        if (candidates.length === 0) {
          logger.debug(`[调度] 无到期候选（当前并发 ${checking}/${cfg.probeConcurrency}），等待 1 秒`);
          await new Promise((r) => setTimeout(r, ONE_SECOND));
          continue;
        }

        logger.debug(`[调度] 取出 ${candidates.length} 个到期候选，当前并发 ${checking}/${cfg.probeConcurrency}，将派发测活`);

        const scheduled = await db.getProxyAddresses(candidates);
        if (scheduled.length !== candidates.length) {
          const scheduledKeys = new Set(scheduled.map(({ addrKey }) => addrKey));
          const rejected = candidates.filter((addrKey) => !scheduledKeys.has(addrKey));
          await db.requeueProbeCandidates(rejected, Date.now() + 5000);
          logger.warn(
            `[调度] ${rejected.length} 个候选的地址信息无效，已延迟 5 秒重新入队，避免永久漏检`,
          );
        }
        for (const { addrKey, proxy } of scheduled) {
          const { ip, port, username, password } = proxy;

          checking++;
          // 单个代理最坏耗时兜底：
          // 4 个协议并发，每个协议最坏 = 超时（超时即放弃重试和备用渠道）+ 1 秒硬超时缓冲，
          // 再加 Redis 写入等开销，整体约 超时×1×2 + 10 秒缓冲。
          // 超出此值说明底层 promise 因未知原因永久挂起，强制释放槽位，
          // 绝不让单个坏代理永久占用并发槽位导致整个循环停转。
          const maxProbeMs = cfg.timeout * 1000 * 2 + 10000;
          const task = probeProxy({ addrKey, ip, port, username, password, db, cfg });
          const timeoutGuard = new Promise<never>((resolve) =>
            setTimeout(() => resolve(undefined as never), maxProbeMs),
          );
          void Promise.race([task, timeoutGuard])
            .then(() => logger.debug(`[测活] ${addrKey} 完成，槽位释放`))
            .catch((e) => logger.debug(`[测活] ${addrKey} 异常: ${String(e)}，槽位释放`))
            .finally(() => checking--);
        }
      } catch (e) {
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
    },
    getChecking: () => checking,
  };
}
