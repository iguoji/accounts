/**
 * 代理池服务入口。
 * 启动时：
 *  1. 连接 Redis
 *  2. 同步兜底源头并按历史更新规律串行进行采集
 *  3. 启动测活调度循环（按 PROXIES_INTERVAL / 指数退避维护各代理检测节奏）
 *  4. 启动对内 HTTP API（/proxies、/proxy、/stats）
 */
import { loadConfig } from './config.js';
import { Database } from './db.js';
import { logger, cleanOldLogs, setDebugEnabled } from './logger.js';
import { startCollectionLoop } from './collect.js';
import { startProbeLoop } from './probeLoop.js';
import { createApiServer } from './api.js';

/**
 * 判断一个错误是否属于测活连接的噪音（代理失效的副产物）。
 * 这类错误源于坏代理在 TLS/SOCKS 握手阶段的连接异常，与系统本身无关，
 * 不应该写日志刷屏（见 README：日志保存的是系统相关信息，并非测活结果）。
 */
function isProbeConnectionNoise(err: unknown): boolean {
  const msg = err instanceof Error ? err.message : String(err);
  return (
    msg.includes('TLS connection was established') ||
    msg.includes('socket disconnected before secure TLS') ||
    msg.includes('connect ECONNREFUSED') ||
    msg.includes('connect ETIMEDOUT') ||
    msg.includes('connect ENETUNREACH') ||
    msg.includes('connect EHOSTUNREACH') ||
    msg.includes('socks') ||
    msg.includes('SOCKS') ||
    msg.includes('tunneling socket') ||
    msg.includes('hard-timeout') ||
    msg.includes('getaddrinfo')
  );
}

async function main(): Promise<void> {
  process.on('uncaughtException', (err) => {
    // 测活连接的噪音（坏代理的 TLS 握手异常等）静默，不写日志；
    // 其余才是真正的系统故障，需要记录。
    if (!isProbeConnectionNoise(err)) {
      logger.error(`未捕获异常（已忽略）: ${String(err)}`);
    }
  });
  process.on('unhandledRejection', (reason) => {
    if (!isProbeConnectionNoise(reason)) {
      logger.error(`未处理的 Promise 拒绝（已忽略）: ${String(reason)}`);
    }
  });

  const cfg = loadConfig();
  setDebugEnabled(cfg.debug);
  if (cfg.debug) logger.info('调试模式已开启，详细日志输出到 logs/proxies/debug.log');
  const db = new Database(cfg);
  await db.connect();
  // 关键：等待 Redis 真正就绪（能响应命令）再启动业务。
  // 容器启动时 Redis 可能端口已开放但还在从磁盘加载数据（LOADING），
  // 此时发命令会失败，会导致测活循环启动即终止。
  await db.waitReady();
  logger.info(`Redis 已连接且就绪: ${cfg.redisHost}:${cfg.redisPort}`);
  await db.reconcileStats();
  logger.info('代理统计已完成分批校准');
  const queueRepair = await db.reconcileCheckQueue();
  logger.info(
    `测活队列已完成分批校准，恢复 ${queueRepair.restored} 个漏失代理，清理 ${queueRepair.removedDead} 个死亡代理`,
  );

  cleanOldLogs();
  const collection = await startCollectionLoop(db, cfg);

  // 测活子任务
  const probe = startProbeLoop(db, cfg);

  // HTTP API
  createApiServer(db, cfg, probe.getChecking);

  // 优雅退出
  const shutdown = async () => {
    logger.info('收到退出信号，正在关闭...');
    collection.stop();
    probe.stop();
    await db.close();
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

main().catch((e) => {
  logger.error(`服务启动失败: ${String(e)}`);
  process.exit(1);
});
