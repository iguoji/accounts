/**
 * 代理池服务入口。
 * 启动时：
 *  1. 初始化数据库表结构
 *  2. 立即执行第一轮采集，并按 FETCH_INTERVAL 定时采集
 *  3. 启动测活调度循环（按 INTERVAL / 指数退避 维护各代理检测节奏）
 *  4. 启动对内 HTTP API（/proxies、/proxy）
 */
import { loadConfig } from './config.js';
import { Database } from './db.js';
import { logger, cleanOldLogs } from './logger.js';
import { loadUrls, runCollection } from './collect.js';
import { startProbeLoop } from './probeLoop.js';
import { createApiServer } from './api.js';

async function collectSchedule(db: Database, fetchIntervalSec: number, fetchTimeoutSec: number): Promise<void> {
  const run = async (): Promise<void> => {
    try {
      const urls = loadUrls();
      logger.info(`开始采集，共 ${urls.length} 个数据源`);
      const n = await runCollection(urls, { fetchTimeout: fetchTimeoutSec }, db);
      logger.info(`采集完成，入库 ${n} 个地址`);
      cleanOldLogs(); // 顺带清理过期日志
    } catch (e) {
      logger.error(`采集失败（等待下一次）: ${String(e)}`);
    }
  };

  await run();
  setInterval(run, fetchIntervalSec * 1000);
}

// 测活时与大量代理打交道，node 网络层对“代理不可用”（对方断连、拒绝、TLS 握手失败、
// 超时等）会直接 throw 成 uncaughtException，这不是代码 bug，而是代理自身的问题，
// 按约定应静默（其结果由测活记录为失效，不入系统日志）。这里按错误特征把这类代理
// 连接错误静默吞掉，只对真正的运行时故障记 ERROR。
function isProxyNetworkError(err: unknown): boolean {
  const code: unknown = (err as any)?.code;
  const msg: string = String((err as any)?.message ?? '');
  const NET_CODES = ['ECONNRESET', 'ECONNREFUSED', 'ETIMEDOUT', 'ENETUNREACH', 'EHOSTUNREACH', 'ENETDOWN', 'EAI_AGAIN', 'ERR_SOCKET_BAD_PORT'];
  if (typeof code === 'string' && NET_CODES.includes(code)) return true;
  return /socket disconnected|secure TLS|handshake|timeout|tls/i.test(msg);
}

async function main(): Promise<void> {
  process.on('uncaughtException', (err) => {
    if (isProxyNetworkError(err)) return; // 代理连通性错误：静默，不打扰日志
    logger.error(`未捕获异常（已忽略）: ${String(err)}`);
  });
  process.on('unhandledRejection', (reason) => {
    logger.error(`未处理的 Promise 拒绝（已忽略）: ${String(reason)}`);
  });

  const cfg = loadConfig();
  const db = new Database();
  db.init();

  // 采集子任务（立即执行一轮，随后定时执行）
  void collectSchedule(db, cfg.fetchInterval, cfg.fetchTimeout);

  // 测活子任务
  startProbeLoop(db, cfg);

  // HTTP API
  createApiServer(db, cfg);

  // 优雅退出
  const shutdown = () => {
    logger.info('收到退出信号，正在关闭...');
    db.close();
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);

  // 保持进程存活；长生命周期由上述异步任务维持
}

main().catch((e) => {
  logger.error(`服务启动失败: ${String(e)}`);
  process.exit(1);
});