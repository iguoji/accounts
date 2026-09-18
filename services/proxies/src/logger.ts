/**
 * 系统日志，按 yyyy-mm-dd.log 记录到 {LOG_DIR}。
 * 仅记录系统运行相关信息，不记录测活结果明细。
 * 自动删除超过一个月的日志文件。
 *
 * 调试模式（PROXIES_DEBUG 开启时）：
 * - 所有调试日志（测活、采集、调度、重试等细节）写入 debug.log
 * - 服务启动时清空 debug.log，重新开始记录
 */
import { appendFileSync, mkdirSync, readdirSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { LOG_DIR } from './config.js';

mkdirSync(LOG_DIR, { recursive: true });

const DAY_MS = 24 * 3600 * 1000;

function dayStamp(d: Date = new Date()): string {
  const p = (n: number) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}`;
}

function nowStamp(): string {
  const d = new Date();
  const p = (n: number) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
}

/** 删除日志目录下超过 30 天的 *.log 文件。 */
export function cleanOldLogs(): void {
  const cutoff = Date.now() - 30 * DAY_MS;
  let files: string[] = [];
  try {
    files = readdirSync(LOG_DIR);
  } catch {
    return;
  }
  for (const f of files) {
    if (!f.endsWith('.log')) continue;
    try {
      const st = statSync(join(LOG_DIR, f));
      if (st.mtimeMs < cutoff) rmSync(join(LOG_DIR, f));
    } catch {
      // 单个日志文件可能被占用或已删除，忽略跳过。
    }
  }
}

/** 调试模式开关，由 setDebugEnabled 在服务启动时设置。 */
let debugEnabled = false;
const DEBUG_FILE = join(LOG_DIR, 'debug.log');

/** 启用调试模式：开启后 debug() 输出会写入 debug.log，并清空旧的 debug.log 内容。 */
export function setDebugEnabled(enabled: boolean): void {
  debugEnabled = enabled;
  if (enabled) {
    try {
      writeFileSync(DEBUG_FILE, '', 'utf8');
    } catch {
      // 清空失败不阻断主流程。
    }
  }
}

/** 当前是否处于调试模式。 */
export function isDebug(): boolean {
  return debugEnabled;
}

function write(level: string, msg: string): void {
  const line = `[${nowStamp()}] [${level}] ${msg}`;
  try {
    appendFileSync(join(LOG_DIR, `${dayStamp()}.log`), line + '\n', 'utf8');
  } catch {
    // 日志写入失败不应中断主流程。
  }
  // 控制台保留一份（便于 docker 日志采集）
  // eslint-disable-next-line no-console
  console.log(line);
}

/** 调试日志：仅在调试模式开启时写入 debug.log，不进主日志、不进控制台（避免刷屏）。 */
function writeDebug(level: string, msg: string): void {
  if (!debugEnabled) return;
  const line = `[${nowStamp()}] [${level}] ${msg}`;
  try {
    appendFileSync(DEBUG_FILE, line + '\n', 'utf8');
  } catch {
    // 调试日志写入失败忽略。
  }
}

export const logger = {
  info: (msg: string) => write('INFO', msg),
  warn: (msg: string) => write('WARN', msg),
  error: (msg: string) => write('ERROR', msg),
  debug: (msg: string) => writeDebug('DEBUG', msg),
};
