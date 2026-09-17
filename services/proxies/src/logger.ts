/**
 * 系统日志，按 yyyy-mm-dd.log 记录到 {LOG_DIR}。
 * 仅记录系统运行相关信息，不记录测活结果明细。
 * 自动删除超过一个月的日志文件。
 */
import { appendFileSync, mkdirSync, readdirSync, rmSync, statSync } from 'node:fs';
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

export const logger = {
  info: (msg: string) => write('INFO', msg),
  warn: (msg: string) => write('WARN', msg),
  error: (msg: string) => write('ERROR', msg),
};