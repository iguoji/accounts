/**
 * 配置加载。
 * 读取仓库根目录下的 .env（即 services/proxies 的 ../../.env），
 * 以系统环境变量 > .env 文件 > 代码默认值的优先级取值。
 * 日志目录、数据源 YAML 路径固定推导，不放进 .env。
 */
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const __dirname = dirname(fileURLToPath(import.meta.url));

/** 服务根目录：由 src/dist 向上两级得到的 services/proxies 目录。 */
export const SERVICE_ROOT = join(__dirname, '..');
/** 仓库根目录（services 的两级父目录），用于定位 logs/.env。 */
const REPO_ROOT = join(SERVICE_ROOT, '..', '..');

export const LOG_DIR = join(REPO_ROOT, 'logs', 'proxies');
export const SOURCE_FILE = join(SERVICE_ROOT, 'source.yaml');
export const ENV_FILE = join(REPO_ROOT, '.env');

/** 从 .env 读出键值对（KEY = VALUE，忽略注释与空行，支持行内注释）。 */
function readEnvFile(): Record<string, string> {
  const out: Record<string, string> = {};
  try {
    const raw = readFileSync(ENV_FILE, 'utf8');
    for (const line of raw.split(/\r?\n/)) {
      const t = line.trim();
      if (!t || t.startsWith('#') || t.startsWith(';') || t.startsWith('--')) continue;
      const idx = t.indexOf('=');
      if (idx === -1) continue;
      const key = t.slice(0, idx).trim();
      // 剥离行内注释：VALUE 后面的 "  # ..." 部分
      let val = t.slice(idx + 1).trim();
      const hashIdx = val.indexOf('#');
      if (hashIdx !== -1) val = val.slice(0, hashIdx).trim();
      if (key) out[key] = val.replace(/^["']|["']$/g, '');
    }
  } catch {
    // .env 不存在或不可读时忽略，回退到环境变量与默认值。
  }
  return out;
}

/** 取值：进程环境变量 > .env 文件 > 默认值；解析为整数。 */
function intOr(target: Record<string, string>, key: string, def: number): number {
  const src = process.env[key] ?? target[key];
  if (src === undefined || src === '') return def;
  const n = Number(src);
  return Number.isFinite(n) && Number.isInteger(n) ? n : def;
}

function strOr(target: Record<string, string>, key: string, def: string): string {
  return process.env[key] ?? target[key] ?? def;
}

/** 取布尔值：进程环境变量 > .env 文件 > 默认值。识别 true/1/yes/on（不区分大小写）。 */
function boolOr(target: Record<string, string>, key: string, def: boolean): boolean {
  const src = process.env[key] ?? target[key];
  if (src === undefined || src === '') return def;
  return /^(true|1|yes|on)$/i.test(src.trim());
}

export interface AppConfig {
  /** 采集间隔，秒 */
  fetchInterval: number;
  /** 数据源下载超时，秒 */
  fetchTimeout: number;
  /** 同时下载的数据源数量 */
  fetchConcurrency: number;
  /** 测活间隔，秒 */
  interval: number;
  /** 指数退避递增倍数基数 */
  intervalBase: number;
  /** 单次测活请求超时，秒 */
  timeout: number;
  /** 单个协议测活失败重试次数 */
  retry: number;
  /** 连续失败达该次数软删 */
  maxConsecutiveFail: number;
  /** 同时探测的最大代理 IP 数 */
  probeConcurrency: number;
  host: string;
  port: number;
  /** 测活主渠道与备用渠道 */
  primaryChannel: string;
  backupChannel: string;
  /** Redis 连接 */
  redisHost: string;
  redisPort: number;
  /** 调试模式开关：开启后输出测活/采集/调度的详细日志到 debug.log */
  debug: boolean;
}

export function loadConfig(): AppConfig {
  const env = readEnvFile();
  return {
    port: intOr(env, 'PORT', 3000),
    host: strOr(env, 'HOST', '0.0.0.0'),
    fetchInterval: intOr(env, 'FETCH_INTERVAL', 300),
    fetchTimeout: intOr(env, 'FETCH_TIMEOUT', 30),
    fetchConcurrency: Math.max(intOr(env, 'FETCH_CONCURRENCY', 4), 1),
    interval: intOr(env, 'INTERVAL', 300),
    intervalBase: intOr(env, 'INTERVAL_BASE', 2),
    timeout: intOr(env, 'TIMEOUT', 5),
    retry: Math.max(intOr(env, 'RETRY', 3), 1),
    maxConsecutiveFail: intOr(env, 'MAX_CONSECUTIVE_FAIL', 3),
    probeConcurrency: intOr(env, 'PROBE_CONCURRENCY', 50),
    primaryChannel: 'https://checkip.amazonaws.com',
    backupChannel: 'https://1.0.0.1/cdn-cgi/trace',
    redisHost: strOr(env, 'REDIS_HOST', 'redis'),
    redisPort: intOr(env, 'REDIS_PORT', 6379),
    debug: boolOr(env, 'DEBUG', false),
  };
}
