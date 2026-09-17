/**
 * SQLite 数据访问层（使用 Node v24+ 内置的 node:sqlite，无需原生编译依赖）。
 * 表结构与索引严格遵循 README：
 *  - proxies: UNIQUE(ip, port)
 *  - protocols: UNIQUE(ip, port, type), INDEX(status), INDEX(type, status)
 * 时间存储约定：
 *  - created_at / updated_at / deleted_at 为 "YYYY-MM-DD HH:MM:SS" 文本
 *  - checked_at（最后检测时间）与 next_check_at（下次具备检测资格的时间）为
 *    毫秒级整数时间戳，便于与 "当前时间" 直接比较。
 */
import { mkdirSync } from 'node:fs';
import { DatabaseSync } from 'node:sqlite';
import { DB_FILE, DATA_DIR } from './config.js';
import { logger } from './logger.js';
import { TYPE_TO_NAME } from './types.js';
import type { ProxyRecord } from './types.js';

/** "YYYY-MM-DD HH:MM:SS"（本地时间）文本格式。 */
export function fmtTime(d: Date = new Date()): string {
  const p = (n: number) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
}

export interface AvailableItem {
  ip: string;
  port: number;
  protocols: string[];
}

export class Database {
  private db: DatabaseSync;

  constructor(path: string = DB_FILE) {
    mkdirSync(DATA_DIR, { recursive: true });
    this.db = new DatabaseSync(path);
  }

  /** 初始化表结构与索引。 */
  init(): void {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS proxies (
        id               INTEGER PRIMARY KEY AUTOINCREMENT,
        ip               TEXT    NOT NULL,
        port             INTEGER NOT NULL,
        status           INTEGER NOT NULL DEFAULT 0,
        checked_at      INTEGER,
        next_check_at    INTEGER NOT NULL DEFAULT 0,
        consecutive_fail INTEGER NOT NULL DEFAULT 0,
        created_at      TEXT    NOT NULL,
        updated_at      TEXT    NOT NULL,
        deleted_at      TEXT
      );
      CREATE UNIQUE INDEX IF NOT EXISTS ux_proxies ON proxies(ip, port);

      CREATE TABLE IF NOT EXISTS protocols (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        ip         TEXT    NOT NULL,
        port       INTEGER NOT NULL,
        type       INTEGER NOT NULL,
        status     INTEGER NOT NULL DEFAULT 0,
        latency_ms INTEGER,
        created_at TEXT    NOT NULL,
        updated_at TEXT    NOT NULL,
        deleted_at TEXT
      );
      CREATE UNIQUE INDEX IF NOT EXISTS ux_protocols ON protocols(ip, port, type);
      CREATE INDEX IF NOT EXISTS ix_protocols_status ON protocols(status);
      CREATE INDEX IF NOT EXISTS ix_protocols_type_status ON protocols(type, status);
    `);
  }

  close(): void {
    this.db.close();
  }

  // -------------------------------------------------------------------------
  // 采集：批量 upsert
  // -------------------------------------------------------------------------

  /**
   * 按 (ip, port) 分批 upsert（每批 1000），整体在一个事务中，语义与 README SQL 一致：
   *  - 不存在           → 插入新记录
   *  - 已存在但未软删   → ON CONFLICT 匹配但 WHERE(deleted_at IS NOT NULL) 为假，跳过不刷新
   *  - 已存在但已软删   → 恢复该记录为初始状态
   * 失败则全体回滚，等待下一次采集。协议表数据不受影响。
   */
  upsertAddresses(addrs: Set<string>): void {
    const now = fmtTime(new Date());

    const upsert = this.db.prepare(`
      INSERT INTO proxies (ip, port, status, next_check_at, consecutive_fail, created_at, updated_at, deleted_at)
      VALUES (?, ?, 0, 0, 0, ?, ?, NULL)
      ON CONFLICT(ip, port) DO UPDATE SET
        checked_at       = NULL,
        next_check_at    = 0,
        status           = 0,
        consecutive_fail = 0,
        updated_at       = excluded.updated_at,
        deleted_at       = NULL
      WHERE proxies.deleted_at IS NOT NULL
    `);

    this.db.exec('BEGIN');
    try {
      const batch: string[] = [];
      const flush = () => {
        for (const key of batch) {
          const idx = key.lastIndexOf(':');
          const ip = key.slice(0, idx);
          const port = Number(key.slice(idx + 1));
          upsert.run(ip, port, now, now);
        }
        batch.length = 0;
      };
      for (const key of addrs) {
        batch.push(key);
        if (batch.length >= 1000) flush();
      }
      if (batch.length > 0) flush();
      this.db.exec('COMMIT');
    } catch (e) {
      this.db.exec('ROLLBACK');
      logger.error(`upsert 事务失败，全体回滚，等待下一次采集: ${String(e)}`);
      throw e;
    }
  }

  // -------------------------------------------------------------------------
  // 测活调度读取
  // -------------------------------------------------------------------------

  /**
   * 读取需要测活的代理：
   *  - 未软删 (deleted_at IS NULL)
   *  - next_check_at <= now
   *  - 排除正在测活（并发队列中）的集合
   *  - 按 next_check_at 升序，最多 limit 条
   */
  getProxiesToCheck(nowMs: number, exclude: Set<string>, limit: number): ProxyRecord[] {
    const rows = this.db
      .prepare(
        `SELECT ip, port, consecutive_fail
           FROM proxies
          WHERE deleted_at IS NULL AND next_check_at <= ?
          ORDER BY next_check_at ASC
          LIMIT ?`,
      )
      .all(nowMs, Math.max(limit, 1)) as Array<{ ip: string; port: number; consecutive_fail: number }>;
    const out: ProxyRecord[] = [];
    for (const r of rows) {
      if (exclude.has(`${r.ip}:${r.port}`)) continue;
      out.push({ ip: r.ip, port: r.port, consecutiveFail: r.consecutive_fail });
    }
    return out;
  }

  // -------------------------------------------------------------------------
  // 测活结果回写
  // -------------------------------------------------------------------------

  /**
   * 维护代理表：记录本次测活的状态、检测时间、连续失败次数与软删标记。
   */
  updateProxy(
    ip: string,
    port: number,
    status: number,
    checkedAtMs: number,
    nextCheckAtMs: number,
    consecutiveFail: number,
    deletedAt: string | null,
  ): void {
    const now = fmtTime(new Date());
    this.db
      .prepare(
        `UPDATE proxies
            SET status = ?, checked_at = ?, next_check_at = ?,
                consecutive_fail = ?, deleted_at = ?, updated_at = ?
          WHERE ip = ? AND port = ?`,
      )
      .run(status, checkedAtMs, nextCheckAtMs, consecutiveFail, deletedAt, now, ip, port);
  }

  /**
   * 维护协议表：单条 (ip, port, type) 的探测结果 upsert。
   * 状态 1 可用 / 0 失效；可用时记录延迟，失效时清空延迟。
   */
  updateProtocol(ip: string, port: number, type: number, ok: boolean, latencyMs: number | null): void {
    const now = fmtTime(new Date());
    this.db
      .prepare(
        `INSERT INTO protocols (ip, port, type, status, latency_ms, created_at, updated_at, deleted_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, NULL)
         ON CONFLICT(ip, port, type) DO UPDATE SET
           status = excluded.status,
           latency_ms = excluded.latency_ms,
           updated_at = excluded.updated_at,
           deleted_at = NULL`,
      )
      .run(ip, port, type, ok ? 1 : 0, latencyMs, now, now);
  }

  // -------------------------------------------------------------------------
  // 对外 API 查询（基于协议表）
  // -------------------------------------------------------------------------

  /**
   * 获取全部可用代理列表（基于协议表），每个 (ip, port) 汇聚其当前测试有效
   * 的协议。types 为协议类型集合（空表示全部）。结果按 updated_at 升序，
   * 保证分页稳定。
   */
  listAvailable(types: number[]): AvailableItem[] {
    const all = types.length === 0;
    const sql = all
      ? `SELECT ip, port, type FROM protocols WHERE deleted_at IS NULL AND status = 1 ORDER BY updated_at`
      : `SELECT ip, port, type FROM protocols
          WHERE deleted_at IS NULL AND status = 1 AND type IN (${types.map(() => '?').join(',')})
          ORDER BY updated_at`;
    const rows = all
      ? (this.db.prepare(sql).all() as Array<{ ip: string; port: number; type: number }>)
      : (this.db.prepare(sql).all(...types) as Array<{ ip: string; port: number; type: number }>);

    const map = new Map<string, AvailableItem>();
    for (const r of rows) {
      const key = `${r.ip}:${r.port}`;
      let item = map.get(key);
      if (!item) {
        item = { ip: r.ip, port: r.port, protocols: [] };
        map.set(key, item);
      }
      item.protocols.push(TYPE_TO_NAME[r.type] ?? 'http');
    }
    return [...map.values()];
  }
}