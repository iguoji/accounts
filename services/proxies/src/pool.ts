/**
 * 有界并发队列（信号量语义）。
 * 往队列中提交任务，队列内部以固定并发数并行执行；超出并发数的任务排队等待。
 * 调度循环用「active 数量」判断是否满载、用「空闲槽位」决定一次取多少条代理入库；
 * 「busy 集合」同时包含等待中与执行中的任务，用于入库查询排除。
 */
export class BoundedQueue<T> {
  private readonly capacity: number;
  /** 排队等待执行的任务（已占用一个逻辑槽位）。 */
  private pending: Array<{ task: T; work: () => Promise<void> }> = [];
  /** 当前正在执行的任务（真正占用槽位）。 */
  private running = new Set<T>();
  private draining = false;

  constructor(capacity: number) {
    this.capacity = Math.max(capacity, 1);
  }

  /** 是否已无空闲槽位（排队 + 执行中的任务数达到上限）。 */
  isFull(): boolean {
    return this.pending.length + this.running.size >= this.capacity;
  }

  /** 当前空闲槽位。 */
  idle(): number {
    return Math.max(0, this.capacity - (this.pending.length + this.running.size));
  }

  /** 排队与执行中的全部任务集合（用于数据库查询排除）。 */
  busy(): ReadonlySet<T> {
    const s = new Set<T>(this.running);
    for (const p of this.pending) s.add(p.task);
    return s;
  }

  /**
   * 提交任务：有槽位立即执行，否则进入等待队列，待任务结束后被拉起。
   * 返回的 Promise 在该任务真正执行完毕后 resolve。
   */
  enqueue(task: T, work: () => Promise<void>): Promise<void> {
    return new Promise<void>((resolve) => {
      this.pending.push({ task, work: () => work().then(resolve, resolve) });
      void this.pump();
    });
  }

  /** 等待至有空闲槽位。 */
  async waitForSlot(): Promise<void> {
    while (this.pending.length + this.running.size >= this.capacity) {
      await new Promise<void>((r) => setTimeout(r, 50));
    }
  }

  /** 尽力把排队任务塞进空闲槽位。 */
  private async pump(): Promise<void> {
    if (this.draining) return;
    this.draining = true;
    try {
      while (this.pending.length > 0 && this.running.size < this.capacity) {
        const item = this.pending.shift()!;
        if (this.running.has(item.task)) continue; // 防御性去重
        this.running.add(item.task);
        void this.execute(item);
      }
    } finally {
      this.draining = false;
    }
  }

  private async execute(item: { task: T; work: () => Promise<void> }): Promise<void> {
    try {
      await item.work();
    } finally {
      this.running.delete(item.task);
      void this.pump(); // 释放槽位后拉起排队任务
    }
  }
}