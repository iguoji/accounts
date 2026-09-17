# 代理池项目规范

本文件是本服务（services/proxies）的开发与交付规范，继承仓库根目录 `AGENTS.md` 的整体约定，
并针对本服务做必要补充。冲突时，根 `AGENTS.md` 优先；本文件仅说明本服务特有内容。

## 一、作用范围

- 通过定期采集 `source.yaml` 中的代理 IP，测活其支持的 HTTP / HTTPS / SOCKS4 / SOCKS5 四类协议，
  维护 Redis 中的可用代理池，并对外（面向内网）提供 HTTP API。
- 详细设计与接口请阅读本目录下 `README.md`。

## 二、技术栈

- NodeJS v24.21.0（运行时）
- TypeScript v6.0.3（编译）
- 数据库：Redis（Sorted Set 调度队列、Set 协议索引、Hash 代理状态、String 计数器）

## 三、代码结构

| 路径 | 用途 |
|---|---|
| `./src`                    | 源代码 |
| `./src/index.ts`           | 服务入口：连接 Redis、串联采集、测活、API |
| `./src/config.ts`          | 配置加载（.env / 环境变量 / 默认值），含 Redis 连接配置 |
| `./src/db.ts`              | Redis 数据访问层（数据结构定义见 README） |
| `./src/collect.ts`         | 采集：下载数据源、解析、去重入库 |
| `./src/parse.ts`           | 源行解析与公网地址合法性校验 |
| `./src/probe.ts`           | 四协议并发测活、结果写入 Redis |
| `./src/probeLoop.ts`       | 测活调度主循环（基于 Redis Sorted Set） |
| `./src/api.ts`             | HTTP API（/proxies、/proxy、/stats） |
| `./src/logger.ts`          | 系统日志（按日分文件、自动清理） |
| `./src/types.ts`           | 协议常量与公共类型 |
| `./tests`                  | 测试用例目录 |
| `./source.yaml`            | 代理数据源列表 |
| `./Dockerfile`             | Docker 镜像文件 |

## 四、数据与日志

- 数据存储在 Redis（由 docker-compose 的 redis 服务提供，持久化到 `../../data/redis/`）
- 日志存放在 `../../logs/proxies/`，按 `yyyy-mm-dd.log`，超过一个月自动删除

## 五、约定

- 日志只记录系统运行相关信息，不记录测活结果明细。
- 地址合法性为纯规则判断，且只在判定为公网单播地址（内网/私有/回环/链路本地/保留/组播/广播一律拒绝）时才入库；详见 `README.md`。
- Redis 数据结构、key 命名、字段含义，必须与 `README.md` 的 "Redis 数据结构" 章节保持一一对应，改动前先确认 README。
- 每次变更坚持"完成一项独立完整变更后提交一次"，提交信息采用"标题 + 多行正文"，提交内容不夹带与本任务无关的修改。
- 沿用根 `AGENTS.md`：开发不直接改动 `main`，环境搭建需先列方案并由使用者明确同意。
