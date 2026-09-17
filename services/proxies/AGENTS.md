# 代理池项目规范

本文件是本服务（services/proxies）的开发与交付规范，继承仓库根目录 `AGENTS.md` 的整体约定，
并针对本服务做必要补充。冲突时，根 `AGENTS.md`…优先；本文件仅说明本服务特有内容。

## 一、作用范围

- 通过定期采集 `source.yaml` 中的代理 IP，测活其支持的 HTTP / HTTPS / SOCKS4 / SOCKS5 四类协议，
  维护数据库中的可用代理池，并对外（面向内网）提供 HTTP API。
- 详细设计与接口请阅读本目录下 `README.md`。

## 二、技术栈

- NodeJS v24.21.0（运行时）
- TypeScript v6.0.3（编译）
- 数据库：`node:sqlite`（Node 内置 SQLite，无需额外原生编译），数据库文件 `proxies.db`

## 三、代码结构

| 路径 | 用途 |
|---|---|
| `./src`                    | 源代码 |
| `./src/index.ts`           | 服务入口：串联采集、测活、API |
| `./src/config.ts`          | 配置加载（.env / 环境变量 / 默认值） |
| `./src/db.ts`              | SQLite 建表与 CRUD |
| `./src/collect.ts`         | 采集：下载数据源、解析、分批 upsert |
| `./src/parse.ts`           | 源行解析与公网地址合法性校验 |
| `./src/probe.ts`           | 四协议并发测活、协议表/代理表维护 |
| `./src/pool.ts`            | 有界并发队列（限流信号量） |
| `./src/probeLoop.ts`       | 测活调度主循环 |
| `./src/api.ts`             | HTTP API（/proxies、/proxy） |
| `./src/logger.ts`             | 系统日志（按日分文件、自动清理） |
| `./src/types.ts`           | 协议常量与公共类型 |
| `./tests`                  | 测试用例目录 |
| `./source.yaml`            | 代理数据源列表 |
| `./Dockerfile`             | Docker 镜像文件 |

## 四、数据与日志

- 数据（数据库 `proxies.db`）存放在 `../../data/proxies/`
- 日志存放在 `../../logs/proxies/`，按 `yyyy-mm-dd.log`，超过一个月自动删除

## 五、约定

- 日志只记录系统运行相关信息，不记录测活结果明细。
- 地址合法性为纯规则判断，且只在判定为公网单播地址（内网/私有/回环/链路本地/保留/组播/广播一律拒绝）时才入库；详见 `README.md`。
- 数据库表和字段命名、索引、状态取值，必须与 `README.md` 的“数据库”章节保持一一对应，改动前先确认 README。
- 每次变更坚持“完成一项独立完整变更后提交一次”，提交信息采用“标题 + 多行正文”，提交内容不夹带与本任务无关的修改。
- 沿用根 `AGENTS.md`：开发不直接改动 `main`（本次自 user 显式确认可暂时留在 main 为例外），环境搭建需先列方案并由使用者明确同意。

## 六、验证

- 未经使用者明确要求，不执行静态分析、编译、运行或测试；运行验证由使用者自行完成（例如通过 `docker compose up -d --build`）。
- 完成后应如实说明改动内容与哪些验证尚未执行，不得把“修改完成”表述为“已通过验证”。