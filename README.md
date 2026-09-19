# 账号管理系统

当前版本：v0.1.7。代理池已改用 Redis 统一维护代理状态、测活调度、可用协议索引和运行统计，支持依据代理源头的实际变化规律动态安排串行采集，并能按业务域名过滤暂时受限的出口 IP。

## 结构

|路径|用途|
|---|---|
|./apps                   | 管理程序|
|./apps/web               | Web端管理程序|
|./apps/desktop           | 桌面端管理程序|
|./services               | 子系统目录|
|./services/proxies/      | 代理池系统|
|./data                   | 数据文件集中存放处|
|./data/{service}/        | 各个子系统数据库挂载目录|
|./logs                   | 日志文件集中存放处|
|./logs/{service}/        | 各个子系统日志库挂载目录|
|./AGENTS.md              | AGENT开发规范|
|./README.md              | 面向普通人的全系统使用及功能介绍|
|./docker-compose.yaml    | 集中管理各个系统的docker服务|
|./.env                   | 集中管理全系统公共配置及各子系统配置，敏感信息不会提交到 Git|

## 配置

系统配置统一放在仓库根目录的 `.env` 中，由 `docker-compose.yaml` 读取并注入对应容器。

- `TZ`：所有容器统一使用的时区。
- `REDIS_HOST`：全系统共用的 Redis 地址。在 Docker Compose 网络中通常填写服务名 `redis`。
- `REDIS_PORT`：Redis 在 Compose 内部网络中的监听端口，其他子系统通过服务名和该端口连接。
- `REDIS_PASSWORD`：全系统共用的 Redis 访问密码。Redis 不向宿主机发布端口，只允许 Compose 内部子系统通过服务名和同一密码连接。
- 各子系统的私有配置使用各自前缀。例如代理池使用 `PROXIES_`，避免不同子系统出现同名配置。
- 代理池调试模式采用周期汇总和有限大小的轮转日志，便于诊断采集、测活及调度状态，同时避免逐代理日志无限增长。

## 系统

### 代理池

请查看 [./services/proxies/README.md](./services/proxies/README.md) 文件。
