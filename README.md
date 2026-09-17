# 账号管理系统

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
|./.env                   | 密钥、密码等等重要敏感信息|

## 系统

### 代理池

请查看 [./services/proxies/AGENTS.md](./services/proxies/AGENTS.md) 和 [./services/proxies/README.md](./services/proxies/README.md) 文件