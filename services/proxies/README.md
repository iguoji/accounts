# 代理池

当前版本：v0.1.2。

本版本在现有 Redis 架构上优化 `/stats`、`/proxies` 和 `/proxy` 的高频实时查询，将一次请求需要的读取和结果组装集中到一次 Redis 脚本调用。接口不使用结果缓存，每次请求仍读取 Redis 当前数据；`/proxy` 继续从符合条件的唯一代理集合中均匀随机选择。

通过定期采集 `source.yaml` 中的代理IP，测试出其所支持的各类协议以及可用性，维护出自己的一份可用代理池。

大致逻辑如下：

- 定期批量采集数据，去重后存入数据库
- 周期性异步分块批量测活库中IP，要求检测HTTP\HTTPS\SOCKS4\SOCKS5四协议
- 根据测活结果维护数据库中IP状态
- 提供对内网的HTTP API接口

## 开发

### 协议

- HTTP
- HTTPS
- SOCKS4
- SOCKS5

### 采集

> 伪代码

```js
function 采集(){
    const urls = 读取文件('source.yaml', 'urls')
    // 只用来对源列表内跨文件去重
    let addrs = new Map()
    // 默认最多同时处理 4 个数据源；每个任务只写自己的 Map，避免共享可变状态。
    有限并发执行(urls, 读取配置(FETCH_CONCURRENCY), (url) => {
        let 当前源地址 = new Map()
        流式对象 = 下载并打开文件(url, 读取配置(FETCH_TIMEOUT))
        while (流式对象.存在下一行()) {
            当前行 = 流式对象.读取当前行()
            // 源行形如 protocol://username:password@ip:port:country
            // 其中 protocol、username:password、country 均可能缺失
            // 按源行格式解析提取出 ip、port、可选的 username 与 password、以及其余字段
            代理 = 格式处理(当前行)
            // 地址合法性纯规则判断：
            //   - 跳过注释行、含中文等非 ASCII 字符的行
            //   - 端口必须在 1~65535，且原始行必须是可解析的 ip + port 形式
            //   - ip 必须是公网单播地址：内网/私有/回环/链路本地/保留/组播/广播一律拒绝
            if (代理 && 可用公网地址(代理.ip, 代理.port)) {
                // 同一 ip:port 但不同认证视为不同代理，去重 key 包含认证信息
                key = 生成代理标识(代理) // IPv6 使用方括号，认证字段进行百分号编码
                当前源地址.set(key, 代理)
            }
            流式对象.移到下一行()
        }
        流式对象.关闭释放()
        return 当前源地址
    })
    // 全部下载完成后按源列表顺序统一合并，保持原有跨来源去重规则。
    按源列表顺序合并到(addrs)

    // 将地址按固定大小分批，通过 SMISMEMBER 一次查询整批地址在
    // known_proxies 和 dead_pool 中的状态，避免逐个地址往返 Redis。
    // 根据批量查询结果统一通过 pipeline 入库：
    //   - 已存在（在 known_proxies 集合中）且未软删  → 跳过，不刷新
    //   - 已存在但已软删（在 dead_pool 中）  → 复活：移出 dead_pool，重置状态，重新入队
    //   - 不存在  → 新增：写入 Hash、加入 known_proxies、加入 check_queue（score=0 立即可测）
    数据库().upsert(addrs)
}
setInterval(function(){
    采集()
}, 读取配置(FETCH_INTERVAL))
采集()
```

### 测活

> 伪代码

```js
const 协议列表 = [/*四协议常量：HTTP, HTTPS, SOCKS4, SOCKS5*/]

function 测试一个协议(代理, 协议): 探测结果 {
    // 构建代理连接，如果代理带认证则附带 username:password
    探测结果 = 发送请求(代理, 协议, {
        超时: 读取配置(TIMEOUT),
        重试: 读取配置(RETRY),
        // 主备渠道都是全球知名网站，理论上永远可用，结果反映的是代理状态而非渠道状态：
        // 返回 200 且含 IP → 代理可用；返回 404 → 渠道自身问题，换备用渠道再试；
        // 超时/连接错误/其他非 200 → 代理不通，换备用渠道也连不上，直接判定失效
        渠道主: 'https://checkip.amazonaws.com',
        // 仅当主渠道明确返回 404 时才回退到备用渠道
        渠道备: 'https://1.0.0.1/cdn-cgi/trace'
    })
    return 探测结果
}

function 测试一个代理(代理){
    // 四个协议并发探测
    结果 = 并发执行(协议列表.map(协议 => 测试一个协议(代理, 协议)))

    // 任一协议成功则代理可用
    状态 = 无一成功(结果) ? 失效 : 可用
    if (状态 == 失效) {
        连续失败次数 = 代理.连续失败次数 + 1
        下次检测时间 = 当前时间 + Math.max(读取配置(INTERVAL), (读取配置(INTERVAL_BASE) ** 连续失败次数) * 读取配置(INTERVAL))
        软删除 = 连续失败次数 >= 读取配置(MAX_CONSECUTIVE_FAIL)
    } else {
        连续失败次数 = 0
        下次检测时间 = 当前时间 + 读取配置(INTERVAL)
        软删除 = null
    }

    // 将四种协议结果、代理状态、调度时间和测活元数据合并为一次 Redis pipeline 写入
    数据库().提交测活结果(代理.addrKey, 结果, 状态, 当前时间, 下次检测时间, 连续失败次数, 软删除)
    return 可用协议(结果)
}

// 测活调度主循环：基于 Redis Sorted Set
while(true) {
    // 原子取出到期的候选代理（score <= 当前时间），取出的同时从队列移除
    // 取出即移除，测活完成后再放回，天然避免重复测活同一个代理
    代理列表 = 数据库().取出到期代理(读取配置(PROBE_CONCURRENCY) - 正在测活数)
    if 为空(代理列表) {
        等待一秒()
        continue
    }

    for(代理 of 代理列表) {
        并发执行(() => {
            测试一个代理(代理)
        })
    }
}
```

### 接口

- 获取所有可用代理列表，基于可用协议索引
    - 地址: /proxies
    - 参数:
        - protocols: 可选，可空，可空数组，默认为所有，传值时必为['http', 'socks4']这样
        - page: 可选，默认1，大于0的整数，表示第几页
        - count: 可选，默认20，大于0的整数，表示一页几条记录
    - 返回: JSON数组 `[{ip, port, protocols, username?, password?}]`，`protocols`为当前测试有效的协议，带认证信息的代理附带`username`/`password`

- 获取一个可用代理，基于可用协议索引
    - 地址: /proxy
    - 参数:
        - protocols: 可选，可空，可空数组，默认为所有，传值时必为['http', 'socks4']这样
    - 返回: JSON对象或空对象 `{ip, port, protocols, username?, password?}`，带认证信息的代理附带`username`/`password`

- 获取系统运行状态
    - 地址: /stats
    - 参数: 无
    - 返回: JSON对象，字段分为三组：数据库分段（proxy_total = proxy_available + proxy_unchecked + proxy_cooldown）、已软删（proxy_dead）、运行时（proxy_checking）、时间与计数（last_collect_at、last_check_at、collect_count、check_count）



## 结构

|目录|用途|
|---|---|
|./src                  | 源代码|
|./tests                | 测试用例|
|../../logs/proxies     | 日志挂载目录|
|./source.yaml          | 代理资源|
|./AGENTS.md            | 本项目规范|
|./Dockerfile           | Docker镜像文件|

## 技术栈

- NodeJS v24.21.0
- TypeScript v6.0.3
- Redis（调度队列、协议索引、统计计数器等数据结构）



## 配置

存放于 [../../.env](../../.env) 文件中

```ini
# 默认参数（可被环境变量覆盖）
FETCH_INTERVAL          = 300   # 单位秒，采集间隔
FETCH_TIMEOUT           = 30    # 单位秒，超时时间
FETCH_CONCURRENCY       = 4     # 同时下载的数据源数量，最小为 1

INTERVAL                = 300   # 单位秒，测活间隔
INTERVAL_BASE           = 2     # 递增倍数基数（指数退避）
TIMEOUT                 = 5     # 单位秒，测活超时时间
RETRY                   = 3     # 测活失败最多重试 3 次
MAX_CONSECUTIVE_FAIL    = 3     # 测活连续失败达 3 次 → 软删
PROBE_CONCURRENCY       = 50    # 同时最多探测多少个 IP

# Redis 连接（默认指向 docker-compose 内的 redis 服务）
REDIS_HOST              = redis
REDIS_PORT              = 6379

# 调试模式：开启后测活/采集/调度的详细日志输出到 logs/proxies/debug.log
# 每次服务启动清空 debug.log 重新记录；排查问题后改回 false 关闭
DEBUG                   = false
```

## Redis 数据结构

本服务使用 Redis 作为数据存储，各数据结构与用途如下：

### 代理状态 `proxy:{代理标识}`（Hash）

单个代理的状态记录。IPv6 地址在代理标识中使用 `[{ip}]:{port}` 形式；带认证信息时，用户名和密码在标识中进行百分号编码，避免冒号等特殊字符造成歧义。

|字段|描述|
|---|---|
|status                 | 0失效，1可用|
|consecutive_fail       | 连续失败次数|
|checked_at             | 最后检测时间（毫秒时间戳，空串表示从未检测）|
|ip                     | 结构化代理地址，避免从标识反向拆分|
|port                   | 结构化代理端口|
|username               | 可选，代理认证用户名（源行含认证时存在）|
|password               | 可选，代理认证密码|

### 调度队列 `check_queue`（Sorted Set）

所有待测代理的调度队列。

|字段|描述|
|---|---|
|member                 | 代理标识；IPv4 为 `{ip}:{port}`，IPv6 为 `[{ip}]:{port}`，认证字段经过百分号编码|
|score                  | `next_check_at`（毫秒时间戳），到点即被取出测活|

取出时通过 Lua 脚本原子完成"取出 + 移除"，测活完成后重新放入并设置新的 score。
服务启动时会分批扫描代理全集，将异常退出期间已取出但尚未来得及重新入队的
非死亡代理恢复到队列，并清理死亡代理可能残留的队列成员，避免代理永久漏检。

### 可用协议索引 `available:{type}`（Set）

按协议类型分组的可用代理集合，type 取值：1=HTTP，2=HTTPS，3=SOCKS4，4=SOCKS5。

|字段|描述|
|---|---|
|member                 | 与调度队列相同的无歧义代理标识|

测活判定某协议可用时 `SADD`，失效时 `SREM`。`/proxies` 与 `/proxy` 保持多协议并集语义：列表查询在 Redis 内完成并集、字典序排序和分页，随机查询在 Redis 内选择候选，只把最终地址传回服务。返回数据所需的认证信息和四类协议状态采用批量查询，避免随返回条数增加而产生逐条往返。

### 代理全集 `known_proxies`（Set）

所有已采集入库的代理地址，用于采集去重。

### 软删池 `dead_pool`（Set）

连续失败达上限被软删的代理地址。再次采集命中时会被复活（移出 dead_pool，重置状态）。

### 统计计数器 `stats`（Hash）

由采集入库和测活结果写回脚本原子维护的系统级统计。服务启动时会使用 `SSCAN` 分批读取代理，并批量查询状态进行校准，以修复异常退出或历史版本可能留下的计数漂移；`/stats` 请求本身只读取此 Hash 和少量元数据，不扫描代理全集。

|字段|描述|
|---|---|
|total                  | 候选代理总数（未软删）|
|available              | 当前可用代理数|
|unchecked              | 从未检测过的代理数|
|cooldown               | 测过但不当前可用、在等下次复查的代理数|
|dead                   | 已软删的代理数|

满足等式：`total = available + unchecked + cooldown`，dead 单独计数。

### 元数据（String）

|key|描述|
|---|---|
|`meta:last_collect`    | 最近一次采集完成时间，`YYYY-MM-DD HH:MM:SS`|
|`meta:collect_count`  | 累计采集次数|
|`meta:last_check`      | 最近一次测活时间，`YYYY-MM-DD HH:MM:SS`|
|`meta:check_count`    | 累计测活次数|


## 日志

日志保存的是系统相关的信息，并非测活结果

按yyyy-mm-dd.log存放，超过一个月的的直接删除
