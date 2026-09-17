# 代理池

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
    let addrs = new Set()
    foreach (const url of urls) {
        本地路径 = 下载文件(url, 读取配置(FETCH_TIMEOUT))
        流式对象 = 文件句柄(本地路径)
        while (流式对象.存在下一行()) {
            当前行 = 流式对象.读取当前行()
            // 源行形如 protocol://ip:port:country ，protocol 或 country 极可能缺失，
            // 但我们只用 ip 和 port，缺失不影响解析
            [ip, port] = 格式处理(当前行)
            // 地址合法性纯规则判断，满足才算可用公网地址：
            //   - 跳过注释行、含中文等非 ASCII 字符的行
            //   - 端口必须在 1~65535，且原始行必须是可解析的 ip + port 形式
            //   - ip 必须是公网单播地址：内网/私有/回环/链路本地/保留/组播/广播一律拒绝
            if (可用公网地址(ip, port)) {
                addrs.add(ip + ':' + port)
            }
            流式对象.移到下一行()
        }
        流式对象.关闭释放()
    }

    // 按 (ip, port) upsert：
    //   - 已存在且未软删  → 跳过，不刷新
    //   - 已存在但已软删  → 恢复该记录为初始状态
    //   - 不存在          → 插入新记录
    // ON CONFLICT(ip, port) DO UPDATE SET
    //  checked_at = NULL,
    //  deleted_at = NULL,
    //  status = 0,
    //  consecutive_fail = 0,
    //  next_check_at = CURRENT_TIMESTAMP
    // WHERE deleted_at IS NOT NULL
    // 全体处于一个事务中，失败则全体回滚，等待下一次采集
    // 协议表的数据不管
    数据库().事务().分批upsert(addrs, 1000).提交()
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

function 测试一个协议(代理, 协议): bool {
    // 探测结果非bool值，实为对象，按需赋值和取值
    探测结果 = 发送请求(代理.ip, 代理.port, 协议, {
        超时: 读取配置(TIMEOUT),
        重试: 读取配置(RETRY),
        // 成功标准为 HTTP 200 且响应体含非空 IP 文本
        渠道主: 'https://checkip.amazonaws.com',
        // 主渠道404的换备用渠道
        渠道备: 'https://1.0.0.1/cdn-cgi/trace'
    })
    数据库().维护协议表(代理.ip, 代理.port, 协议, 探测结果)
    return 探测结果
}

function 测试一个代理(代理){
    let 结果 = {}
    for(let 协议 of 协议列表) {
        结果[协议] = 并发实现(测试一个协议(代理, 协议))
    }

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

    数据库().维护代理表(代理.ip, 代理.port, 状态, 当前时间, 下次检测时间, 连续失败次数, 软删除)
    return 可用协议(结果)
}

并发队列 = new 并发队列(读取配置(PROBE_CONCURRENCY))
while(true) {
    if 并发队列.满载状态() {
        并发队列.等待空位()
        continue
    }

    代理列表 = 数据库().读取代理(
        deleted_at: null,
        next_check_at: 小于当前时间,
        排除集合: 并发队列.代理集合()
        sort: next_check_at asc, limit: 并发队列.空闲数量()
    )
    if 为空(代理列表) {
        并发队列.等待一秒()
        continue
    }

    for(代理 of 代理列表) {
        并发队列.入栈(代理, function(代理){
            测试一个代理(代理).完成((结果)=>{
                并发队列.出栈(代理)
            })
        })
    }
}
```

### 接口

- 获取所有可用代理列表，基于协议表
    - 地址: /proxies
    - 参数:
        - protocols: 可选，可空，可空数组，默认为所有，传值时必为['http', 'socks4']这样
        - page: 可选，默认1，大于0的整数，表示第几页
        - count: 可选，默认20，大于0的整数，表示一页几条记录
    - 返回: JSON数组 `[{ip: '127.0.0.1',port:8080,protocols:['http','https']}]`，`protocols`为当前测试有效的协议

- 获取一个可用代理，基于协议表
    - 地址: /proxy
    - 参数:
        - protocols: 可选，可空，可空数组，默认为所有，传值时必为['http', 'socks4']这样
    - 返回: JSON对象或空对象  `{ip: '127.0.0.1',port:8080,protocols:['http','socks4']}`，`protocols`为当前测试有效的协议



## 结构

|目录|用途|
|---|---|
|./src                  | 源代码|
|./tests                | 测试用例|
|../../data/proxies     | 数据挂载目录|
|../../logs/proxies     | 日志挂载目录|
|./source.yaml          | 代理资源|
|./AGENTS.md            | 本项目规范|
|./Dockerfile           | Docker镜像文件|

## 技术栈

- NodeJS v24.21.0
- TypeScript v6.0.3



## 配置

存放于 [../../.env](../../.env) 文件中

```ini
-- 默认参数（可被环境变量/env覆盖） --
FETCH_INTERVAL          = 300   // 单位秒，采集间隔
FETCH_TIMEOUT           = 30    // 单位秒，超时时间

INTERVAL                = 300   // 单位秒，测活间隔
INTERVAL_BASE           = 2     // 递增倍数基数（指数退避）
TIMEOUT                 = 5     // 单位秒，测活超时时间
RETRY                   = 3     // 测活失败最多重试 3 次
MAX_CONSECUTIVE_FAIL    = 3     // 测活连续失败达 3 次 → 软删
PROBE_CONCURRENCY       = 50    // 同时最多探测多少个 IP
```

## 数据库

数据库文件名：`proxies.db`

### 代理

|字段|描述|
|---|---|
|ip                     | ip地址|
|port                   | 端口|
|status                 | 0失效，1可用|
|checked_at             | 最后检测时间|
|next_check_at          | 下次具备检测资格的时间|
|consecutive_fail       | 连续失败次数|
|created_at             | `YYYY-MM-DD HH:MM:SS`|
|updated_at             | `YYYY-MM-DD HH:MM:SS`|
|deleted_at             | `YYYY-MM-DD HH:MM:SS`|

索引：

- UNIQUE(ip, port)

### 协议

|字段|描述|
|---|---|
|ip                     | ip地址|
|port                   | 端口|
|type                   | 1HTTP，2HTTPS，3SOCKS4，4SOCKS5|
|status                 | 0失效，1可用|
|latency_ms             | 延迟，单位毫秒|
|created_at             | `YYYY-MM-DD HH:MM:SS`|
|updated_at             | `YYYY-MM-DD HH:MM:SS`|
|deleted_at             | `YYYY-MM-DD HH:MM:SS`|

索引：

- UNIQUE(ip, port, type)
- INDEX(status)
- INDEX(type, status)


## 日志

日志保存的是系统相关的信息，并非测活结果

按yyyy-mm-dd.log存放，超过一个月的的直接删除

