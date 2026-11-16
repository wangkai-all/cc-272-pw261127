# Pointwise Shared-Scan Server

多个查询并发读取同一个大文件时，**只发生一次物理磁盘扫描**，而每个查询仍拥有
独立的过滤、超时、背压与读取版本。系统让真实的物理字节读取可观测——不是缓存
最终答案再分发，而是把同一次扫描产出的记录扇出（fan-out）给各自独立的逻辑流。

纯 Python 标准库实现（`http.server`、`threading`、`hashlib`、`bisect` …），
无第三方依赖。

---

## 1. 它解决什么问题

N 个人对同一个大文件发起查询。朴素做法是每人各扫一遍，磁盘读放大 N 倍。本系统：

- 同一个**不可变内容版本**的在途扫描只跑一次，后来的查询挂到同一次扫描上；
- 每个订阅者用自己的过滤器、自己的有界缓冲、自己的超时/截止时间；
- 某个订阅者取消、超时、或消费太慢，**只终止它自己**，不影响其他人，内存有上界；
- 文件被替换后，新查询绝不会挂到旧版本的在途扫描上；
- 进程崩溃恢复时，绝不会把两个不同版本的分片拼接在一起。

---

## 2. 四大组件

| 组件 | 代码 | 职责 |
| --- | --- | --- |
| **执行器** Executor | `server/engine.py` | `ScanSession` 单 worker 分片扫描一个不可变 blob，记录扇出给所有订阅者；权威行号表；catch-up 重读 |
| **分发缓冲** Dispatch buffers | `engine.py: BoundedBuffer` / `Subscriber` | 每订阅者一个**条目数 + 字节数双上限**缓冲；连接门控；offer 超时只杀自己 |
| **版本记录** Version records | `server/versions.py` + `server/journal.py` | 内容寻址（sha256）不可变 blob、版本指针链；分片完成的持久化 journal 与恢复校验 |
| **真实网页** Web page | `web/index.html` `web/app.js` `web/styles.css` | 并发查询、拖慢某个消费者、取消/恢复、与独立查询对比、物理读取量与分片进度可视化 |

`server/manager.py` 是编排层：会话注册表、共享/分裂决策、取消恢复、看门狗。
`server/main.py` 是 HTTP + SSE 入口。`server/filters.py` 是过滤器
（all / contains / regex / json 字段比较）。

---

## 3. 关键机制

### 3.1 一次物理扫描，N 条独立逻辑流
`ScanSession` 对一个不可变 blob 只有一个 worker 线程，按确定性分片计划
`plan_shards(size, shard_size)` 逐片 `os.pread`。每读出一行，在会话锁内快照订阅者
集合，再依次调用各订阅者的 `offer_record`——每个订阅者独立过滤、独立入自己的缓冲。
worker 自身读取的字节计入 `worker_bytes_read`，因此 UI 能直接证明「两个订阅者，
worker 只读了约一个文件」。

### 3.2 共享还是分裂
决策只看 `(文件名, 内容哈希, 分片计划)`，集中在 `_find_or_create_session`：

- `split-new`：没有在途会话，新建扫描；
- `shared-head`：在途会话还停在第 0 片，完全共享，零额外读；
- `shared-late`：在途会话已消费若干片，未来的片走 live，错过的片由该订阅者
  **私有 catch-up 重读**（字节单独计量，不会污染共享 worker 的计数）；
- `resume` / `split-completed`：取消后恢复；扫描已结束则绝不复用结果缓存，重新分裂。

### 3.3 原子挂载与恰好一次
后来的订阅者在**同一次会话锁内**完成「快照 worker 已认领的分片 → 设置
catch-up/live-skip 预留 → 发布订阅者」（`attach_with_claimed_shards`）。
`index < next_shard` 的片归 catch-up，其余走 live，边界单一且无时间窗口：
一片要么由 live 发、要么由 catch-up 发，既不重复也不遗漏。

**worker 必须在首个订阅者挂载之后才启动。** 否则首片会扇出给空集合而静默丢失
第 1 行（曾是一个真实竞态，见 `test_worker_does_not_start_before_first_subscriber`）。

### 3.4 权威行号，跨分片不错位
跨分片边界的半行由**后一个分片**拥有。会话构建时用与 worker 完全相同的 carry
算法重放整个 blob，得到权威的 `_shard_lines`（分片 → 该行号列表），并用全局
`_line_offsets` + `bisect` 按字节偏移定位行号。worker 与 catch-up 因此对同一片
发出**完全相同的行号集合**，与分片如何切割无关。

### 3.5 故障隔离与有界内存
- `BoundedBuffer` 同时限制条目数与字节数，单条超大记录和海量小记录都无法突破预算；
- `offer` 在 `offer_timeout` 内入不了队 → 只把该订阅者置为 `overflow` 并关闭其缓冲，
  共享扫描与其他订阅者照常；
- 连接门控：SSE 消费者连上之前记录不入缓冲，独立的 `connect_timeout` 区分
  「没人连」与「连上了但消费慢」；
- 看门狗按每订阅者的 `deadline` 单独判超时；无活跃订阅者时会话暂停并保留进度。

### 3.6 版本隔离与崩溃恢复
- 文件替换 = 内容寻址存储里的**指针翻转**，新旧版本是两个不同 blob，物理上不可能
  被同一次扫描触及；显式指定旧 `version_id` 仍可钉在历史版本；
- 每个完成的分片写 durable journal。重启后 `recover_journals` 校验 blob 存在、
  文件大小与分片计划一致，任何越界/损坏/孤儿 journal 进隔离区，恢复出的会话保持
  `paused`，等新订阅者挂载才续扫；
- 已持久化的分片 worker 不重读（`worker_bytes_read` 严格小于一个整文件），
  catch-up 字节另计——两个版本的分片因此在结构上无法拼接。

---

## 4. 运行

无需安装依赖，标准库即可。

```bash
# 启动服务（环境变量可选）
SCAN_PORT=8080 SCAN_DATA_DIR=./state SCAN_SHARD_PACE=0.15 \
    python3 -I server/main.py
# 打开 http://127.0.0.1:8080/
```

- `SCAN_PORT`：监听端口（默认 8080）
- `SCAN_DATA_DIR`：blob / registry / journals / quarantine 的存放目录
- `SCAN_SHARD_PACE`：分片之间的演示性停顿秒数，便于在网页上观察共享与背压；
  生产设 0

建议用 `-I`（隔离模式）运行，避免当前目录被放进 `sys.path`。

### 网页上能做什么
1. 生成或上传文件，查看版本链（可钉历史版本）；
2. 设定过滤类型、缓冲上限、offer 超时，发单个或并发查询；
3. 在订阅者卡片上 **slow / cancel / resume / compare**：拖慢一个消费者观察它
   因背压被单独终止、取消后恢复恰好一次、与独立查询逐行对比；
4. 右栏实时显示共享/catch-up/独立三类**物理读取字节**、会话分片进度与事件流；
5. 调试按钮模拟硬崩溃（`/api/debug/crash`）与 journal 投毒
   （`/api/debug/corrupt`），重启后观察恢复与隔离。

---

## 5. HTTP / SSE 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/` `/static/*` | 网页与静态资源 |
| GET | `/api/files` | 文件与版本列表 |
| POST | `/api/files` `/api/files/generate` | 上传 / 确定性生成文件 |
| POST | `/api/query` | 发起查询，返回 `session_id`、`subscriber_id`、`shared_mode` |
| GET | `/api/subscribe/<id>/stream` | SSE：`ready` / `record` / `heartbeat` / `end` |
| POST | `/api/subscribe/<id>/cancel` `/resume` `/slow` | 取消 / 恢复 / 标记慢消费者 |
| GET | `/api/subscriber/<id>` | 单个订阅者快照（缓冲占用、计数、状态） |
| POST | `/api/independent` | 独立全量扫描，作为结果正确性的 ground truth |
| GET | `/api/state` `/api/events` | 总量、会话、订阅者、事件 |
| POST | `/api/debug/crash` `/api/debug/corrupt` | 崩溃 / journal 投毒演示 |

---

## 6. 测试与端到端工具

```bash
python3 -I tests/run_tests.py      # 单元 + 集成，共 30 个测试
python3 -I tools/smoke.py          # 真实 HTTP+SSE 端到端（需服务在 8090）
python3 -I tools/crash_recovery.py # 硬杀进程 -> 重启恢复 -> 恰好一次校验（自管 8095）
```

- `tests/test_units.py`：有界缓冲、过滤器、分片计划、内容寻址版本、journal 恢复/隔离；
- `tests/test_manager.py`：并发共享一次扫描、结果与独立查询一致、慢消费者背压隔离、
  取消恢复恰好一次、新版本不挂旧扫描、历史版本钉选、worker 启动顺序回归。

`smoke.py` 默认连 `127.0.0.1:8090`；`crash_recovery.py` 会自行拉起/清理自己的
服务与数据目录（端口 8095）。运行时目录 `state/`、`state-crash/` 已在 `.gitignore`
中忽略。
