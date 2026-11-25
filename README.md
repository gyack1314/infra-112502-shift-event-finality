# 矿区事件总线的按班封口与迟到纠正

作业班次由固定时间和若干卸点组成。车端与称台独立编号。本地样例是消息传输及排班控制信号的输入。

## 输入消息

每个 JSONL 文件是一段独立输入记录。每行包括整数秒点 at、消息名称 kind 与对象 record。at 代表中间件收到消息的时刻，同一秒内按行序提交。业务时间和对应回执引用写在 record 中。

- **shift**　发布班次 shift、starts、ends，时间段左闭右开。pads 映射各卸点需要的来源。required_weight 为触发保留卸点的最低已确认重量，kg 使用整数。
- **load**　车端提交 source、seq、rev、trip、observed_at、pad。标有 observed_at 的首次 load 决定在哪个班。修订可更正 pad，不能把记录转移到另一个发生时间。 lane 为发出事实的原始卸点通道，可能不同于经过更正后的 pad。
- **weigh**　称台提交 source、seq、rev、trip、kg。较高 rev 可更正它引用的 trip。重复使用同 rev 但内容变化应报告冲突，等待更高修订。 lane 为称重端原始卸点通道。
- **seal**　指定 shift、pad、source 对该卸点的生产通道声明 last_seq 和 gaps，gaps 为闭区间。序号在班次与来源、原始卸点通道的组合内独立。没有消息的通道以 last_seq 0 表达；更正导致的跨卸点业务重算不转移旧通道序号。
- **close**　值班台在班末请求关闭本班。
- **pull**　client 查该班和卸点待处理的信号。
- **ack**　client 确认持有的通知，notice 按该 client 对该班该卸点接到的通知序号引用。
- **claim**　排班端声明在正式释放信号下已接手卸点。notice 引用已经确认且当下有效的释放信号。
- **inspect**　查询指定班次或卸点。

## 示例

- two-pads-one-late-weigh.jsonl
- change-pad-after-claim.jsonl

## 读取演练资料

运行环境为 Python 3.11，只使用标准库。在根目录运行以下命令可核对文件格式及读取接口。

    python3 scenario_reader.py examples
    python3 -m unittest discover -v

资料读取器会列出各文件包含的消息和时间范围。演练记录只描述各参与方交给系统的输入。

## 事件总线

`event_bus.py` 仅使用标准库，在读取器之外提供可长期驻留的 `EventBus`。它按输入顺序处理同一个 `{at, kind, record}` 信封；`at` 是上报/中间件接收时间，业务归属使用 `load.observed_at` 或运输当前有效开始事实。

```python
from event_bus import EventBus, replay_file

bus = replay_file("examples/change-pad-after-claim.jsonl")

# 同一 client 名重新连接后，未 ack 的通知仍可再次领取。
ops = bus.connect("ops", "E")
batch = ops.pull("B")
ops.ack("B", batch[0]["notice"])
ops.claim("B", batch[0]["notice"])

# 查询某班某卸点缺哪路证据。
bus.inspect("E", "B")["missing_evidence"]

# 查询哪个更正使哪个已接手卸点待复核。
bus.correction_review_index()
```

也可以继续送入实时记录：

```python
bus.handle({"at": 16, "kind": "weigh", "record": {...}})
bus.pull("schedule", "E", "B")
bus.ack("schedule", "E", "B", 2)
bus.claim("schedule", "E", "B", 2)
```

### 通知阶段

- `close` 会为每个卸点生成 `phase="provisional"` 的临时状态；其中 `tentative_decision` 仅供准备，不能 claim。
- 只有 `shift.pads[pad]` 中各路来源都有明确 seal、声明范围内的序号均已收到或明确标为 gap，并且修订/关联冲突均已解决，才会生成 `phase="formal"` 信号。
- 正式信号的 `decision` 为 `release` 或 `retain`；累计已确认重量达到 `required_weight` 时为 `retain`。
- `claim` 要求该 client 已 ack 属于自己的、当下仍有效的正式 `release` 通知。临时通知、`retain`、已被更正/取代的通知都不能 claim。
- 一个卸点缺材料只会使该卸点保持 provisional/pending，不影响其他卸点发出正式信号。

### 序号、gap 与更正

- 来源记录按“班次 + source + 原始 `lane` + `seq`”保留通道位置；业务 `pad` 被更正后，`lane` 和 `seq` 不变。
- 没有 seal 时，即使序号连续也不能推断无记录；只有 seal 明确给出 `last_seq`/`gaps` 且对应位置没有现存消息，位置状态才是 `never_produced`。
- 同一 `rev` 内容不同会产生阻塞性 `revision_conflict`；更高 `rev` 才能解决。更旧的 `rev` 晚到只会留痕，不能覆盖当前版本。
- 运输先到称重、后到 load 时，称重会在 load 到达后沿运输号补归到首次 `observed_at` 对应的班次。
- 关班后的跨卸点更正会重算新旧两个卸点：未接手的卸点收到新的当前决定；已有有效 claim 的卸点生成 `correction_review`，其中包含更正引用和原开始事实。

### 常用查询

- `bus.missing_evidence(shift, pad?)`：每班/卸点/来源缺少的 seal、序号和冲突。
- `bus.inspect(shift, pad?)`：完整状态，包含信号版本、当时依赖的 `source_positions` 快照、原始来源记录、更正、复核、claim 和 client 通知。
- `bus.client_activity(shift?, pad?)`：每个 client 的每条 notice 处理的是 `provisional_pending`、`formal_release` 还是 `formal_retain`，以及 ack/claim 状态。
- `bus.correction_review_index()`：更正、复核、被要求复核的 client/claim 与原开始事实的对应关系。
- `bus.pending_reviews(shift?, pad?)`：仍待处置的已接手现场复核。

命令行可重放并输出 `pull`、`ack`、`claim`、`inspect` 结果及摘要：

```bash
python3 event_bus.py examples/two-pads-one-late-weigh.jsonl
python3 event_bus.py examples --quiet
python3 -m unittest discover -v
```

`shift_bus.py` 和 `shift_event_bus.py` 只是兼容导入名；主实现和推荐导入名仍是 `event_bus.EventBus`。
