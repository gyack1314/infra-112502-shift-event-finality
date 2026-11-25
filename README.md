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
