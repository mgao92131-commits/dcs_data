# DeltaV Processed History Collector

本仓库包含 DCS Historian 采集器和现有 Receiver。DCS 端面向
Windows 7 32-bit、.NET Framework 3.5、x86 基线，使用
readProcessed、Aggregate.InterpolatedValue 和 10 秒采样网格读取配置的
Historian tag。

## v3.4.1 双窗口 ACK 流水线

DCS 端仍只有一个可靠边界，但允许最多两个尚未被连续 ACK 覆盖的内存
Batch 同时在流水线中：

    Historian producer (single thread)
        -> memory Batch 1 / Batch 2
        -> two sender workers
        -> PostgreSQL COMMIT ACK
        -> ordered CheckpointEnd coordinator
        -> release one slot and read the next window

Historian 永远只有一个读取线程，流水线深度固定为 2。Batch 2 可以先于
Batch 1 ACK，但只要最老 Batch 尚未 ACK，就不会读取 Batch 3，Checkpoint
也不会越过 Batch 1。网络错误、超时和 HTTP 5xx 会按
[Receiver] SendRetrySeconds 固定间隔重发同一个内存 Batch；401/403 和
数据/协议错误立即停止 Producer 与两个 Sender worker。

state.ini 只有：

    [ContinuousSync]
    CheckpointEnd=2026-08-29 07:30:00.0000000

它表示该时间点之前的数据已经得到 PostgreSQL database ACK。Continuous
每轮从 CheckpointEnd - OverlapSeconds 开始，进程重启后重新读取尚未推进
checkpoint 的窗口；Receiver 通过 sample key 和 batch 幂等处理重复历史样本。

## 构建和打包

    scripts\build-dcs.bat
    scripts\package-dcs.bat

打包输出为 artifacts\dcs_data，包含：

    bin\
    config\
    scripts\
    state\
    logs\
    README.txt

正常运行最多只在内存中保留两个 Batch，不创建本地批次文件。

## 命令

    HistorySync.exe run
    HistorySync.exe stop
    HistorySync.exe sync
    HistorySync.exe init --start "2026-07-01 00:00:00" --end "2026-08-01 00:00:00" --slice 1d
    HistorySync.exe backfill --last 1d --slice 6h
    HistorySync.exe validate --tags tags.txt
    HistorySync.exe status

sync、init 和 backfill 都是完整的 Historian -> Receiver -> database
同步。没有只采集不发送的正式模式。

## 配置

部署模板位于 deploy\dcs\config.example.ini。关键项如下：

    [Sync]
    IntervalMinutes=5
    EndDelaySeconds=30
    OverlapSeconds=60
    MaxWindowMinutes=30
    MinWindowSeconds=10

    [Batch]
    TargetRows=25000
    MaxRows=50000
    TargetBytes=10485760
    MaxBytes=20971520

    [Receiver]
    TimeoutSeconds=135
    SendRetrySeconds=30
    AckMode=database

第一次运行且没有 state 文件时，DCS 以当前完成时间前 15 分钟作为初始
checkpoint；之后唯一的同步起点来自 CheckpointEnd。

## 相关文档

- docs\architecture.md：组件职责、ACK 背压和状态不变量
- docs\deployment.md：部署、运行和现场验证
- docs\protocol.md：DCS 与 Receiver 的 HTTP/CSV/ACK 协议
