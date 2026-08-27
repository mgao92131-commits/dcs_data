# dcs_data v2

DeltaV Historian 数据同步系统。DCS 端保持 Windows 7 32 位、.NET Framework
2.0/3.5、x86 兼容；数据库电脑运行 Go Receiver 和 PostgreSQL。

## 数据链路

```text
DeltaV Historian
  -> HistorianClient
  -> RangeSyncEngine / dynamic slicing
  -> in-memory CSV batch
  -> HTTP
  -> PostgreSQL transaction COMMIT
  -> ACK
  -> atomic state.ini checkpoint
```

发送失败时才写入 `spool\pending`。pending 有效批次严格按 `RangeStart` 顺序发送，
旧批次未确认时新批次不能绕过它。永久坏数据进入 `failed`，本地损坏数据进入
`quarantine`；continuous 同步会暂停，直到缺口被人工处理。

## 仓库基线

- `v1-legacy`：重构前稳定版本
- `main`：v1 稳定分支
- `refactor/v2`：v2 开发与部署分支
- `refactor/v2-layout`：v2 工程结构重构分支

生产配置 `config.ini`、`receiver.ini`、`state.ini` 和运行目录被 Git 忽略。
请从 `deploy/dcs`、`deploy/receiver` 中的模板复制，不要提交 API Key 或数据库密码。

源码位于 `src/`，测试位于 `tests/`，构建输出和发布包位于 `artifacts/`。

## Receiver 部署

1. 安装 PostgreSQL，创建数据库和 writer 用户。
2. 执行 `psql -d deltav_history -f database\migrations\001_create_tables.sql`。
3. 复制 `deploy\receiver\receiver.example.ini` 为 `receiver.ini`，设置监听地址、API Key 和
   PostgreSQL 密码，保持 `SynchronousCommit=true`。
4. 在构建机执行 `scripts\package-receiver.bat`，将 `artifacts\receiver` 复制到
   数据库电脑，再启动 `HistoryReceiver.exe --config receiver.ini`。

`committed=true` 且 `commit_level=database` 只在 `imported_batches` 与
`history_samples` 的同一数据库事务提交后返回。PostgreSQL 不可用时 Receiver
返回 HTTP 503。

## DCS Collector 部署

1. 将 `artifacts\dcs` 复制到 DCS 电脑；现场配置由
   `deploy\dcs\config.example.ini` 复制为 `config.ini`，设置 Receiver 地址和同一 API Key。
2. 构建机执行 `scripts\package-dcs.bat`。脚本只接受 .NET Framework 2.0/3.5
   编译器，并生成 x86 EXE 和 `DcsData.Historian.dll`。
3. 在 DCS 电脑执行 `scripts\test-dcs-compatibility.bat` 和
   `HistorySync.exe validate --tags tags.txt`。
4. 用同一 Tag 和时间范围比较 v1/v2 的行数、时间戳、值、类型和 Flags。
   可以直接运行 `tools\compare-history-csv.ps1` 做逐行比较；详细验收证据见
   [docs/dcs-acceptance.md](docs/dcs-acceptance.md)。
5. 先运行 `HistorySync.exe --console` 观察；确认无误后双击
   `start-historysync.vbs` 后台运行，需要停止时双击 `stop-historysync.vbs`。

普通发布包不需要管理员权限，不创建服务、计划任务或开机启动项；程序绝不重启、关机或强制结束 DCS。

## 命令

```bat
HistorySync.exe sync
HistorySync.exe send
HistorySync.exe status
HistorySync.exe init --start "2026-01-01 00:00:00" --end "2026-08-26 00:00:00" --slice 1d
HistorySync.exe backfill --start "2026-08-20 08:00:00" --end "2026-08-20 12:00:00" --slice 30m
```

`init` 和 `backfill` 共用 RangeSyncEngine，但不会修改 continuous checkpoint。

## Checkpoint

`state.ini` 原子保存：

- `LastCollectedEnd`：已成功远程提交，或已可靠保存到 outbox。
- `LastAcceptedEnd`：Receiver 已确认接收的位置。
- `LastCommittedEnd`：PostgreSQL 已提交的连续位置。

生产 v2 使用 `AckMode=database`。从 v1 升级时，必须先部署同步提交 Receiver、
清空旧 inbox 并确认数据库已导入，再切换该配置。

## 测试

数据库电脑执行 `test-baseline-local.bat`，或分别执行
`scripts\test-dcs-local.bat`、`scripts\test-receiver.bat`。真实 PostgreSQL
事务测试使用 `scripts\test-postgres-integration.bat`，需要先设置
`DCS_HISTORY_TEST_DATABASE_URL`。如果本机没有 .NET 2.0/3.5 编译器，脚本会明确
警告并只做新版框架回归；旧框架兼容性必须在 DCS 电脑运行严格门禁。

## 文档

- [架构](docs/architecture.md)
- [部署、升级和回滚](docs/deployment.md)
- [HTTP 协议](docs/protocol.md)
- [DCS 现场验收](docs/dcs-acceptance.md)
