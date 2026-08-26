> 当前状态：v2 的代码实现已完成；工程结构重构在 `refactor/v2-layout` 分支进行。本文主体保留为规划和设计历史；实际部署、配置和验收以仓库根目录 `README.md`、`docs/architecture.md` 和 `docs/dcs-acceptance.md` 为准。现在剩余的唯一硬门槛是：在真实 Win7 32 位 DeltaV DCS 电脑上通过 .NET 2.0/3.5 x86 编译，并完成 v1/v2 Historian 数据对比。

可以。这个项目现在已经从“能不能读 DeltaV 历史数据”进入到“做一套稳定的数据同步系统”的阶段了。下面我按实际落地来规划，不追求一开始就做得很复杂，而是分阶段把风险逐个消掉。

一、项目目标

最终要实现的是：

DeltaV APP Historian         ↓ DCS 端 HistorySync.exe         ↓ 本地 spool 缓存         ↓ 网线 / HTTP         ↓ 数据库电脑 HistoryReceiver         ↓ PostgreSQL         ↓ 查询 / Python / Grafana / 报表

系统同时支持三种数据任务：

sync 日常自动增量同步，每 5 分钟执行

init 第一次初始化历史数据，可以指定很长时间范围

backfill 补历史数据，可以指定任意时间范围和任意 Tag

另外保留：

validate 检查 tags.txt 中的 Tag 是否在 Historian 中有效

---

二、核心设计原则

这个项目建议坚持几个原则：

1. DCS 端尽量简单 不安装 PostgreSQL、不安装 Python、不安装额外复杂软件。
2. 数据库电脑负责复杂逻辑 PostgreSQL、Receiver、统计、查询、可视化都放到另外一台电脑。
3. 先本地落盘，再发送 避免网络故障导致数据丢失。
4. 数据库允许重复发送 DCS 可以反复补同一时间范围，数据库负责去重。
5. 所有模式共用同一套 Historian 读取核心 sync / init / backfill / CSV 导出 都调用同一个 Core。
6. 配置文件文本化 DCS 上只需要 CMD + Notepad 就能维护。

---

三、完整系统架构

建议最终拆成 4 个部分。

DeltaVHistory.Core     ↓ 负责： 连接 APP 解析 Tag 读取 Raw History 时间范围处理 自动分片 去重 状态解析

HistoryReader.exe     ↓ 人工 CSV 导出

HistorySync.exe     ↓ 自动采集 sync init backfill spool HTTP Sender

HistoryReceiver     ↓ 运行在数据库电脑 接收批次 校验 写 PostgreSQL 返回 ACK

数据库端：

PostgreSQL     ├── history\_raw     ├── tags     ├── sync\_jobs     ├── sync\_batches     └── collectors

---

四、项目目录规划

DCS 端建议最终目录：

C:\\DeltaVHistory\\ │ ├─ HistoryReader.exe ├─ HistorySync.exe │ ├─ config.ini ├─ tags.txt │ ├─ spool\\ │    ├─ pending\\ │    ├─ sending\\ │    ├─ failed\\ │    └─ archive\\ │ └─ logs\\      ├─ sync\_20260826.log      └─ reader\_20260826.log

数据库电脑：

D:\\DeltaVHistoryServer\\ │ ├─ HistoryReceiver.exe ├─ receiver.ini │ ├─ logs\\ │ ├─ backup\\ │ └─ sql\\      ├─ 001\_create\_database.sql      ├─ 002\_create\_tables.sql      └─ 003\_create\_indexes.sql

---

五、DCS 端 Core 设计

这是整个系统最重要的一层。

建议接口最终整理成：

HistorianClient

Connect\(server\) Disconnect\(\)

ResolveTag\(tag\) ResolveTags\(tags\)

ReadRaw\(     tag,     start,     end \)

ReadRawRange\(     tag,     start,     end,     maxSamples \)

内部继续使用我们已经验证成功的：

DeltaV.Historian.DvCHDataAccess.dll

链路：

Initialize     ↓ ReadInterface     ↓ createConnection\("APP"\)     ↓ getServerTagHandles\(\)     ↓ createTimeSpan\(\)     ↓ FILETIME     ↓ readRaw\(\)

这一部分已经有实际运行基础，所以后续不要大改，只做封装。

---

六、HistorySync.exe 的 CLI 设计

最终建议：

日常同步

HistorySync.exe sync

无需人工输入时间。

读取 config.ini：

\[Historian\] Server=APP

\[Sync\] IntervalMinutes=5 LookbackMinutes=15 EndDelaySeconds=30 MaxSamples=10000

\[Files\] Tags=tags.txt Spool=spool Logs=logs

\[Receiver\] Url=http://192.168.10.2:8080/api/history TimeoutSeconds=15

例如 09:35 执行：

Start = 09:19:30 End   = 09:34:30

每次有重叠是故意的。

---

初始化

例如导入最近一个月：

HistorySync.exe init ^

--tags tags.txt ^

--start "2026-07-01 00:00:00" ^

--end "2026-08-01 00:00:00" ^

--slice 1d

内部：

07-01 → 07-02 07-02 → 07-03 07-03 → 07-04 ...

不会一次读整个月。

---

补数据

HistorySync.exe backfill ^

--tags tags.txt ^

--start "2026-08-20 08:00:00" ^

--end "2026-08-20 12:00:00" ^

--slice 1h

单 Tag：

HistorySync.exe backfill ^

--tag "TI-021007/AI1/PV.CV" ^

--last 2d ^

--slice 6h

---

Tag 验证

HistorySync.exe validate --tags tags.txt

返回：

\[OK\] TI-021007/AI1/PV.CV \[OK\] LIA-021008/AI1/PV.CV \[UNKNOWN\] TEST-001/AI1/PV.CV

---

七、时间参数设计

支持：

m = 分钟 h = 小时 d = 天

例如：

--last 30m

--last 6h

--last 1d

--last 7d

初始化、补数据再支持：

--slice 30m

--slice 1h

--slice 6h

--slice 1d

绝对时间：

yyyy-MM-dd HH:mm:ss

例如：

--start "2026-08-20 08:00:00"

--end   "2026-08-21 08:00:00"

---

八、Tag 配置设计

tags.txt：

# 温度

TI-021007/AI1/PV.CV TI-021008/AI1/PV.CV

# 液位

LIA-021006/AI1/PV.CV LIA-021008/AI1/PV.CV

# 流量

FIC-021001/PID1/PV.CV

以后增减 Tag：

Notepad tags.txt

即可。

不需要重新编译程序。

---

九、Spool 缓存设计

这是可靠性的核心。

一个同步批次建议：

spool\\pending\\     batch\_20260826\_093500\_0001.csv     batch\_20260826\_093500\_0001.meta

CSV：

Tag,Timestamp,Value,DataType,Flags,SequenceNo,ArchiveStatus TI-021007/AI1/PV.CV,2026-08-26 09:30:01.250,52.341,Float,,12,0 TI-021007/AI1/PV.CV,2026-08-26 09:30:18.600,52.356,Float,,13,0

meta：

BatchId=20260826\_093500\_0001 Mode=sync Server=APP Start=2026-08-26 09:19:30 End=2026-08-26 09:34:30 Rows=527 CreatedAt=2026-08-26 09:35:04

生命周期：

采集  ↓ 写临时文件 .tmp  ↓ 完成后 rename 到 pending  ↓ Sender 发送  ↓ 服务器 ACK  ↓ 成功删除或进入 archive

这样不会发送“写了一半”的文件。

---

十、第二阶段：网络传输

DCS：

HistorySync.exe         ↓ 读取 pending batch         ↓ HTTP POST

数据库电脑：

HistoryReceiver 监听 TCP 8080

API：

POST /api/history/batch

请求带：

batch\_id collector\_id mode start end samples

服务器成功：

{   "ok": true,   "batch\_id": "20260826\_093500\_0001",   "received": 527 }

DCS 收到成功后：

pending → 删除

失败：

pending 保留

下一次继续重试。

---

十一、数据库设计

我仍然建议 PostgreSQL。

history\_raw

核心表：

tag timestamp sequence\_no value\_double value\_text data\_type archive\_status flags received\_at batch\_id

建议唯一键：

\(tag, timestamp, sequence\_no\)

如果后面确认 sequence\_no 不稳定，再调整。

数据库写入：

INSERT ... ON CONFLICT \(...\) DO UPDATE / DO NOTHING

这样：

sync init backfill

可以重复跑。

---

tags

保存：

tag description unit enabled created\_at updated\_at

后面可以加入：

设备 区域 工艺段 类别

---

sync\_jobs

记录：

job\_id mode start\_time end\_time status total\_tags total\_rows started\_at finished\_at error\_message

状态：

pending running success failed partial

---

sync\_batches

记录每个实际发送批次：

batch\_id job\_id collector row\_count received\_at status

这样以后可以追踪：

DCS 发了没有 服务器收了没有 数据库写入多少

---

十二、定时任务设计

DCS 不常驻死循环。

使用：

Windows Task Scheduler

每 5 分钟：

HistorySync.exe sync

每次：

启动  ↓ 读取  ↓ spool  ↓ 发送  ↓ 退出

下一次再重新启动。

比常驻：

while\(true\) Sleep\(...\)

更可靠。

---

十三、每日补账机制

建议每天额外跑一次：

最近 24 小时 backfill

例如凌晨 02:00：

HistorySync.exe backfill --last 1d --slice 6h

数据库自动去重。

这可以覆盖：

网络中断 程序短时失败 Historian 延迟写入 数据库电脑重启

---

十四、日志设计

DCS 日志至少记录：

启动时间 模式 读取时间范围 Tag 数量 Tag 错误 读取行数 dataTruncated 自动分片 spool 文件 发送结果 HTTP 状态 运行耗时 异常信息

数据库电脑记录：

batch\_id 来源 IP 收到行数 有效行数 重复行数 新增行数 数据库耗时 错误

---

十五、网络设计

因为是 DCS 与数据库电脑直接网线连接，我建议使用独立网段。

例如：

DCS： 现场配置的 DCS IP（与数据库电脑同网段）

数据库电脑： 192.168.1.10

子网： 255.255.255.0

DCS 只主动访问：

192.168.1.10:8080

不需要数据库电脑反向访问 DeltaV。

这一点很重要：

数据流方向尽量： DCS → 数据库电脑

不要让外部电脑主动查询 DCS。

---

十六、项目分阶段实施

Phase 1：Historian Core + 本地 spool

目标：

DeltaV → spool

完成：

Core 封装

sync

init

backfill

validate

tags.txt

config.ini

时间范围

自动分片

spool

日志

验收

10\~50 Tag 最近1小时 最近1天 指定时间范围 重复执行 错误Tag 大数据范围

均不漏采、不崩溃。

---

Phase 2：Sender + Receiver

目标：

DCS → 数据库电脑

完成：

HTTP Sender

Receiver

batch\_id

ACK

超时

自动重试

spool 删除规则

验收

断网测试：

断开网线  ↓ spool 累积  ↓ 恢复网线  ↓ 自动补发

不能丢数据。

---

Phase 3：PostgreSQL

目标：

Receiver → Database

完成：

PostgreSQL

history\_raw

tags

sync\_jobs

sync\_batches

UPSERT

索引

验收

同一个 batch 发两次：

数据库数据不能翻倍

---

Phase 4：自动调度

完成：

每5分钟 sync 每天 backfill

观察至少：

3～7 天

确认：

无长期漏数据 无重复膨胀 spool 正常清理

---

Phase 5：数据查询与可视化

系统稳定后再做：

Grafana Python 报表 趋势分析 工艺分析

这一阶段不影响采集系统。

---

十七、技术栈建议

DCS 端：

C\# .NET Framework 2.0/3.5 x86

因为已经验证可以运行。

数据库电脑：

PostgreSQL

Receiver：

Go

我倾向 Go，原因是：

单 EXE 部署简单 性能足够 HTTP 服务成熟 PostgreSQL 支持成熟

也可以用现代 .NET，但 Go 在这种小型服务端程序上很干净。

---

十八、项目最终应该达到的状态

以后日常你不需要管它：

DCS Historian      ↓ 每5分钟      ↓ 自动同步      ↓ PostgreSQL

第一次建库：

HistorySync.exe init --start "2026-01-01 00:00:00" --end "2026-08-26 00:00:00"

某天发现数据缺失：

HistorySync.exe backfill --start "2026-08-20 00:00:00" --end "2026-08-21 00:00:00"

增加 Tag：

修改 tags.txt

然后重新同步。

---

当前最应该做的下一步

现在不要同时做全部模块。

第一个正式开发任务就是 Phase 1：

把目前已经能读取数据的 HistoryReader         ↓ 抽成 DeltaVHistory.Core         ↓ 新增 HistorySync.exe         ↓ 实现： sync init backfill validate config.ini tags.txt spool

Phase 1 验证稳定以后，我们再接网络。

这个顺序风险最低，而且每一个阶段都可以独立验证。

---

## v2 实施状态（2026-08-26）

本文件前面的内容是早期规划记录。当前实现以仓库根目录 `README.md`、
`docs/architecture.md` 和 `docs/dcs-acceptance.md` 为准。

当前已经落地：

1. DCS 端已抽出 `HistorianClient`、`HistorySample` 和范围读取逻辑，保留 DeltaV 数据截断后的自动二分读取。
2. `HistorySync` 正常路径直接从内存 Batch 发送 HTTP，不再先生成 Tag CSV 再重新读取组合 CSV。
3. 已加入 `LastCollectedEnd`、`LastAcceptedEnd`、`LastCommittedEnd` 状态，以及自动追赶、重叠窗口和动态切片。
4. spool 已调整为失败时使用的 Outbox，并按时间顺序补发 pending Batch。
5. 已支持 Console、Windows Service、`status`、`init` 和 `backfill`，不包含强制结束进程或重启 DCS 电脑的逻辑。
6. Receiver 已支持 PostgreSQL 同步提交模式：只有数据库事务 COMMIT 成功才返回 `committed=true`。
7. PostgreSQL 已增加 `history_samples` 完整字段模型，支持数值和文本值；旧 `history_raw` 保留为 v1 历史数据。

当前版本位于 Git 分支 `refactor/v2`，稳定旧版本标记为 `v1-legacy`。现场升级前，必须在 DCS 电脑运行
`scripts\\test-dcs-compatibility.bat`，并按 `docs\\dcs-acceptance.md` 完成真实 Historian 数据对比、断网补发和数据库恢复验证。
