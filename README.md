# 建设外来入侵物种处置执行台账基础平台

本项目是一套可离线运行的 Python 服务端平台，供国家森林公园管理局、保护站、生态监测人员和巡护队管理野生动植物观察、采样标本、保护站资源、巡护路线、风险告警与处置工单。业务状态、角色权限、幂等结果和审计事件保存在 SQLite 中，可在单个 Linux 应用容器内运行。

## 目录

- src/collection_logistics/：保护站、巡护路线、应急资源、调拨计划和治理情景；
- src/taxonomy_lab/：调查协议、观察记录、异常复核、分析任务租约和生态结论；
- src/biosafety_ops/：园区监测、风险告警、处置工单、资源分配与入侵物种处置执行台账；
- src/biosafety_ops/plans.py：不可覆盖的处置方案版本（风险与影响范围快照、内容哈希、步骤时限）；
- src/biosafety_ops/ledger.py：从方案版本到独立复核结案的执行台账（签收、隔离、分区清除、返工、废弃物去向、幂等上报、可注入时钟查询视图）；
- fixtures/：离线验收使用的调查协议与结构化观察记录；
- tests/：领域规则、事务边界、权限、HTTP API 和命令行验收测试。

## 环境

- Linux
- Python 3.11 或更高版本
- 运行时只依赖 Python 标准库与 SQLite

## 测试

~~~bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
~~~

## 构建检查

~~~bash
python3 -m compileall -q src tests
~~~

## 离线验收

~~~bash
PYTHONPATH=src python3 -m collection_logistics.acceptance --workspace .
PYTHONPATH=src python3 -m taxonomy_lab.acceptance --workspace .
PYTHONPATH=src python3 -m biosafety_ops.acceptance
~~~

三条命令会在临时 SQLite 数据库中完成保护站和路线登记、资源调拨、生态观察分析与风险处置，不访问外部网络。

## HTTP 服务

~~~bash
PYTHONPATH=src python3 -m collection_logistics.api --database park.sqlite3 --host 127.0.0.1 --port 8080
PYTHONPATH=src python3 -m taxonomy_lab.api --database ecology.sqlite3 --host 127.0.0.1 --port 8081
PYTHONPATH=src python3 -m biosafety_ops.api --database safety.sqlite3 --host 127.0.0.1 --port 8082
~~~

服务提供浏览器无关的 JSON 接口和健康检查。进程重启后可以继续读取 SQLite 中的业务状态与审计历史。

## 处置执行台账（加拿大一枝黄花等入侵物种）

`biosafety_ops` 在风险告警与处置工单之上提供从方案版本到结案的执行台账：

- `POST /ledgers`：以当时有效的风险记录和影响范围快照生成不可覆盖的方案 v1（含通知名单、清除分区、药剂批次和各步骤时限），返回内容哈希；
- `POST /ledgers/{id}/revisions`：风险撤回（`risk_withdrawn`，直接关闭）或范围变化（`scope_changed`）只能形成新版本，旧版本下已完成与未完成的步骤原样保留；
- `POST /ledgers/{id}/progress`：按 通知签收 → 现场隔离 → 分区清除 → 复查样方 → 废弃物去向 → 独立复核 的严格门控上报；复查或复核不通过自动开出新一轮“返工 + 复查”；每条上报带 `report_id` 幂等键，外协队伍重复上报不会推进两次，同一 `report_id` 不同内容返回冲突；
- `GET /ledgers/{id}`：显示当前版本与全部历史版本、`missing_steps`、`next_step`、`next_deadline_at`（逾期标 `overdue`）以及每次变更（`changes`）所依据的 `plan_version`。

时限计算使用可注入时钟（`SystemClock`/`FrozenClock`）；复核结案要求复核人未参与过任何执行步骤。角色：`engineer/admin` 生成与修订方案，`operator` 执行上报，`quality` 独立复核。
