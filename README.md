# 建设外来入侵物种处置执行台账基础平台

本项目是一套可离线运行的 Python 服务端平台，供国家森林公园管理局、保护站、生态监测人员和巡护队管理野生动植物观察、采样标本、保护站资源、巡护路线、风险告警与处置工单。业务状态、角色权限、幂等结果和审计事件保存在 SQLite 中，可在单个 Linux 应用容器内运行。

## 目录

- src/collection_logistics/：保护站、巡护路线、应急资源、调拨计划和治理情景；
- src/taxonomy_lab/：调查协议、观察记录、异常复核、分析任务租约和生态结论；
- src/biosafety_ops/：园区监测、风险告警、处置工单、资源分配，以及从处置方案版本到关闭的入侵物种处置执行台账；
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

## 处置执行台账（加拿大一枝黄花处置闭环）

biosafety_ops 在风险告警与处置工单之上提供从“处置决定”到“关闭”的执行台账：

- **不可覆盖的方案版本**：`POST /work-orders/{id}/plans` 以当时有效的风险记录（告警快照、违规概率）和
  影响范围（物种、分区、面积）生成方案，计算内容指纹并整体哈希固化。方案表与事件流由 SQLite 触发器
  禁止 UPDATE/DELETE；风险撤回或范围变化只能生成 `scope_changed`/`risk_withdrawn` 新版本，旧台账封存为
  `superseded`，旧任务的步骤与事件永久保留来历，新版本中已完成分区以 `carried_from_step_id` 结转。
- **执行步骤**：通知签收（`ack-notification`）、现场隔离（`isolation`）、分区清除与药剂批次（`clearing`）、
  废弃物联单去向（`waste`）、复查样方（`review`）、独立复核结案（`closeout`）。复查不通过仅对失败分区
  在同一方案版本内开新拨次（attempt+1）返工；结案驳回则全分区返工。结案人必须具备审批权限且从未出现在
  现场执行事件中。
- **幂等上报**：外协队伍每次上报携带 `idempotency_key`；同键同内容重放返回首次结果（`duplicate:true`），
  不产生第二条事件、不二次推进步骤；同键不同内容返回 409。
- **时限**：所有截止时刻由可注入时钟（`SystemClock`/`FrozenClock`，服务构造时传入）按方案窗口推算，
  前序步骤完成才排定后续截止时刻；查询中 `overdue` 标识超时。
- **查询**：`GET /ledgers/{id}` 显示 `missing_steps`（含 `actionable`、`waiting_on`、`overdue`）、
  `next_deadline`（下一截止时刻）和仅追加哈希链 `timeline`（每个事件带 `plan_version`），并校验
  `event_chain_ok`；`GET /work-orders/{id}/plans` 列出版本谱系，
  `GET /work-orders/{id}/overview` 给出工单当前缺口总览。风险撤回收口于
  `POST /alerts/{alert_id}/withdraw`。

