# 水库防汛调度与操作确认

根据库位、入库流量、下游警戒和施工限制生成复核授权的泄洪指令。

## 模块结构

- `app.py`：参数解析、依赖组装和HTTP服务启动。
- `src/domain.py`：数据结构、错误、状态和基础校验。
- `src/rules.py`：状态机、角色矩阵、优先级、期限和关闭不变量。
- `src/repository.py`：SQLite建表、事务、版本控制和审计链。
- `src/service.py`：权限检查、用例编排、并发控制和审计。
- `src/http_api.py`：JSON路由和统一错误响应。
- `src/audit.py`：UTC时间和SHA-256审计事件。
- `static/index.html`：最小演示页。
- `tests/`：完整流程、规则和失败测试。

## 初始化与启动

```bash
python3 app.py --db ./data.db --port 8315
```

默认端口为`8315`，首次启动自动建库。使用`X-Actor`和`X-Role`请求头传递身份。

## 主要接口

- `GET /health`
- `GET /api/items`
- `POST /api/items`
- `GET /api/items/{id}`
- `POST /api/items/{id}/records`
- `POST /api/items/{id}/transition`，必须提交`expected_version`
- `GET /api/audit`
- `GET /api/batches`
- `POST /api/batches`，提交`batch_no`、`item_ids`和`target`（默认`authorized`）
- `GET /api/batches/{id}`
- `POST /api/batches/{id}/confirm`，总工确认；被并发修改的指令留在批次等待，其余照常完成
- `POST /api/batches/{id}/resume`，继续确认待处理指令（重新冻结当前版本与未关闭记录后授权）

允许角色：duty_officer, chief_engineer, dispatcher, viewer。库位超过汛限或入库流量上升时提升紧迫度；授权前必须有复核记录，执行后仍要闭环现场反馈。

## 授权批次

汛期值班员连续建多条调度指令，总工程师逐条授权。批次在创建时冻结每条指令的当前版本和未关闭操作记录；总工确认时若值班员同时提交了操作记录（指令被并发修改），受影响指令留在批次里等待处理，其余照常完成。

- 写入失败后按`batch_no`重试，只能复用首次结果，重放不会多出审计记录。
- 批次、调度指令、操作记录和审计记录都对应同一份快照；服务重启后未完成项仍可继续确认（`/resume`）。
- 审计记录通过`batch_id`关联到批次，可在`GET /api/audit`中查看。

## 测试

```bash
python3 -m unittest discover -s tests -v
```
