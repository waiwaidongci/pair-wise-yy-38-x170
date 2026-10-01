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
- `POST /api/batches`，总工授权批次，请求体`{"item_ids":[...]}`，建批次即冻结指令版本与未关闭操作记录
- `POST /api/batches/{batch_no}/confirm`，总工确认；可按批次号重试，已授权项复用首次结果，重放不产生重复审计
- `GET /api/batches`、`GET /api/batches/{batch_no}`
- `GET /api/audit`

授权批次语义：

- 建批次时生成同一`snapshot_id`，冻结各指令的当前版本与未关闭操作记录（含内容），批次、批次指令、冻结记录和审计事件共享该快照。
- 总工确认时采用乐观并发控制：冻结后若值班员并发提交/关闭操作记录，或指令版本变化，受影响指令记为`affected`留在批次内等待处理，其余指令照常授权；待现场恢复到冻结基线后再次确认即可。
- 按批次号重试天然幂等：状态更新（条件`UPDATE ... WHERE version=? AND status=?`）与审计写入在同一事务内完成，已授权项只返回首次结果。
- 批次状态全部持久化，服务重启后打开同一数据库即可继续确认未完成项。

允许角色：duty_officer, chief_engineer, dispatcher, viewer。库位超过汛限或入库流量上升时提升紧迫度；授权前必须有复核记录，执行后仍要闭环现场反馈。

## 测试

```bash
python3 -m unittest discover -s tests -v
```
