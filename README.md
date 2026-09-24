# 机器人作业数据回流服务

这是一个面向机器人作业数据团队的服务端应用，负责管理机型、场景、技能、作业记录、人工标注和数据集。服务使用 FastAPI 提供本地 HTTP 接口，以 SQLite 保存业务数据；质量评分、数据集审核、版本、订阅和统计分析均在同一进程内完成。

## 目录

- `main.py`：应用入口、健康检查和路由注册。
- `app/models`：业务实体及其关系。
- `app/routers`：基础资源、作业、数据集、分析与快照导出接口。
- `app/services`：评分、统计、策略目录、时间窗口、脱敏与快照导出工具。
- `app/seed_data.py`：可重复执行的示例数据初始化逻辑。
- `scripts/init_sample_data.py`：初始化脚本的兼容入口。

## 配置与运行

默认数据库文件为项目根目录的 `robot_data.db`，可以通过 `DATABASE_URL` 指定 SQLite 文件。快照产物默认写入 `./exports`，可通过 `EXPORT_DIR` 调整；`EXPORT_FAULT_INJECTION`（默认开启）允许通过 `X-Export-Fail` / `X-Export-Delay-Ms` 请求头注入失败与延迟，便于本地验证，生产环境应设为 `false`。安装依赖后运行 `python3 main.py`，服务默认监听 `8000` 端口；`GET /health` 返回服务状态，接口文档位于 `/docs`。

## 快照导出（外部复核）

合规人员可把某一审核时点的数据冻结为服务端快照并交给外部团队，避免逐项导出期间数据变化导致文件前后不一致。

接口均需通过 `X-Caller-Key` 标识调用方。系统启动时内置两个调用方：`compliance-team`（full 权限）与 `external-review`（restricted 权限），并内置 revision=1 的脱敏策略。

- `POST /api/v1/datasets/{id}/snapshot`：冻结并发起导出。幂等键由数据集版本、冻结数据指纹、调用方、权限档位、脱敏策略版本与指纹组成；相同请求返回同一快照，任一要素变化形成新版本。
- `GET /api/v1/exports/{id}`：查询 `preparing / ready / failed` 状态、计数、脱敏命中数与摘要值。
- `GET /api/v1/exports/{id}/download`：下载已完成快照（JSON，含成员、标注、质量摘要、内容摘要 `content_digest` 与成员级来源清单 `manifest.sources`），响应头携带 `X-Snapshot-Content-SHA256`；准备中/失败/文件缺失均返回 409，不会给出半成品。
- `GET /api/v1/exports`：列出快照（restricted 调用方仅见本人，full 可见全部），支持 `dataset_id`、`status` 过滤。
- 管理接口（full 权限）：`GET/POST /api/v1/export-callers`、`PATCH /api/v1/export-callers/{key}`、`GET/POST /api/v1/redaction-policies`（内容真正变化才会发布新策略版本）。

一致性保证：冻结与物化在响应返回前完成，后台线程仅负责序列化与原子落盘（临时文件 fsync 后 `os.replace`）；失败只落 `failed` 状态并清理临时文件，相同请求重试在同一快照记录上原地完成；服务重启时未完成的任务标记为失败，已完成快照文件逐字节保持。

初始化示例数据可执行 `python3 scripts/init_sample_data.py`。该命令会重建本地数据库并写入机型、场景、技能、作业、标注及数据集示例。

## 验证

运行 `python3 -m pytest -q` 执行服务和领域工具测试，运行 `python3 -m compileall -q app main.py scripts` 检查编译。测试只使用临时 SQLite 数据库，不需要额外服务。

快照相关测试包括：`tests/test_snapshot_unit.py`（脱敏与指纹单元测试）、`tests/test_snapshot_api.py`（进程内接口测试：冻结一致性、并发相同请求、空数据集、权限差异、策略/权限/版本变更、失败重试）、`tests/test_snapshot_live.py`（启动真实 uvicorn 子进程，验证并发修改期间的快照自洽性，以及服务重启后的下载一致性与中断恢复）。
