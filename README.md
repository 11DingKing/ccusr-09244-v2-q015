# 机器人作业数据回流服务

这是一个面向机器人作业数据团队的服务端应用，负责管理机型、场景、技能、作业记录、人工标注和数据集。服务使用 FastAPI 提供本地 HTTP 接口，以 SQLite 保存业务数据；质量评分、数据集审核、版本、订阅和统计分析均在同一进程内完成。

## 目录

- `main.py`：应用入口、健康检查和路由注册。
- `app/models`：业务实体及其关系（含合规快照导出记录 `SnapshotExport`）。
- `app/routers`：基础资源、作业、数据集、分析与快照导出接口。
- `app/services`：评分、统计、策略目录、时间窗口工具与快照导出构建逻辑。
- `app/seed_data.py`：可重复执行的示例数据初始化逻辑。
- `scripts/init_sample_data.py`：初始化脚本的兼容入口。

## 配置与运行

默认数据库文件为项目根目录的 `robot_data.db`，可以通过 `DATABASE_URL` 指定 SQLite 文件。安装依赖后运行 `python3 main.py`，服务默认监听 `8000` 端口；`GET /health` 返回服务状态，接口文档位于 `/docs`。

快照导出文件默认写入 `./data/snapshot_exports/`（可通过 `SNAPSHOT_EXPORT_DIR` 配置）。文件先写同目录 `.tmp/` 临时文件并回读校验，再通过原子改名发布，因此构建失败或进程崩溃都不会留下可下载的半成品；进程启动时会自动重新构建卡在 preparing 的快照。

初始化示例数据可执行 `python3 scripts/init_sample_data.py`。该命令会重建本地数据库并写入机型、场景、技能、作业、标注及数据集示例。

## 合规快照导出

面向合规复核的服务端快照接口：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/v1/snapshot-exports` | 请求冻结并导出；相同请求（数据集版本+权限画像+脱敏策略版本）返回同一条快照（`reused=true`，HTTP 200），新请求 201 |
| GET | `/api/v1/snapshot-exports` | 查询导出列表，支持按数据集/团队/状态过滤 |
| GET | `/api/v1/snapshot-exports/{id}` | 查询单条状态：`preparing` / `completed` / `failed`（含错误原因与尝试次数） |
| GET | `/api/v1/snapshot-exports/{id}/download` | 下载已完成快照，响应头带 `X-Content-SHA256`，下载前服务端重新核验摘要 |
| POST | `/api/v1/snapshot-exports/{id}/retry` | 失败后重试，仍是同一条逻辑快照，`attempts` 递增 |

**冻结语义**：成员与标注在请求事务内一次性读入快照记录（`frozen_sources`），之后数据集增删成员、修改序列号或标注文本均不影响已生成的文件；逐字节重复下载结果一致。

**版本规则**：`request_fingerprint = SHA-256(数据集版本 + 权限画像 + 脱敏策略版本 + 产物结构版本)`。权限变化（角色或权限收窄）、脱敏规则版本（`MASKING_POLICY_VERSION`）或结构版本变化都会形成新版本。

**权限画像**：`owner`（内部所有，可见序列号与自由文本）、`reviewer`（内部，隐藏设备序列号、保留文本）、`external`（仅已发布数据集，序列号与自由文本全部替换为 `[REDACTED]`）；`permission_overrides` 只允许在角色默认权限上收窄，不允许提权。

**快照内容**：`manifest`（来源清单、脱敏计数、权限画像、策略版本、`content_sha256`）+ `quality_summary`（成员数、标注完成率、成败数、均分、等级分布）+ `members[]`（成员含脱敏标注与质量字段）。

## 验证

运行 `python3 -m pytest -q` 执行服务和领域工具测试，运行 `python3 -m compileall -q app main.py scripts tests` 检查编译。测试只使用临时 SQLite 数据库，不需要额外服务。新增的快照导出测试（`tests/test_snapshot_export.py`）覆盖：导出期间并发修改数据、空数据集、三种权限画像差异、构建失败无半成品及重试成功、进程重启后的下载一致性，以及相同请求并发下只生成一条快照。
