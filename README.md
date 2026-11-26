# 服务依赖启动协同台

离线微服务冷启动控制面模拟。使用 Python 3.11+ 标准库、SQLite 和本地探针事件；不启动容器，也不处理版本发布。

## 三个入口

- `contract`：服务修订、依赖边和启动令牌。封存依赖图前检查循环；关键上游缺席时，限时降级授权可单独改变启动结论。上游撤回后可重新裁定下游契约。
- `wave`：启动波次、节点启动槽和尝试代次。槽容量、令牌代次、上游就绪回执和期限共同决定是否下发；超时释放槽，迟到成功不能重用旧尝试。
- `ready`：探针样本、就绪回执和级联回滚案。仅当前尝试时间窗内的最新连续健康样本、当前服务修订和关键上游回执可发布就绪；上游撤回传播到下游。

业务实体分属不同表，所有写入通过 IMMEDIATE 事务。原始事件、业务裁决和完整状态可重放核对。

## 运行

```sh
python -m cloudcourt demo --db demo.sqlite
python -m cloudcourt replay --db demo.sqlite
python -m unittest discover -s tests -v
```

每个入口各有 `fixtures/<入口>-normal.json` 和 `-failure.json`。`fixtures/path.json` 在数据库服务启动后先发布上游就绪，再下发 API 服务启动波次。独立命令例如 `python -m cloudcourt contract --db state.sqlite --fixture fixtures/contract-normal.json`；跨入口运行时复用同一个新数据库。

测试覆盖依赖环、槽容量、旧代令牌、限时降级、探针迟到、上游回执撤销与下游回滚。关键业务结果通过 `readiness_receipts`、`boot_waves` 和 `rollback_cases` 观察，不仅看命令退出码。

降级就绪回执记录许可的最早到期时刻；`ready expire` 会撤销到期回执并传播到后继服务。启动尝试绑定节点槽代次，节点重建后旧探针不能发布就绪。服务修订换代会清除旧修订的依赖边。

## 冷启动电力承诺（power 增强）

`power` 在原有 `feed` / `reserve` / `observe` 之上增加可证实的冷启动供电承诺，相关验收见 `tests/test_startup_power.py` 与 `fixtures/startup-power-{normal,failure}.json`：

- `segment`（别名 `ups`）登记 UPS/母线等共享上游段；`topology` 记录馈线实际汇入的共享段、失电转供备用路径、保护余量 `margin_kw`、测量/校准代次 `sensor_generation` 与可用时段。两条名义馈线汇入同一 UPS 时，峰值在共享段合并核算，不能各卖一份冗余。
- `profile` 区分 `inrush_kw/inrush_ticks`（瞬时冲击）、`warm_kw/warm_ticks`（预热稳定）与 `run_kw`（运行期）。`commit` 按所有曲线相位切换点求最坏重叠峰值，并同时校验馈线额定与共享段额定（额定减去保护余量），另给出单故障（断一条馈线 / 失一个 UPS）打包结论。
- `production` 登记既有生产负载，默认优先级 100，高于新承诺（50）；故障评估按优先级打包，运行中服务优先得容量，装不下才生成 `degrade` 指令，未启动承诺生成 `block_start`。高优先级新承诺若会在单故障下挤掉数据库，`commit` 返回 `production_protection_breach`。
- `measurement` 带测量代次与 `recorded_at`（到达时刻），`calibrate` 推进传感器校准代次。只有当前代次、已到达且在新鲜期内的测量给出 `basis='measured'`；迟到 / 校准失效 / 过期分别返回 `power_measurement_late`、`sensor_calibration_stale`、`power_measurement_stale`。允许额定值兜底时结论必须自报 `basis='nameplate', verified=false`，不得把额定值当成实测容量。
- `commit` 生成 `power_committed` 承诺；`wave launch` 携带 `power_commitment` 时按**实际启动时刻**复核峰值并把尝试 id 绑定到承诺，抢启动会得到 `power_peak_exceeded`（槽有空位也不行）。`ready publish` 后承诺自动转为 `energized` 运行期负载。不带该字段的旧 wave 调用行为不变。
- `timeout` 不释放电力。清偿 `clear` 必须引用回落基线的有效 `measurement` 与 `field_ref/confirmed_by` 现场确认；仍在 running、缺测量、缺现场确认或残余负载超基线分别拒绝。清偿后用新承诺才能 retry/relaunch。
- `switch_feed`/`feed_outage` 与 `segment_outage`（别名 `ups_failure`）是不同事件：前者按备用馈线转供并重算可启动/降级集合，后者让共享段后的服务全部 `segment_lost`；`apply_directive` 执行降级/阻止，`availability` 输出客户既有服务可用性。
- 两种合法安排：错峰本地启动（共享 12 UPS，最坏峰值 11，客户零额外设施费，查询较晚开放）与索引租用独立 UPS 外部机架（实际并发但峰值分到两个段，租金计入 `power_costs` 客户承担，查询较早开放）。`fallback` 记录跨机架回退的记名责任并 `test` 目标路径容量。

运行：

```sh
python -B -m unittest discover -s tests -q
```
