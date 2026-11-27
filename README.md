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

## 人责交接与现场操作资格（duty 扩展）

`duty` 在原有 `shift`/`handoff`/`ack`（行为保持不变、可重放）之外，增加了一套“资质—现场会话—交接清单—签认—时段责任”模型。`wave.launch`、`wave.retry`、`ready.probe/publish/rollback` 会调用 `duty.evaluate_operation` 核班；不带 `actor` 的旧事件保持兼容，`duty arm_control` 可对指定现场开启强制记名。

- 资质 `certification` 绑定服务等级、现场/设备位置、有效期与培训签发方（`issuer` 注册认可机构）；同名的 `qualification` 字符串不是证书。`revoke_cert` 记录真实撤销时刻，撤销只对该时刻之后生效。
- 现场会话 `session_start` 可标记 `independent`；接入人必须用独立现场会话 `site_confirm` 确认实物节点与权限。
- 严格交接：`handoff_open` 自动锁定在跑 wave、未完成尝试、待处理回滚案、客户已确认操作（`customer_confirm`）和下一时限；`accept_item` 逐项接收，交出/接入双方分别 `sign`，`handoff_complete` 校验双签、独立现场确认、清单齐全（含交接后新冒出来的回滚案）与接入人当下资质。完成时刻取最晚证据时间，迟到签认（可用 `received_at` 区分签署与送达）不能倒填空窗；完成即关闭交出人现场会话。
- 异常时段：`withdraw`（病退，真实离开时刻，不改 `shift.ends`）、`extension`（双方签署、主管授权、可绑定具体尝试的受监督延班）、`emergency`（主管限时紧急授权，主管责任留档）、跨站代班（资质须覆盖目标现场，时段标记为 `cross_site`）。`pause`/`resume` 记录关键操作暂停与接入人在交接后的恢复授权。
- `duty.responsible_at(db, site, t)`、`duty.timeline(...)` 与 `duty.recompute(db)` 从事件日志、现场会话、资质和交接清单重算每一时刻的唯一负责人；空窗为 `status='gap'`，双人重叠为 `conflict`，并审计每个带 `actor` 的 wave/ready 事件。直接改 `shift.ends` 掩盖空窗会被 `store.replay` 的状态比对捕获。

跨班启动剧本见 `fixtures/shift-handover-cross-shift.json`（甲 0–10 时当班、乙 10 时起接班、B 服务尝试等待上游数据库核验、客户可见回滚案 pending）。`tests/test_shift_handover.py` 分别演练“主管批准甲受监督延班、12 时恢复”和“先暂停、乙逐项接收、15 时由乙恢复”两条合规路径，并覆盖迟到签认、病退、跨站代班、资质撤销/临期、并发 launch 与遗漏回滚案。

```sh
python -B -m unittest discover -s tests -q
```
