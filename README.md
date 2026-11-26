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

## 租户审计证据包（`tenant_audit`）

`audit_slice` 的“payload LIKE 服务名 + 筛出一行即 verified”不能作为完整性证明。
`tenant_audit.py` 围绕固定验收序列（10 B 修订、11 D 就绪、12 另一租户请求 D、
13 B 启动、14 D 撤销、15 B 就绪裁决）给出两类租户认可的交付：

- `self`（客户自验）：全量事件的序号/事件 ID/payload/result 绑定哈希链与
  Merkle 检查点；给出租户图感知的限定序列、包含证明、撤销/超时/更早启动的
  区间缺席证明，以及字段白名单脱敏的依赖摘要。无关租户事件只以带随机盲因子的
  不透明叶子出现，不披露原文，也不提供可字典比对的标识哈希或可逆令牌。
- `custodian`（独立保管人）：原件交保管人验证全量事件与 `store.replay`，
  再用可追索的保管人密钥签发限定序列结论；客户不持有 D 的其他租户原文。

离线客户用 `verify_package` / `review_offline` 复核签名、链、Merkle 证明与结论。
删行重编号、改内容都会与已签名检查点冲突；恢复旧库由 `verify_database` /
`inspect_database` 识别为 `restored_stale_database`。证据包封存于
`audit_packages`，文档与哈希由触发器保护不可覆盖；签收绑定确切文档哈希。
迟到事件只能形成链接原包的更正（`issue_correction`），说明结论变化；客户离线
导入更正后可辨认旧包已 `superseded`。两种安排的原件持有人、客户可复算范围与
补证通知对象见 `delivery_obligations(mode)`。

```py
from cloudcourt import tenant_audit as ta
pkg = ta.acceptance_package(mode="self")       # 同一 10..15 事件
assert ta.verify_package(pkg)                  # 离线自验
pkg["conclusions"]["launch_admissible_at_13"]  # True
pkg["conclusions"]["serviceable_at_15"]        # False（14 已撤销 D）
```

回归：`python -B -m unittest discover -s tests -v`。
