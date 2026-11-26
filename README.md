# 服务依赖启动协同台

离线微服务冷启动控制面模拟。使用 Python 3.11+ 标准库、SQLite 和本地探针事件；不启动容器，也不处理版本发布。

## 四个入口

- `contract`：服务修订、依赖边和启动令牌。封存依赖图前检查循环；关键上游缺席时，限时降级授权可单独改变启动结论。上游撤回后可重新裁定下游契约。
- `wave`：启动波次、节点启动槽和尝试代次。槽容量、令牌代次、上游就绪回执和期限共同决定是否下发；超时释放槽，迟到成功不能重用旧尝试。
- `ready`：探针样本、就绪回执和级联回滚案。仅当前尝试时间窗内的最新连续健康样本、当前服务修订和关键上游回执可发布就绪；上游撤回传播到下游。
- `revision_transition`：换代修订事件必须声明接口兼容性、数据迁移阶段、受影响依赖边和允许的回滚窗口，并对已运行的下游做有证据的再裁定。旧回执被取代而非复活；客户流量按读写分别裁决，业务负责人签字的限时许可只覆盖指定依赖与指定操作，只读许可不能放行写入；迁移完成且已确认账单核对前，全停不解除。回滚旧代须核对旧数据格式与客户确认前缀，不允许只把代次数字减回去。

业务实体分属不同表，所有写入通过 IMMEDIATE 事务。原始事件、业务裁决和完整状态可重放核对。

## 运行

```sh
python -m cloudcourt demo --db demo.sqlite
python -m cloudcourt replay --db demo.sqlite
python -m unittest discover -s tests -v
```

每个入口各有 `fixtures/<入口>-normal.json` 和 `-failure.json`。`fixtures/path.json` 在数据库服务启动后先发布上游就绪，再下发 API 服务启动波次。独立命令例如 `python -m cloudcourt contract --db state.sqlite --fixture fixtures/contract-normal.json`；跨入口运行时复用同一个新数据库。

测试覆盖依赖环、槽容量、旧代令牌、限时降级、探针迟到、上游回执撤销与下游回滚，以及换代后的读写裁决、降级窗口、未确认写入归属、迁移失败回滚与账单核对。关键业务结果通过 `readiness_receipts`、`boot_waves`、`rollback_cases` 和 `traffic_rulings` 观察，不仅看命令退出码。

降级就绪回执记录许可的最早到期时刻；`ready expire` 会撤销到期回执并传播到后继服务。启动尝试绑定节点槽代次，节点重建后旧探针不能发布就绪。服务修订换代会清除旧修订的依赖边。
