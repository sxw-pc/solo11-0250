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

## 制品来源鉴别（artifact 扩展）

`artifact` 入口在登记之外增加逐层构建证据、独立复现、来源证书、撤销与分发测量；`contract seal` 可把修订锁定到一份已登记构建产物，启动波次在 `plan` 时固定其获准摘要，`launch` 与挂载都按摘要与证书实况判定。

- `evidence` 把源码提交、依赖锁文件、构建环境、工具链、签署身份、制品摘要与客户批准（已封存）的服务修订逐层绑定；层缺失、摘要或修订不符即拒绝。
- `revoke` 只追加撤销范围与处置记录，不改写历史证据、证书与挂载事实；证书有效性按信任基础是否被撤销实时判定。签名钥匙泄漏后，仅有旧签名不能签发来源证书：须由独立构建器按原始锁文件复现出同一摘要（`reproduce` 结论 `reproducible`）。差异无法解释、复现者不独立或复现工具链已被撤销时不得签发。
- `observe` 分别测量仓库镜像、节点缓存与挂载字节；带摘要的 `mount` 要求三处测量、尝试固定摘要与有效证书全部一致，镜像标签不参与判定。
- 替代制品（替代依赖）须重新确定许可证（`license`）与客户功能范围（`feature`），旧证书不覆盖新字节，也不能靠改 `bundle.state` 让旧证明生效。

`fixtures/artifact-compromised-builder.json` 重放完整泄钥场景：独立构建器复现 D1 后保留节点缓存字节并交付新来源证明；替代依赖洁净重建得到 D2，重新分发并暂停受影响功能。
