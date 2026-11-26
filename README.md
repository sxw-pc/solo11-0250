# 服务依赖启动协同台

离线微服务冷启动控制面模拟。使用 Python 3.11+ 标准库、SQLite 和本地探针事件；不启动容器，也不处理版本发布。

## 三个入口

- `contract`：服务修订、依赖边和启动令牌。封存依赖图前检查循环；关键上游缺席时，限时降级授权可单独改变启动结论。上游撤回后可重新裁定下游契约。
- `wave`：启动波次、节点启动槽和尝试代次。槽容量、令牌代次、上游就绪回执和期限共同决定是否下发；超时释放槽，迟到成功不能重用旧尝试。
- `ready`：探针样本、就绪回执和级联回滚案。仅当前尝试时间窗内的最新连续健康样本、当前服务修订和关键上游就绪可发布就绪；上游撤回传播到下游。
- `loadtest`：除原有 `plan`/`sample`/`report` 草稿试验外，提供对客户负责的启动容量试验与分级验收。`capacity_plan` 锁定服务修订代次、启动尝试、负载发生器身份、请求到达时间轴、并发客户类别和关键上游回执；测量窗口必须落在启动尝试生命期内。发生器漏发（时间轴上无请求记录）、超时未回和客户端重试都进入原始分母，分位延时只按业务请求计算，快速空请求单独计数且不能稀释吞吐与尾延时，各租户/优先级类别分别出成绩。验收 `accept` 同时裁定吞吐、尾延时、错误率、资源保护护栏和测量窗口，结果按版本追加，第一次不达标不会被重跑抹掉。`certify` 复核节点槽指纹与上游回执，节点扩容、上游退化或修订换代后旧报告对新实例一律拒绝发证；分级证书区分 full 与 partial（候补期限与服务抵扣），签发、客户签收（`sign`/`onboard`）与撤回（`withdraw`）各自留痕。试验流量只能进入登记的隔离数据集，生产写入会被拦截。`fixtures/capacity-acceptance.json` 是含漏发请求、超时、重试与空请求的跨模块重放样例。

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
