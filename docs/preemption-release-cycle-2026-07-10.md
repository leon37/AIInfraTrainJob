# 抢占—释放闭环端到端复现记录

## 1. 复现目标

验证 gang-aware 抢占的**控制器侧**完整闭环(不含 scheduler 侧抢占方落地):

```text
scheduler PostFilter 在 victim PodGroup 写 PreemptedBy
-> TrainJob 控制器拆除 victim worker Pod、置 Preempted
-> 抢占方消失(本次用手动 delete 触发)
-> Watches mapfunc 经 PreemptedBy 索引 + ownerRef 唤醒 victim
-> case Preempted:Get(抢占方)=NotFound -> 删旧 PodGroup、保 Attempt、回 Queued
-> Queued -> Starting -> ensureWorkerPodGroup 重建干净 PodGroup -> victim 重新 Running
```

复现时间：2026-07-10。环境：本地 `make run`（controller + scheduler 各一），kind 2 worker。

## 2. 场景

| 对象 | 优先级 | worldSize | 每 worker CPU | 初始状态 |
|------|--------|-----------|---------------|----------|
| job-low | low | 2 | — | Running |
| job-mid | mid | 2 | 7 | Running（本次 victim） |
| job-high | high | 2 | — | 提交后 Pending，触发 PostFilter |

`queue: team-a`，三者同队。job-high 塞不下 → PostFilter 触发抢占。

## 3. 关键现象（逐段对上）

**拆除侧（piece 1）**
- `job-mid` `status.phase: Preempted`，`status.lastFailure.preemptedBy: job-high`；
- `job-mid` 两个 worker Pod `Terminating`。
- 结论：scheduler 写信号 → 控制器拆除 → 置 Preempted，通。

**释放 + 唤醒管道（piece 2 + Watches）**
- `kubectl delete trainjob job-high` → mapfunc 收 delete 事件 → 用 `status.preemptedBy` 索引 List 出 victim PodGroup → 跳 controller ownerRef 得到 `job-mid` → 入队；
- `case Preempted`：`Get(job-high)=NotFound` → `requeue=true` → 删旧 PodGroup、`Attempt` 不动、`Phase=Queued`；
- 观察到 `job-mid` 经历 `Preempted → Queued → Starting`，worker Pod 重建，最终稳定 `Running`。

**活锁修复双向坐实**
- 行为：`job-mid` 回到 Running 后稳定，未再次自我抢占；
- 对象：`kubectl get podgroup job-mid-attempt-0 -o jsonpath='{.status}'` 返回**空**——`preemptedBy` / `round` / `failed` 全缺席，重建出的是白纸。若复用旧 PodGroup，陈旧 `PreemptedBy` 会在 victim 再次 Running 时触发假抢占活锁。

## 4. 本次证明了什么

控制器侧抢占-释放闭环六步全绿：拆除 → 置 Preempted → 事件唤醒 → 释放判断 → 干净重建 → 回 Running。核心修复（回 Queued 保 `Attempt`、删旧 PodGroup 让 `ensureWorkerPodGroup` 走 NotFound→Create 重建白纸）经实跑验证消除了假抢占活锁。

## 5. 诚实边界 / 遗留

1. **三条释放路径只验了一条**。`case Preempted` 认抢占方 Running / Failed / 消失三种触发；本次仅验"消失"（delete→NotFound）。Running 路径依赖 scheduler 侧抢占方落地（见下）；Failed 路径可后续用"抢占方 gang 认输打满 round"单独验。
2. **scheduler 侧抢占方尚不能自行落地 Running**。当前 PostFilter 只算出"每节点可容纳抢占方 worker 数"的方案并写下，**缺少抢占方 worker 按调度顺序认领具体节点的 claim/提名逻辑**，因此抢占方本身停在 Pending。本次用手动 delete 代替"抢占方起来"来触发释放。补齐 claim 后，Running 释放路径方可端到端连通。
3. **`lastFailure.preemptedBy` 回到 Running 后仍残留**在 status 上（回 Queued 只翻 Phase、未清该记录）。信息性字段，暂无害；若后续有逻辑读 `lastFailure` 判定"是否失败过"会被误导，列入 polish。
4. **victim 选择挑了 job-mid 而非 job-low**。优先级上 low 更应先被牺牲，本次选 mid 大概率因"只有 mid 腾出的空间能凑出可行 slot"。属 scheduler 侧 victim 选择的后续打磨项。

## 6. 异步删除竞态（已知、自愈、未加守卫）

回 Queued 时 `r.Delete(旧 PodGroup)` 是异步的；`ensureWorkerPodGroup` 的"Get 到了"分支未检查 `DeletionTimestamp`。理论上若在旧对象尚未删净时 Get 到它，会当作活对象跳过重建。本次因 `Preempted→Queued→Starting` 中间隔两轮 reconcile 未触发；命中也会下一轮自愈，不退化为活锁。若 PodGroup 将来挂 finalizer，需补 `DeletionTimestamp` 守卫。
