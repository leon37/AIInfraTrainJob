# 抢占—认领(claim)端到端复现记录

## 1. 复现目标

在上一篇(`preemption-release-cycle-2026-07-10.md`,控制器侧释放闭环)基础上,补上 **scheduler 侧抢占方落地**这一环,验证完整链路**无需人工介入**即可闭环:

```text
抢占方 Filter 全挂 → PostFilter 写 plan + 驱逐 victim → 返回 Unschedulable
-> 抢占方重排,PreFilter 读 plan 入 CycleState
-> Filter 按 plan 把抢占方 worker 钉在预留节点(每节点不超 count)
-> 抢占方整组 Permit 凑齐 -> Running
-> 控制器侧释放:victim 从 Preempted 回到 Queued -> Starting -> Running
```

复现时间：2026-07-14。环境：本地 `make run`(controller + scheduler),kind 2 worker(kind-worker / kind-worker2)。

## 2. 场景

`queue: team-a`。job-low、job-mid 各 worldSize 2、每 worker 7 CPU,先占住两台节点并 Running。提交高优 job-high(worldSize 2)→ 塞不下 → 触发抢占。

## 3. 关键现象(claim 干对的决定性证据)

scheduler 日志:
```text
node kind-worker  gang job-mid-attempt-0 curAvailableCPU 10900 singlePodResourcesRequests 10000
node kind-worker2 gang job-mid-attempt-0 curAvailableCPU 10900 singlePodResourcesRequests 10000
find victim gang candidates candidatesGang job-mid-attempt-0
preemption plan preemption detail  plans={"kind-worker":1,"kind-worker2":1}
TracerPlugin Filter
tracer-plugin Reserve  pod="job-high-attempt-0-rank-0" node="kind-worker2"
TracerPlugin Filter
tracer-plugin Reserve  pod="job-high-attempt-0-rank-1" node="kind-worker"
```

- plan = `{kind-worker:1, kind-worker2:1}`;
- `job-high-rank-0` → kind-worker2,`job-high-rank-1` → kind-worker。
- **一个 worker 一台、各 1、严丝合缝对上 plan**。若 claim 失效,两 worker 可能挤同台或跑到非预留节点——现在被精确钉在预留节点、每台不超 count,证明 PreFilter 读 plan + Filter 逐节点否决生效。

TrainJob phase 时序(`kubectl get trainjob -w`,提前开好 watch 抓到):
```text
job-high  Submitted -> Queued -> Starting -> Running
job-mid   Running   -> Preempted -> Queued -> Starting -> Running
```

## 4. 一个两层分离的观察(面试叙述点)

时序里 job-high 先到 `Starting`(**配额层**已准入),此刻 job-mid 仍 Running。即"配额准入 ≠ 物理装得下":配额是平台层的账(quota 够),物理上两组同时塞不进两台节点。**抢占是调度层用来消解"配额说行、物理说不行"矛盾的手段**。两层各管一层、抢占做和解——这套 demo 现场演出了这个结构。

## 5. 至此闭合了什么

抢占全链路(scheduler 侧 + controller 侧)端到端无人工介入闭环:识别 victim → 生成 plan(count=额外+baseline)→ 驱逐 → 抢占方按 plan 认领落地 → 整组 Running → victim 释放回流。设计速查见 `AIInfraSchedulerPlugin/preemption-claim-design.md`。

## 6. 下一颗雷(未解决)

**用完的 `preemptionDetail` 未清理。** 现在 Filter 会 act on 它:当某个 worker pod 在整组 Running **之后**又重新进入调度(例如被删后 `ensureWorkerPods` 在同一 attempt 下重建),会重读这张已消费的 plan,被 Filter 按过期节点集约束。触发面较窄(需同 attempt 内 Running 后重调度),但真实可达。待定:在"抢占方整组认领完成"这个窗口关闭时,由谁、在何处把 plan 清掉/失效。
