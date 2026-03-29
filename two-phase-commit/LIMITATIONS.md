# Limitations of two-phase commit (2PC)

This demo implements a simplified **two-phase commit** coordinator and two REST participants. The real protocol has important constraints. The points below are the main limitations you should understand before using 2PC in production.

## Blocking protocol

During **phase 1 (prepare)**, participants typically **reserve resources and hold locks** until the coordinator sends either **commit** or **rollback**. While they wait for that decision, other work that needs the same resources may be blocked. Under load or slow networks, the system can spend a long time waiting and throughput drops.

## Single point of failure

The **coordinator** decides the outcome. If it **crashes after participants have prepared** but before it records and broadcasts the decision, participants can be left in **PREPARED** with no clear instruction: they do not know whether to commit or abort. Recovering safely requires **persistent coordinator state**, **timeouts**, and often **human intervention** or a dedicated recovery protocol—none of which are implemented in this educational project.

## No timeout handling

This codebase does **not** implement timeouts on participant calls. If a participant **never responds** after prepare, the coordinator (and other participants) can wait indefinitely. Production systems need **deadlines**, **retries with care**, and **abort/timeout policies** so transactions do not stall forever.

## Scalability issues

2PC **coordinates every participant** for each transaction. As the number of participants or transaction rate grows, **latency and contention** tend to grow as well. The protocol does not scale out as easily as patterns that avoid a global synchronous decision across many services.

## No partial commits — all or nothing

2PC aims at **atomicity**: either every participant **commits** or every one **rolls back** (in the ideal case). There is **no built-in notion of partial success** or **eventual consistency** across services. Business flows that need **“best effort”** steps, **compensation after the fact**, or **long-running** work are usually modeled differently.

## Comparison with the SAGA pattern

| Aspect             | 2PC (this demo)                                                   | SAGA                                                                                        |
| ------------------ | ----------------------------------------------------------------- | ------------------------------------------------------------------------------------------- |
| Consistency model  | Strong atomicity across participants in one logical transaction   | Often **eventual consistency** via **forward steps + compensating transactions**            |
| Locking / blocking | Participants may hold resources while waiting for the coordinator | Typically **no global lock**; each local transaction completes and may be compensated later |
| Coordinator        | Single decision point for commit vs rollback                      | Usually **orchestration** or **choreography** with **per-step** success/failure handling    |
| Failure handling   | Depends on coordinator and all participants reaching agreement    | **Explicit compensations** (e.g. cancel order, refund payment) when a later step fails      |

SAGA fits many **microservice** scenarios where **latency, partial failure, and long workflows** make a synchronous global commit impractical. 2PC remains relevant in some **database / transaction manager** contexts but is **hard to apply** across heterogeneous, loosely coupled HTTP services without additional infrastructure and operational discipline.

---

_This file documents conceptual limitations of the protocol as illustrated by the `two-phase-commit/` demo; it is not an exhaustive treatment of distributed transactions._
