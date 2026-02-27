# TimeTick Message

A system-generated WAL message acting as a **visibility barrier**: when a consumer sees TimeTick with timestamp T, all messages with TimeTick < T are committed and safe to consume. No future message will have TimeTick < T.

## IsPersist

TimeTick messages have an **IsPersist** flag. Persisted when real data messages exist in the batch; non-persisted for idle sync ticks (skips WAL backend write but still flows through interceptors and WriteAheadBuffer).

## Example

Three concurrent producers on one PChannel:

```
Producer A: Allocate(tt=10) → Append → Ack ✓
Producer B: Allocate(tt=11) → Append → (in-flight)
Producer C: Allocate(tt=12) → Append → Ack ✓

Watermark = 10 (tt=11 blocks advancement)
→ TimeTick message generated with Timestamp=10
→ Consumer knows: all messages with tt ≤ 10 are safe to consume

Producer B: Ack ✓
Watermark = 12 (10, 11, 12 all acknowledged)
→ TimeTick message generated with Timestamp=12
→ Consumer knows: tt=11 and tt=12 are now also safe
```

Consumer-side reordering via ReOrderByTimeTickBuffer:

```
WAL physical order:  [msg@tt=10] [msg@tt=12] [msg@tt=11]
On TimeTick@tt=10:   PopUtilTimeTick(10) → delivers [msg@tt=10]
On TimeTick@tt=12:   PopUtilTimeTick(12) → delivers [msg@tt=11, msg@tt=12]
                     (reordered by TimeTick, regardless of WAL write order)
```
