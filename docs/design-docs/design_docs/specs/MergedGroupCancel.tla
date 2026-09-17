------------------------- MODULE MergedGroupCancel -------------------------
(***************************************************************************)
(* How a QueryNode search group survives the cancellation of one of the    *)
(* requests merged into it.                                                *)
(*                                                                         *)
(* A QueryNode folds compatible searches waiting in its scheduler queue    *)
(* into one group and runs them as a single segcore call. Cancelling one   *)
(* member must not end the others. Cancellation can land at any of four    *)
(* moments: before the request is enqueued, while the group waits in the   *)
(* queue, between the group leaving the queue and reaching the executor,   *)
(* and while the group is executing. Tests can sample those moments; this  *)
(* model enumerates every interleaving of them.                            *)
(*                                                                         *)
(* Modelled from internal/querynodev2/tasks/search_task.go (Merge,         *)
(* PruneCancelled, useGroupContext, Done) and                              *)
(* internal/util/searchutil/scheduler/concurrent_safe_scheduler.go         *)
(* (setupExecListener, schedule, exec) on branch                           *)
(* cancel-5-merged-group-isolation.                                        *)
(*                                                                         *)
(* Deliberately left out, because none of it bears on the property:        *)
(*  - which searches may merge (topk ratio, mvcc timestamp, expression     *)
(*    plan, segment set): merging is simply allowed or not, at will;       *)
(*  - time, deadlines and queue expiry;                                    *)
(*  - what a search computes. Its call either succeeds or fails.           *)
(* Every request carries one unit of work, so a group's size is its member *)
(* count and the scheduler's work counter counts requests.                 *)
(*                                                                         *)
(* Setting Isolation to FALSE models the behaviour this branch replaces,   *)
(* where only the group owner's context was ever consulted and the error   *)
(* it produced was handed to every request merged behind it. That is       *)
(* milvus-io/milvus issue 53508, and it is there so that the properties    *)
(* below can be shown to fail on it rather than merely to hold here.       *)
(*                                                                         *)
(* RememberNQ does the same for the second thing this branch changes: the  *)
(* work a group is debited is the work it was credited with when it was    *)
(* enqueued, not what is left of it after pruning. Setting it FALSE debits *)
(* the pruned size instead, which is the obvious way to write it.          *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
    Tasks,
    Isolation,    \* TRUE models this branch; FALSE models the code it replaces
    RememberNQ    \* TRUE debits the work the counters were credited with

VARIABLES
    loc,        \* Tasks -> where the request is
    owner,      \* Tasks -> the request whose group it belongs to
    cancelled,  \* the requests whose caller or an operator has cancelled them
    notified,   \* Tasks -> how many times the caller has been told an outcome
    result,     \* Tasks -> what the caller was told
    heldNQ,     \* the work the counters were credited with for the held group
    wCount,     \* the scheduler's waiting-request counter
    wNQ         \* the scheduler's waiting-work counter

vars == <<loc, owner, cancelled, notified, result, heldNQ, wCount, wNQ>>

Locations == {"new", "queued", "merged", "held", "handed", "running", "done"}
Outcomes  == {"none", "ok", "cancelled", "grouperr"}

\* A group travels as one object: only its owner carries the group's location,
\* and the requests merged behind it sit at "merged" until the group ends.
InPlay(t)  == loc[t] \notin {"new", "done"}
Members(o) == {m \in Tasks : owner[m] = o /\ InPlay(m)}

\* The scheduler holds one dequeued group at a time (lastWaitingTask).
NothingHeld == \A t \in Tasks : loc[t] # "held"

Init ==
    /\ loc       = [t \in Tasks |-> "new"]
    /\ owner     = [t \in Tasks |-> t]
    /\ cancelled = {}
    /\ notified  = [t \in Tasks |-> 0]
    /\ result    = [t \in Tasks |-> "none"]
    /\ heldNQ    = 0
    /\ wCount    = 0
    /\ wNQ       = 0

(***************************************************************************)
(* The environment: a caller disconnects, or an operator cancels.          *)
(***************************************************************************)
Cancel(t) ==
    /\ t \notin cancelled
    /\ loc[t] # "done"
    /\ cancelled' = cancelled \cup {t}
    /\ UNCHANGED <<loc, owner, notified, result, heldNQ, wCount, wNQ>>

(***************************************************************************)
(* Arrival. A request already cancelled is refused before it is enqueued   *)
(* and never reaches the counters. Otherwise it either starts a group of   *)
(* its own or merges into one already waiting. Merge refuses a cancelled   *)
(* owner and a cancelled newcomer, and only those two.                     *)
(***************************************************************************)
SubmitRefused(t) ==
    /\ loc[t] = "new"
    /\ t \in cancelled
    /\ loc'      = [loc      EXCEPT ![t] = "done"]
    /\ notified' = [notified EXCEPT ![t] = @ + 1]
    /\ result'   = [result   EXCEPT ![t] = "cancelled"]
    /\ UNCHANGED <<owner, cancelled, heldNQ, wCount, wNQ>>

SubmitAlone(t) ==
    /\ loc[t] = "new"
    /\ t \notin cancelled
    /\ loc'   = [loc   EXCEPT ![t] = "queued"]
    /\ owner' = [owner EXCEPT ![t] = t]
    /\ wCount' = wCount + 1
    /\ wNQ'    = wNQ + 1
    /\ UNCHANGED <<cancelled, notified, result, heldNQ>>

SubmitMerged(t, o) ==
    /\ loc[t] = "new"
    /\ t \notin cancelled
    /\ o # t
    /\ loc[o] = "queued"
    /\ owner[o] = o
    /\ o \notin cancelled
    /\ loc'   = [loc   EXCEPT ![t] = "merged"]
    /\ owner' = [owner EXCEPT ![t] = o]
    \* Merging adds no queue entry, so only the work counter is credited.
    /\ wNQ' = wNQ + 1
    /\ UNCHANGED <<cancelled, notified, result, heldNQ, wCount>>

(***************************************************************************)
(* Pruning, which happens twice: once when the group leaves the queue and  *)
(* again just before it executes. Members whose caller cancelled are told  *)
(* so and leave; the rest are regrouped behind whichever of them is first, *)
(* so cancelling the owner does not end the requests merged behind it.     *)
(***************************************************************************)
Pruned(M, dest, keepOwner) ==
    LET gone  == M \cap cancelled
        alive == M \ cancelled
        lead  == IF keepOwner \in alive THEN keepOwner ELSE CHOOSE x \in alive : TRUE
    IN
    /\ loc' = [t \in Tasks |->
                 CASE t \in gone            -> "done"
                   [] alive = {}            -> loc[t]
                   [] t = lead              -> dest
                   [] t \in alive           -> "merged"
                   [] OTHER                 -> loc[t]]
    /\ owner' = [t \in Tasks |->
                   IF alive # {} /\ t \in alive THEN lead ELSE owner[t]]
    /\ notified' = [t \in Tasks |-> IF t \in gone THEN notified[t] + 1 ELSE notified[t]]
    /\ result'   = [t \in Tasks |-> IF t \in gone THEN "cancelled" ELSE result[t]]

\* Leaving the queue. The work the counters were credited with is remembered
\* before any member is pruned away, so the debit matches the credit however
\* many members survive.
\* The group as a whole is ended with one error, which is what the code
\* before this branch did whenever the owner's context was cancelled.
EndWholeGroup(M) ==
    /\ loc'      = [t \in Tasks |-> IF t \in M THEN "done" ELSE loc[t]]
    /\ notified' = [t \in Tasks |-> IF t \in M THEN notified[t] + 1 ELSE notified[t]]
    /\ result'   = [t \in Tasks |-> IF t \in M THEN "cancelled" ELSE result[t]]
    /\ UNCHANGED owner

Dequeue(o) ==
    LET M == Members(o) IN
    /\ loc[o] = "queued"
    /\ owner[o] = o
    /\ NothingHeld
    /\ heldNQ' = Cardinality(M)
    /\ IF Isolation
       THEN /\ Pruned(M, "held", o)
            /\ IF M \subseteq cancelled
               THEN /\ wCount' = wCount - 1
                    /\ wNQ'    = wNQ - Cardinality(M)
               ELSE UNCHANGED <<wCount, wNQ>>
       ELSE IF o \in cancelled
            THEN /\ EndWholeGroup(M)
                 /\ wCount' = wCount - 1
                 /\ wNQ'    = wNQ - Cardinality(M)
            ELSE /\ loc' = [loc EXCEPT ![o] = "held"]
                 /\ UNCHANGED <<owner, notified, result, wCount, wNQ>>
    /\ UNCHANGED cancelled

\* Handing the group to the executor. This is where the counters are debited
\* on the ordinary path.
HandToExecutor(o) ==
    /\ loc[o] = "held"
    /\ loc'    = [loc EXCEPT ![o] = "handed"]
    /\ wCount' = wCount - 1
    /\ wNQ'    = wNQ - (IF RememberNQ THEN heldNQ ELSE Cardinality(Members(o)))
    /\ UNCHANGED <<owner, cancelled, notified, result, heldNQ>>

\* The second prune, inside the executor. A group left with nobody simply
\* never runs; its counters were already debited when it was handed over.
PruneBeforeRun(o) ==
    LET M == Members(o) IN
    /\ loc[o] = "handed"
    /\ IF Isolation
       THEN Pruned(M, "running", o)
       ELSE IF o \in cancelled
            THEN EndWholeGroup(M)
            ELSE /\ loc' = [loc EXCEPT ![o] = "running"]
                 /\ UNCHANGED <<owner, notified, result>>
    /\ UNCHANGED <<cancelled, heldNQ, wCount, wNQ>>

(***************************************************************************)
(* The search itself. It runs under a context that is cancelled only once  *)
(* every member has been cancelled, so one member cancelled mid-flight     *)
(* cannot fail the call the others are waiting on. When the call ends,     *)
(* each member is told its own outcome: a member cancelled while the call  *)
(* ran learns that, the others learn the call's result.                    *)
(***************************************************************************)
Run(o, err) ==
    LET M == Members(o) IN
    /\ loc[o] = "running"
    /\ err \in {"ok", "grouperr"}
    \* With isolation the call is cancelled only once every member is; without
    \* it the call rides on the owner's context alone.
    /\ IF Isolation
       THEN (M \subseteq cancelled) => (err = "grouperr")
       ELSE (o \in cancelled) => (err = "grouperr")
    /\ loc'      = [t \in Tasks |-> IF t \in M THEN "done" ELSE loc[t]]
    /\ notified' = [t \in Tasks |-> IF t \in M THEN notified[t] + 1 ELSE notified[t]]
    /\ result'   = [t \in Tasks |->
                      IF t \in M
                      THEN IF Isolation
                           THEN (IF t \in cancelled THEN "cancelled" ELSE err)
                           ELSE (IF o \in cancelled THEN "cancelled" ELSE err)
                      ELSE result[t]]
    /\ UNCHANGED <<owner, cancelled, heldNQ, wCount, wNQ>>

Arrive(t)  == SubmitRefused(t) \/ SubmitAlone(t) \/ (\E o \in Tasks : SubmitMerged(t, o))
Advance(t) == Arrive(t) \/ Dequeue(t) \/ HandToExecutor(t) \/ PruneBeforeRun(t)
                        \/ (\E e \in {"ok", "grouperr"} : Run(t, e))

Next == \E t \in Tasks : Cancel(t) \/ Advance(t)

Spec == Init /\ [][Next]_vars /\ \A t \in Tasks : WF_vars(Advance(t))

(***************************************************************************)
(* What must hold.                                                         *)
(***************************************************************************)
TypeOK ==
    /\ loc       \in [Tasks -> Locations]
    /\ owner     \in [Tasks -> Tasks]
    /\ cancelled \subseteq Tasks
    /\ result    \in [Tasks -> Outcomes]
    /\ heldNQ    \in 0..Cardinality(Tasks)

\* Every caller is answered exactly once. Answering twice would mean sending
\* twice on a channel nobody reads again; not answering leaves a caller
\* waiting forever.
AnsweredAtMostOnce == \A t \in Tasks : notified[t] <= 1
AnsweredWhenDone   == \A t \in Tasks : (loc[t] = "done") <=> (notified[t] = 1)

\* A cancelled request is told it was cancelled, never anything else.
CancelledLearnTheirOwnFate ==
    \A t \in Tasks : (loc[t] = "done" /\ t \in cancelled) => result[t] = "cancelled"

\* The isolation this branch exists for: a request nobody cancelled is never
\* told it was cancelled, however many of the requests beside it were.
NobodyElseIsCancelled ==
    \A t \in Tasks : (loc[t] = "done" /\ t \notin cancelled) => result[t] # "cancelled"

\* The scheduler's counters never go negative and return to zero once nothing
\* is in flight, whatever was pruned on the way.
CountersNeverNegative == wCount >= 0 /\ wNQ >= 0
CountersClearWhenIdle ==
    (\A t \in Tasks : loc[t] \in {"new", "done"}) => (wCount = 0 /\ wNQ = 0)

\* A group every one of whose members was cancelled is never searched.
Waiting == {t \in Tasks : InPlay(t) /\ loc[owner[t]] \in {"queued", "held"}}
CountersCoverTheQueue == wNQ >= Cardinality(Waiting)

Safety ==
    /\ TypeOK
    /\ AnsweredAtMostOnce
    /\ AnsweredWhenDone
    /\ CancelledLearnTheirOwnFate
    /\ NobodyElseIsCancelled
    /\ CountersNeverNegative
    /\ CountersClearWhenIdle
    /\ CountersCoverTheQueue

\* Every request eventually gets its answer.
Termination == <>(\A t \in Tasks : loc[t] = "done")
=============================================================================
