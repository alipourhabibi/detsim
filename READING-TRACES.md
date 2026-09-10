# Reading Traces

I will use the `cmd/lockcheck` for this.

```
go run -tags simfaults ./cmd/lockcheck/ -seed 1
```

It will produce this message and you see your protocol has bugs:

```
shrunk 12 faults to 2, 1 buggify to 1

seed 1
invariant: invariant broken at t=1519 #100: clients [2 3] are all inside the critical section, durable owner record says 3

schedule (2 faults):
  t=275 node 0 paused; duration: 247
  t=1416 node 0 crashed; wipe disk: false
  buggify lockserver/skip-sync

history: 39 ops, 5 ok, 34 unknown, 0 open

storage trace:
  a write with no sync after it, then a rollback, is the bug.

t=2      #2    n0   durableWrite    lock.owner
t=522    #40   n0   durableWrite    lock.owner
t=522    #41   n0   durableWrite    lock.owner
t=522    #45   n0   durableWrite    lock.owner
t=838    #63   n0   durableWrite    lock.owner
t=904    #65   n0   durableWrite    lock.owner
t=1214   #82   n0   durableWrite    lock.owner
t=1238   #84   n0   durableWrite    lock.owner
t=1416   #8    n0   crash
t=1416   #8    n0   rollback        1 unsynced keys lost
t=1507   #95   n0   restart
t=1514   #99   n0   durableWrite    lock.owner
exit status 1
```

That is the short version. `-trace storage` is the default and shows only the
disk lines. The six steps below use the full trace:

```
go run -tags simfaults ./cmd/lockcheck -seed 1 -trace all -out report.txt
```

Every `grep` on this page runs on that file.

## The line Format
```
t=1238   #84   n0   durableWrite    lock.owner
```

It has 5 parts:

| Part | Meaning |
|---|---|
| t=1238 | the time |
| #84 | the event number |
| n0 | the node |
| durableWrite | what happened |
| lock.owner | extra details |

### Event Number
It does two things:
1. Decides The order: When two events happen at the same time, the smaller number runs first.
2. It groups lines:
```
t=1238   #84   n0   event           deliver 2->0 Acquire#10
t=1238   #84   n0   durableWrite    lock.owner
t=1238   #84   n0   state           granted=-1 -> granted=2
```
The numbers are not in order in the file. You will see this:
```
t=1514   #99   n0   deliver 3->0
t=1516   #97   n0   deliver 1->0
```
#99 comes before #97. That is correct. The number is given when the event is made, not when it runs. Node 1 sent its message first, but the message was slower on the network, so it arrived later.

This is useful. A small number at a late time means something waited a long time. In the example, #8 runs at t=1416. It was made at the very start and waited 1416 units.

## The kinds of line

| Kind | Meaning |
|---|---|
| `event` | a node is handling something now |
| `sent` | a node sent a message |
| `dropped` | something did not happen, and why |
| `deferred` | the node is paused, so this waits |
| `durableWrite` | a write to disk |
| `sync` | the write is now really on the disk |
| `rollback` | a crash threw away writes that were never synced |
| `crash` | the node died |
| `restart` | the node came back |
| `pause` / `resume` | the node was frozen, then woke up |
| `timerSet` / `timerCancelled` | a timer was started or stopped |
| `buggify` | the code took a bad path on purpose |
| `state` | a node changed what it believes |

The two most useful are `state` and `rollback`. Start with those.

## The six steps

### Step 1. Read the failure line

```
invariant: invariant broken at t=1519 #100: clients [2 3] are all inside the
critical section, durable owner record says 3
```

This tells you **which nodes** (2 and 3) and **when** (1519).

The first word says which check found it. `invariant` runs after every event.
`history` runs at the end and looks at what the clients saw. `liveness` runs
after the faults are removed. Each one has its own section further down.

Everything after this goes backwards from that point.

### Step 2. Look only at the state lines

```
grep " state " report.txt
```

The `state` lines say what each node believes. There are far fewer of them than
normal lines, so this is much easier to read.

### Step 3. Find when the nodes went wrong

```
grep " state " report.txt | grep -E "n[123]"
```

Keep only the clients:

```
t=8      n1   waiting attempt=1  -> HOLDING attempt=1
t=530    n2   waiting attempt=6  -> HOLDING attempt=6
t=830    n2   HOLDING attempt=6  -> waiting attempt=6
t=909    n3   waiting attempt=10 -> HOLDING attempt=10
t=1209   n3   HOLDING attempt=10 -> waiting attempt=10
t=1246   n2   waiting attempt=10 -> HOLDING attempt=10
t=1519   n3   waiting attempt=13 -> HOLDING attempt=13
```

Read down the list. Every `HOLDING` has a `-> waiting` after it, except the last
two.

Client 2 goes in at 1246 and never comes out. Client 3 goes in at 1519. Both are
inside. That is the bug.

### Step 4. Find what caused it

```
grep "t=1246 \|t=1519 " report.txt
```

Look at the same time, one line above:

```
t=1246   #85   n2   event   deliver 0->2 Granted#10
t=1519   #100  n3   event   deliver 0->3 Granted#13
```

Both clients were told "you have the lock" by node 0.

So node 0 gave the lock away twice. The bug is on node 0, not on the clients.

### Step 5. Read what node 0 believed

```
grep " n0 " report.txt | grep state
```

```
t=1238   granted=-1 -> granted=2
t=1416   granted=2  -> down
t=1507   down       -> granted=-1
t=1514   granted=-1 -> granted=3
```

Read it as a story:

* 1238: I gave the lock to client 2.
* 1416: I died.
* 1507: I woke up. Nobody has the lock.
* 1514: I gave the lock to client 3.

Between 1416 and 1507 the server forgot. Nobody gave the lock back. It just
forgot.

### Step 6. Find what is missing

```
grep "rollback\|crash" report.txt
```

Look at the crash:

```
t=1416   #8   n0   crash
t=1416   #8   n0   rollback   1 unsynced keys lost
```

```
grep "#84" report.txt
```

One key was lost. Now go back to where that key was written:

```
t=1238   #84   n0   durableWrite   lock.owner
t=1238   #84   n0   sent           Granted#10 -> 2
```

The server wrote to disk. Then it told the client "you have the lock".

**There is no `sync` line between them.**

That is the bug. `durableWrite` only puts the data in memory. `sync` is what
puts it on the real disk. The server said yes before the data was safe. Then it
lost power, and the data was gone.

The fix is one line: call `Sync()` after the write.

---

## The most important idea

**The bug is almost never a line you can see. It is a line that is missing.**

You find it by looking at a place where something should be and is not:

| Missing line | What you see instead |
|---|---|
| `sync` after `durableWrite` | a `rollback` later throws the write away |
| a timeout on the server | one node holds something forever |
| a check on an old message | a slow reply is accepted after it stopped being true |

So step 6 is always: look at the two lines around the problem, and ask what
should be between them.

### The same steps on a different bug

The third row of that table looks like this in a trace. No crash, no rollback,
nothing lost:

```
t=400    #30   n2   sent      Acquire#3 -> 0
t=500    #34   n2   timerSet  retry
t=500    #34   n2   sent      Acquire#4 -> 0
t=610    #41   n2   event     deliver 0->2 Granted#3
t=610    #41   n2   state     waiting attempt=4 -> HOLDING attempt=4
```

Step 3 finds the `-> HOLDING` at 610. Step 4 finds the `Granted#3` that caused
it. Step 6 asks what is missing: the client is on attempt 4 and it accepted an
answer to attempt 3. The check that should throw away an old reply is not there.

Same six steps. Different missing line.

---

## Buggify lines

Your protocol marks places where something bad could happen. When the simulator
takes one of those paths it writes a line:

```
t=1238   #84   n0   durableWrite  lock.owner
t=1238   #84   n0   buggify       lockserver/skip-sync
t=1238   #84   n0   sent          Granted#10 -> 2
```

That line says the sync was skipped on purpose. Without it you would not know
whether the missing sync was your bug or this run making it happen.

The schedule above the trace lists which points were on:

```
schedule (2 faults):
  t=212 node 3 crashed
  buggify lockserver/skip-sync
```

To see how often each point was reached and how often it fired:

```
buggify:
    lockserver/skip-sync           seen=6      fired=6
  . lockserver/drop-release        seen=3      fired=0
  ! lockserver/long-retry          seen=0      fired=0
```

`seen` counts every time the code reached the point. `fired` counts the times it
took the bad path. A `!` means the code never ran that line, so that point is
doing nothing for you.

Buggify needs the build tag. Without `-tags simfaults` no point ever fires and
you will see none of these lines.

---

## The history

Above the trace there is one line about the history:

```
history: 39 ops, 5 ok, 34 unknown, 0 open
```

The history is what the clients saw. Not what happened inside the nodes. The
trace knows the server lost a write. No real client can know that. So a check
on the history is a check a real user could do.

To see it:

```
go run -tags simfaults ./cmd/lockcheck -seed 1 -trace history
```

```
op1    c1 acquire    key=1    0..8 ok
op2    c2 acquire    key=1    0..? unknown
op11   c2 acquire    key=5    400..412 ok
op20   c2 release    key=5    712..? unknown
```

Each line is one request. The two numbers are when it was sent and when the
answer came back.

There are three endings:

* `ok` means the answer came back.
* `unknown` means it never came back. This is not a failure. The server may have
  done it. The client will never find out.
* `open` means it was still waiting when the run ended.

Most lines are `unknown`, and that is normal here. Every retry gives up on the
attempt before it, and a release gets no reply at all.

Reading it: pair each `ok` acquire with the next release from the same client.
Two acquires with no release between them is the bug, and you can see it without
opening the trace.

---

## What to run

Start small. Most bugs need only a few lines.

```
go run -tags simfaults ./cmd/lockcheck -seed 1 -trace storage
```

This shows only writes, syncs, rollbacks, crashes and restarts. For a disk bug
this is often the whole answer, in about six lines.

If that is not enough:

```
go run -tags simfaults ./cmd/lockcheck -seed 1 -trace all -out report.txt
```

Then use `grep` on the file:

```
grep " state " report.txt        what the nodes believed
grep " n0 " report.txt           everything one node did
grep "#84" report.txt            one event and what it caused
grep "rollback\|crash" report.txt   the dangerous moments
grep "buggify" report.txt        the bad paths taken on purpose
```

To run one check at a time:

```
go run -tags simfaults ./cmd/lockcheck -check liveness
```

The checks stop at the first failure. If you have a known bug in one of them,
the later ones never run, and this is how you look at them anyway.

---

## Reading the fault list

Above the trace you see the faults that were used:

```
schedule (2 faults):
  t=275 node 0 paused; duration: 247
  t=1416 node 0 crashed; wipe disk: false
  buggify lockserver/skip-sync
```

This is after shrinking. The tool started with 12 faults and 1 buggify point,
removed them one at a time, and kept only the ones needed to still break the
rule.

Two faults is short enough to think about. Twelve is not. This is why shrinking
matters.

To see the full list before shrinking:

```
go run -tags simfaults ./cmd/lockcheck -seed 1 -no-shrink
```

---

## Four kinds of problem

Do not mix these up when reading a trace.

**Crash.** The node dies. It loses everything in memory. Writes that were not
synced are lost. When it comes back it is empty.

```
t=1416   n0   crash
t=1416   n0   rollback   1 unsynced keys lost
t=1507   n0   restart
```

**Pause.** The node is frozen. It keeps everything. Messages wait in a queue.
When it wakes up they all arrive at once.

```
t=275    n0   pause
t=305    n0   deferred   deliver 2->0 Acquire#4
t=306    n0   deferred   deliver 3->0 Acquire#4
t=315    n0   deferred   deliver 1->0 Release
t=522    n0   resume
t=522    n0   deliver 2->0 Acquire#4
t=522    n0   deliver 3->0 Acquire#4
t=522    n0   deliver 1->0 Release
```

Look at `t=522`. Three messages arrive in the same instant. This is why pause
finds different bugs from crash.

**Partition.** The node keeps running. Its timers still fire. It changes its
mind about things. It just cannot talk to anyone.

```
t=800   n0   dropped   deliver 1->0   (partitioned in flight)
```

A crashed node does nothing. A partitioned node does everything, alone. That is
why a partitioned node can still think it is the leader.

**Buggify.** Nothing is done to the node. The node takes a bad path inside its
own code, one that your protocol marked.

```
t=1238   n0   buggify   lockserver/skip-sync
```

The first three are things done to a node from outside. This one is inside.

---

## Why some messages disappear

Every message that does not arrive says why:

| Reason | Meaning |
|---|---|
| `loss` | the network dropped it, as configured |
| `stale epoch` | the target node crashed and came back; this was for the old one |
| `timer superseded` | a newer timer with the same name replaced this one |
| `partitioned in flight` | the message left, then the network split |
| `node down` | the target was crashed when it arrived |

If a message vanishes with no reason line, that is a bug in the simulator, not
in your protocol. Please let me know.

---

## Writing your own protocol

Two methods make your nodes readable in traces. Both are on the harness driver,
not on the protocol.

```go
// What this node believes right now. Keep it short: it runs after every event.
func (d *driver) StateString() string {
	return fmt.Sprintf("role=%s term=%d leader=%d", d.node.Role, d.node.Term, d.node.Leader)
}
```

The simulator only writes a `state` line when this string changes. So you get
the moments a node changed its mind, not one line per event.

```go
// The same state, as bytes, for the run hash.
func (d *driver) StateDigest(w io.Writer) {
	binary.Write(w, binary.LittleEndian, d.node.Term)
}
```

Both run between events, so they can only read plain fields. Do not call
`Send`, `Get`, `Put` or timers inside them. There is no `Ctx` there and it will
panic.

Make `StateString` say the thing your rule is about. If your rule is "one
leader", put the role and the term in it. Then step 3 works for your protocol
the same way it worked here.
