# Deterministic Simulator

A deterministic simulator for testing distributed systems.

You write a protocol(say [raft](https://github.com/alipourhabibi/raft) for example) and you want to test whether it works fine or not. You can bring up 5 nodes of your code and start playing with that. You may even catch some bugs. But distributed systems bugs need bad luck: a crash at one exact moment, two messages arriving in the wrong order and many more. You can find out about them in production when it runs for a while, or try to make them happen on purpose.

You can use the simulator to run your protocol on it. The simulator runs on a fake network with fake failures. Nodes crash, messages get lost, the network splits in two. All run in one process with no real time and no real sockets.

Every run is repeatable. Same seed gives the same run every time on every machine.

The simulator makes the bad luck happen thousands of times and tells you the seed number when things break.

## Main ideas

### Time is fake

Time is a number that counts up. Nothing sleeps. A run of 1 simulated hour finishes in a few milliseconds.

### Node handlers

You will implement 3 handlers for each of your nodes to interact with the simulator.

```go
type Handler interface {
	OnMessage(ctx *Ctx, from int, msg Message)
	OnTimer(ctx *Ctx, name string)
	OnRestart(ctx *Ctx)
}
```
`OnRestart` also runs the first time and not only after a crash.

A factory builds your node. The simulator calls it again after every restart:

```go
type NodeFactory func(id int, deps Deps) Handler

type Deps struct {
	Id    int
	Store Store
	Rand  Rand
}
```

### Writes happen after your code returns

Inside the node's callback, reads are done right away but writes are not. The
simulator collects them and carries them out when your callback returns, in the
order you wrote them.

```go
ctx.Put("term", encode(5))
ctx.Sync()               // makes the line above durable, and nothing after it
ctx.Send(peer, vote)     // goes out over a log that is on disk
```

Two things follow from this:

* A `Ctx` only works inside the callback that got it. If you save it and use it
  later, it panics.
* `Sync` covers the `Put` calls before it and nothing after it. A `Put` written
  after the `Sync` is not durable, and a crash throws it away.

### Disk survives a crash, memory does not

Each node has a `Store`. It stays alive across crash and restart. Everything
else (your struct, your timers, waiting messages) is gone.

```go
ctx.Put("term", encode(5))
ctx.Sync()                  // safe: this survives a crash
ctx.Put("vote", encode(3))  // not synced: this is lost in a crash
```

When a node crashes, every write since the last `Sync` is thrown away. That is
the point of `Sync`. It is also the most useful disk failure you can test.

If you want to lose everything, crash with `wipeDisk` set to true.

## Quick start

Two nodes play ping pong. Each one sends a ball to the other every 50ms.

You write two files. The **protocol** is your real code. It does not know the
simulator exists. The **harness** connects your protocol with the simulator.

### 1. The protocol

The node says what it needs (`Transport`) and someone else provides it.

No `sim` import in the protocol.

```go
package pingpong

import (
	"encoding/binary"
	"io"
)

// What the node needs from the outside world.
type Transport interface {
	Send(to int, msg Ball)
}

// The message. The two methods are needed so the simulator can put messages in
// its hash.
type Ball struct {
	Round  uint64
	IsPing bool
}

func (b Ball) HashInto(w io.Writer) {
	var buf [10]byte
	buf[0] = 'b'
	binary.LittleEndian.PutUint64(buf[1:9], b.Round)
	if b.IsPing {
		buf[9] = 1
	}
	w.Write(buf[:])
}

func (b Ball) Equal(other any) bool {
	o, ok := other.(Ball)
	return ok && b.Round == o.Round && b.IsPing == o.IsPing
}

// The node itself. No clock, no network, no goroutines.
type Node struct {
	Id    int
	Peer  int
	Round uint64
	Pongs uint64

	transport Transport
}

func New(id, peer int, t Transport) *Node {
	return &Node{Id: id, Peer: peer, transport: t}
}

// OnTick is called every 50ms by whoever owns the timer.
func (n *Node) OnTick() {
	n.Round++
	n.transport.Send(n.Peer, Ball{Round: n.Round, IsPing: true})
}

func (n *Node) OnPing(from int, b Ball) {
	n.transport.Send(from, Ball{Round: b.Round, IsPing: false})
}

func (n *Node) OnPong(from int, b Ball) {
	n.Pongs++
}
```

Put every field that matters in `HashInto`. Put the same fields in `Equal`. If
they do not match, the hash says two runs are different and the compare tool
says they are the same, and you cannot trust either one.

There are some example protocols in the repo, all with no `sim` import:
`protocol/pingpong` and `protocol/maslave`.

### 2. The harness

This one imports `sim`. It does three jobs:

* turns simulator callbacks into protocol calls
* gives the protocol a `Transport` that sends through the simulator
* builds the `Sim`

`sim.Turn` holds the current `Ctx` while a callback runs. The transport reads
it from there. If the protocol tries to send at the wrong time, `Turn` panics
instead of using a dead `Ctx`.

```go
package pingpong

import (
	"fmt"

	"github.com/alipourhabibi/detsim/protocol/pingpong"
	"github.com/alipourhabibi/detsim/sim"
)

const Interval = sim.Duration(50)

// Sends through the simulator, using whatever Ctx is active right now.
type transport struct {
	turn *sim.Turn
}

func (t *transport) Send(to int, msg pingpong.Ball) {
	t.turn.Ctx().Send(to, msg)
}

// driver is what the simulator sees. It implements sim.Handler.
type driver struct {
	node *pingpong.Node
	turn *sim.Turn
}

func (d *driver) OnRestart(ctx *sim.Ctx) {
	d.turn.Enter(ctx)
	defer d.turn.Leave()

	ctx.SetTimer("tick", Interval)
}

func (d *driver) OnTimer(ctx *sim.Ctx, name string) {
	d.turn.Enter(ctx)
	defer d.turn.Leave()

	d.node.OnTick()
	ctx.SetTimer(name, Interval)
}

func (d *driver) OnMessage(ctx *sim.Ctx, from int, msg sim.Message) {
	d.turn.Enter(ctx)
	defer d.turn.Leave()

	ball, ok := msg.(pingpong.Ball)
	if !ok {
		panic(fmt.Sprintf("harness: got %T, want pingpong.Ball", msg))
	}

	if ball.IsPing {
		d.node.OnPing(from, ball)
	} else {
		d.node.OnPong(from, ball)
	}
}

func (d *driver) Node() *pingpong.Node { return d.node }

func Build(cfg sim.Config, a, b int) (*sim.Sim, map[int]*driver) {
	drivers := map[int]*driver{}

	factory := func(peer int) sim.NodeFactory {
		return func(id int, deps sim.Deps) sim.Handler {
			turn := &sim.Turn{}
			d := &driver{
				node: pingpong.New(id, peer, &transport{turn}),
				turn: turn,
			}
			drivers[id] = d
			return d
		}
	}

	s := sim.New(cfg, []int{a, b}, factory(b), sim.WithFactory(b, factory(a)))
	return s, drivers
}
```

The factory runs again after every restart.

### 3. Run it

```go
cfg := sim.Config{
	Seed:       42,
	MaxEvents:  1_000_000,
	TraceLevel: sim.TraceHashEvents,
	TraceKeep:  sim.KeepAll,
	NetworkConfig: sim.NetworkConfig{
		Delay:   sim.DelaySpec{Kind: sim.DelayUniform, Base: 1, Spread: 4},
		LossPPM: 10_000, // 1 percent of messages are lost
	},
}

s, drivers := Build(cfg, 0, 1)
s.Start()
```

Run it twice with seed 42 and the hash is the same. Change the seed and you get
a different run: different delays, different lost messages, a different order.

## Injecting faults

### ScheduleFault by hand

```go
s.ScheduleFault(1000, sim.NewCrashFault(1, false, 500))
s.ScheduleFault(2000, sim.NewPartitionFault([]int{0}, []int{1}))
s.ScheduleFault(3000, sim.NewHealFault(0))

if err := s.RunUntil(5000); err != nil {
	log.Fatal(err)
}

fmt.Printf("hash=%016x events=%d pongs=%d\n",
	s.Hash(), s.EventCount(), drivers[0].Node().Pongs)
```

Node 1 dies at 1000 and comes back at 1500. The network splits at 2000 and is
fixed at 3000. Some messages are lost the whole time.

### Writing the faults yourself

A `Plan` is just a list. You can write one by hand:

```go
plan := &sim.Plan{Faults: []sim.Scheduled{
	{At: 1000, Fault: sim.NewCrashFault(1, false, 500)},
	{At: 2000, Fault: sim.NewPartitionFault([]int{0}, []int{1, 2})},
}}
plan.Apply(s)
```

This is better than calling `ScheduleFault` one by one. The plan prints itself
when a test fails, so you can see what happened, and `Shrink` can make it
smaller.

### Generate a Plan from a seed

You do not have to write the fault list by hand. `GeneratePlan` writes one from
a seed.

```go
plan := sim.GeneratePlan(sim.PlanConfig{
	Until:   10_000,
	MeanGap: 200,
	MaxDown: 1, // keep enough nodes alive for a quorum
	Weights: sim.DefaultWeights(),
}, nodes, r)

plan.Apply(s)
```

The whole plan is made before the run starts, not while it runs. This matters
for shrinking. If faults were picked during the run, removing one would change
all the ones after it, and you could not make a small plan from a big one.

`PlanConfig` also has `MaxDowntime`, `MaxPause`, `WipeChance` and `MaxDropPPM`.
They all have defaults.

`Weights` sets the mix. The numbers are relative, not percent. `{Crash: 3,
Heal: 1}` means three crashes for every heal.

### Trying many seeds

```go
f := sim.Sweep(1, 10_000, gen, build, check)
if f != nil {
	f.Plan = sim.Shrink(f.Plan, func(p *sim.Plan) bool {
		return check(build(f.Seed, p)) != nil
	})
	t.Fatal(f) // prints the seed and the small plan
}
```

`Sweep` tries every seed from 1 to 10000 and stops at the first failure.

`Shrink` then makes the plan smaller. It removes one fault, runs again, and
keeps the removal if it still fails. A failure with 47 faults is too hard to
read. The same failure with 3 faults is usually clear.

`Shrink` must use the same seed as the failure. With a different seed you are
only asking "does it fail at all", not "does this fault matter".

## Trace

You can use the `cmd/lockcheck` to test the traces yourself.

It uses a buggy lock server and clients protocol which will fail via the sim and its harness.

Run it:
```
go run ./cmd/lockcheck                    sweep 1..500, show the first failure
go run ./cmd/lockcheck -seeds 1..10000    sweep further
go run ./cmd/lockcheck -seed 1            replay one seed
go run ./cmd/lockcheck -seed 1 -trace all show the whole trace, not just storage
go run ./cmd/lockcheck -mermaid           print a diagram to paste in an issue
go run ./cmd/lockcheck -out report.txt    write the report to a file
```

Read [READING-TRACES.md](READING-TRACES.md) to learn how to read the traces and
find the bug in this/your protocol.

## Faults

A fault is a value that can be run now or scheduled for later.

```go
s.InjectFault(f)          // now
s.ScheduleFault(at, f)    // later
```

### Node faults

| Function | What it does |
|---|---|
| `NewCrashFault(node, wipeDisk, downtime)` | Kills the node. Memory gone, timers gone, unsynced writes gone. If `downtime` is more than 0, it comes back after that. If it is 0, it stays down. |
| `NewRestartFault(node)` | Brings a dead node back. |
| `NewPauseFault(node, duration)` | Freezes the node. Messages wait in a queue. When it wakes up, they all arrive at once. |
| `NewResumeFault(node)` | Wakes a frozen node early. |

Crash, pause and partition are three different problems. Do not mix them up:

* A **crashed** node loses everything and comes back empty.
* A **paused** node keeps everything, then gets a pile of old messages at once.
* A **partitioned** node keeps running. Its timers fire, its state changes. It
  just cannot talk to anyone.

The third one is why partitions find bugs that crashes do not. The node still
thinks it is the leader.

### Network faults

| Function | What it does |
|---|---|
| `NewPartitionFault(a, b)` | Cuts all traffic between two groups. |
| `NewIsolateFault(node)` | Cuts one node off from everyone. |
| `NewDropLinkFault(from, to, ppm)` | Sets a loss rate on one direction of one link. |
| `NewDuplicateLinkFault(from, to, ppm)` | Same, for duplicate messages. |
| `NewResetConnectionFault(from, to)` | Resets the link, like a TCP reset. |
| `NewHealFault(id)` | Removes one rule. Pass 0 to remove all of them. |

A scheduled partition cannot give you back its rule id, so you can only remove
it with `NewHealFault(0)`. If you need to remove one rule by itself, do it
directly instead:

```go
id := s.Partition([]int{0, 1}, []int{2, 3})
s.Heal(id)
```

Rules are checked in order and **the last one that matches wins**. So an allow
rule after a block rule makes a hole in the block. This is how you build a node
that can reach both sides of a split(Bridge node). Those cases find the worst
bugs, the ones where two nodes both think they are the leader.

The simulator checks twice if a message can pass: once when you send it, once
when it arrives. A partition that appears while the message is flying still
kills it. The trace tells you which of the two happened.

## Network settings

```go
sim.NetworkConfig{
	Delay:          sim.DelaySpec{Kind: sim.DelayUniform, Base: 20, Spread: 100},
	LossPPM:        10_000, // 1 percent
	DuplicationPPM: 1_000,  // 0.1 percent
	IsReordering:   true,
}
```

`Spread` is the **width** of the random part, not the top. Messages take
between `Base` and `Base + Spread`. So the example above gives 20 to 120ms.

`DelayConstant` uses `Base` only and ignores `Spread`.

If `IsReordering` is false (the default), messages arrive in the order they
were sent, like TCP. If it is true, they can pass each other. Reordering finds
more bugs, but most real systems use TCP, so a bug it finds may not be possible
in production. Choose on purpose.

An empty `NetworkConfig` gives a perfect network: no delay, no loss, in order.
Start there. If your protocol breaks on a perfect network, the bug is in your
protocol.

## Buggify

Some bugs live in a very small moment. Your server writes to disk, then sends a
reply. In a real machine the gap between those two lines is about 50
microseconds. A run is 5 seconds. So a random crash almost never lands there.

You do not wait for luck. You mark the gap:

```go
s.storage.Put(keyOwner, buf[:])

if faults.Enabled && s.faults.Buggify("lockserver/skip-sync") {
    return // reply without syncing, and see what happens
}

s.storage.Sync()
```

Your protocol says where the danger is and what the bad thing would be. The
simulator only says yes or no.

### It costs nothing in production

`faults.Enabled` is a constant. In a normal build it is false, so the compiler
removes the whole line. Not a call, not a comparison.

```
go build ./...                 no fault injection in the program
go test -tags simfaults ./...  the lines are put back in
```

The price: the program you test is not the program you ship. Keep the bodies of
those `if` blocks small, and run your tests both ways.

### Your protocol still does not import sim

It takes an interface, the same way it takes `Transport`:

```go
type Injector interface {
    Buggify(name string) bool
}
```

Production passes `faults.NoOp`, which is always false. The harness passes a
small piece that asks the simulator.

### On for the whole run

A name is one point. Before a run starts, about one point in four is turned on.
A point that is on fires every time the code reaches it. A point that is off
never fires.

It does not decide again each time. A node that does the bad thing once and
then behaves is not much of a test. A node that keeps doing it is.

Each point is decided from its own name and the seed. So one point cannot
change another. Add a new point and nothing else moves: not the faults, not the
other points. Renaming a point does change it.

### The points are in the Plan, so they shrink

```
shrunk 19 faults to 2, 1 buggify to 1

schedule (2 faults):
t=212    node 3 crashed
t=773    node 0 crashed
buggify  lockserver/skip-sync
```

`Shrink` takes away points as well as faults, so a failure tells you which one
mattered.

### Check your points are reached

```
buggify:
    lockserver/skip-sync           seen=6      fired=6
  . lockserver/drop-release        seen=3      fired=0
  ! lockserver/long-retry          seen=0      fired=0
```

`seen` counts every time the code reached the point, on or not. `fired` counts
the times it took the bad path.

A `!` means the code never ran that line. That point is doing nothing for you.

### Where to put them

* after a write to disk, before the reply
* between two writes that should be one
* in a retry or backoff path
* when a node becomes leader

## Running

```go
s.RunUntil(t)             // until time t
s.Step(n)                 // n events, or until nothing is left
s.RunUntilQuiescent()     // until nothing is left
s.RunHealed(budget)       // fix everything, then run
```

They all return `ErrMaxEvents` if you hit the `MaxEvents` cap. Always set that
cap. If a node sets a timer with 0 delay from inside its own timer callback,
the run never ends, and the cap turns a hang into a test failure.

`RunHealed` is for checking that things get better. Some things cannot be
proved wrong at any single moment. "A leader is chosen" is one: no leader yet
does not mean no leader ever. So instead you remove all the faults, wake
everyone up, give it time, and check at the end.

```go
s.ScheduleFault(1000, sim.NewPartitionFault([]int{0}, []int{1, 2}))
if err := s.RunUntil(5000); err != nil {
	t.Fatal(err)
}

if err := s.RunHealed(10_000); err != nil {
	t.Fatal(err)
}
if leaders(s) != 1 {
	t.Fatal("no leader after healing")
}
```

## Checking

The simulator gives you runs. Something else has to decide whether a run was
correct. That something is called an oracle.

There are three kinds, and each catches what the others miss.

### Invariants

Something that must be true at every moment. Two leaders. Two clients holding
one lock. Register one before `Start` and the simulator runs it after every
event, so a failure stops at the exact event that caused it.

```go
s.AddInvariant(func(s *sim.Sim) error {
	if holders(s) > 1 {
		return errors.New("two holders")
	}
	return nil
})
```

An invariant must only read. If it sends or writes, it changes the run it is
watching. It runs after every event, so keep it cheap.

A crashed node has no handler, so `s.Handler(id)` gives nil. Every invariant has
to decide what that means for it.

### History

What was asked for, and what came back.

The trace looks inside the nodes. It knows one node wrote a value and then lost
it in a crash. Nobody outside the system can know that. So a check on the trace
can use facts that no real user has.

The history only has requests and answers. That is what a real user sees, so a
check on the history is a check a real user could do.

Your harness records operations:

```go
s.History().Invoke(ctx.Now(), clientID, "acquire", attempt)
// later
s.History().Complete(ctx.Now(), clientID, attempt)
```

Every operation ends `ok` or `unknown`. Unknown means the client never got an
answer. It is not a failure: the operation may have happened on the server and
the client will never find out. A checker that calls it a failure reports bugs
that are not real.

The simulator marks a crashed client's waiting operations unknown at the crash.
The client's memory is gone, so it can never match a reply again.

An operation that is still waiting also reads `unknown`, because at that moment
the client has no answer and a checker may assume nothing else. But it is not
finished. A run has phases, and a run mode is the end of a phase, not the end of
the run:

```go
s.RunUntil(until)     // the faulty phase
s.RunHealed(budget)   // heal, then run some more
```

An answer that arrives in the second phase still counts as `ok`. Reading the
history does not end it.

`Spans` turns a history into time windows so you can ask about overlaps:

```go
spans := s.History().Spans("acquire", "release", s.Now())
if a, b, found := sim.Overlapping(spans); found {
	// two clients at once
}
```

### Liveness

Something that must happen eventually. A leader is chosen. These cannot be
proved wrong at any single moment, so they are not invariants. Instead remove
the faults, wait, and check once at the end.

```go
if err := s.RunHealed(budget); err != nil {
	return err
}
// now assert the good thing happened
```

The healing is required. Under a permanent partition no protocol can make
progress, so asserting it would be asserting something false. The budget matters
too: too short and a slow but correct recovery looks like a failure.

## Looking at a run

```go
s.Hash()          // one number for the whole run
s.EventCount()
s.Now()
s.Nodes()
s.Up(id)          // is it healthy?
s.Status(id)      // Healthy, Crashed or Paused
s.Get(id, key)    // read the disk directly
s.Keys(id)
s.Handler(id)     // your node, for checks. nil while crashed
s.Trace()
```

### Trace levels

| Level | Cost | Finds |
|---|---|---|
| `TraceOff` | none | nothing |
| `TraceHashEvents` | small | two runs with different events |
| `TraceHashEventsAndState` | large | same events, different node state |

`TraceKeep` says how much history to keep. `0` keeps none and only builds the
hash. `KeepAll` keeps everything. A number `N` keeps the last `N`. Use
`KeepAll` in tests. Use a number for very long runs, where old history costs
too much memory.

`Len()` is how many entries you can read back: `0` when `TraceKeep` is `0`, and
at most `N` for a ring. `Count()` is how many were recorded in total, including
the ones a ring threw away.

To put your own state in the hash and show what it believes, add these methods
to your driver, not to your protocol node:

```go
func (d *driver) StateDigest(w io.Writer) { /* write your fields */ }
func (d *driver) StateString() string { /* return what your node believes. */ }
```

Both run between events, so they can only read plain fields. They cannot call
`Send`, `Get`, `Put` or the timers: there is no `Ctx` there.

The simulator writes a `state` line only when `StateString` changes, so you get
the moments a node changed its mind, not one line per event.

### Comparing two runs

```go
if a.Hash() != b.Hash() {
	if d := sim.FirstDivergence(a.Trace(), b.Trace()); d != nil {
		d.Report(os.Stdout, a.Trace().Entries(), b.Trace().Entries(), 5)
	}
}
```

Every dropped message has a reason in the trace: `stale epoch`, `timer
superseded`, `partitioned in flight`, `node down`, `loss`. If a message
disappears with no reason, you cannot read the run.

## Rules to follow

1. Events run in `(At, Seq)` order and nothing else changes that.
2. A `Ctx` only works inside its own callback.
3. Writes happen after your callback, in the order you wrote them. `Sync`
   covers the writes before it and nothing after it.
4. `HashInto` and `Equal` must use the same fields.
5. In protocol code:
   * ask for the clock and for random numbers through an interface. The sim
     harness gives you `ctx.Now()` and `ctx.Rand()`. Your production adapter
     gives you `time.Now()` and a real RNG.
   * no goroutines inside the node. In production, goroutines read sockets and
     wait on timers, then push events into a queue. One goroutine takes them
     out and calls the node. The sim does the same thing without threads.
   * no map loops where the order changes what happens. Sort a slice instead.
