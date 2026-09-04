# Deterministic Simulator

A deterministic simulator for testing distributed systems.

You write a protocol(say [raft](https://github.com/alipourhabibi/raft) for example) and you want to test weather it works fine or not. You can bring up 5 nodes of your code and start playing with that. You may even catch some bugs. But distributed systems bugs need bad luck: a crash at one exact moment, two messages arriving in the wrong order and many more. You can find about them in production when it runs for a while or try to make them happen on purpose.

You can use simulator to run your protocol on it. The simulator runs on a fake network with fake failures. Nodes crash, messages get lost, the network splits in two. All run on one process with no real time and no real sockets.

Every run is repeatable. Same seed gives the same run every time on every machine.

You try to make the bad luck happen, thousands of time and tell you the seed number when things break.

## Quick start
 
Two nodes play ping pong. Each one sends a ball to the other every 50ms.
 
You write two files. The **protocol** is your real code. It does not know the
simulator exists. The **harness** connects your protocol with simulator.
 
### 1. The protocol
 
The node says what it needs (`Transport`) and someone else provides it.
 
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
 
Run it twice with seed 42 and the hash is the same. Change the seed and you get
a different run: different delays, different lost messages, a different order.

## Main ideas

### Time is fake

Time is a number that counts up. Nothing sleeps. run of 1 simulated hour finishes in few millisecond.

### Node handlers

You will implement 3 handler for each of your nodes to interact with simiulator.

```go
type Handler interface {
	OnMessage(ctx *Ctx, from int, msg Message)
	OnTimer(ctx *Ctx, name string)
	OnRestart(ctx *Ctx)
}
```
`OnRestart` also runs first time and not only after crash.

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

Inside the nodes callback, reads are dones right away but writes does not.
```
Put -> Sync -> CancelTimer -> SetTimer -> Send
```

Writes go to disk before messages go out. So a node can never answer a vote
that it has not saved yet. You cannot write that bug, even by accident.

Two things follow from this:

* A `Ctx` only works inside the callback that got it. If you save it and use it
  later, it panics.
* `Sync` covers every `Put` in the same callback, even ones written after it in
  the code. If you want a write that is not synced, do it in another callback.

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

## Faults

Is a value that can be run now or schedule for later.

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
that can reach both sides of a split(Bridge node). Those cases find the worst bugs.

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

To put your own state in the hash, add this method to your node:

```go
func (n *Node) StateDigest(w io.Writer) { /* write your fields */ }
```

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
disappears with no reason, you cannot read the run. So they all have one.

## Writing a protocol

Keep your protocol free of any `sim` import. Say what you need, and let a small
adapter connect it.

```go
// protocol/myproto: no sim import here
type Transport interface {
	Send(to int, msg Msg)
}

type Node struct {
	transport Transport
}
```

The adapter joins the two. `sim.Turn` holds the `Ctx` while a callback runs, so
if your protocol tries to send at the wrong time, it panics instead of using a
dead `Ctx`:

```go
type driver struct {
	node *myproto.Node
	turn *sim.Turn
}

func (d *driver) OnTimer(ctx *sim.Ctx, name string) {
	d.turn.Enter(ctx)
	defer d.turn.Leave()
	d.node.OnTick(name)
}

type transport struct {
    turn *sim.Turn
}

func (t transport) Send(to int, msg myproto.Msg) {
	t.turn.Ctx().Send(to, msg)
}
```

Your messages need two methods:

```go
type Message interface {
	HashInto(w io.Writer)
	Equal(other any) bool
}
```

Put every field that matters in `HashInto`. Put the same fields in `Equal`. If
they do not match, the hash says two runs are different and the compare tool
says they are the same, and you cannot trust either one.

There are some example protocols in the repo, all with no `sim` import:
`protocol/pingpong` and `protocol/maslave`.

## Making faults for you

You do not have to write the fault list by hand. `GeneratePlan` writes one from
a seed.

```go
plan := sim.GeneratePlan(sim.PlanConfig{
	Until:   10_000,
	MeanGap: 200,
	MaxDown: 1, // keep enough nodes alive for a quorum
	Weights: sim.DefaultWeights(),
}, nodes, r)

s.Start()
plan.Apply(s)
```

The whole plan is made before the run starts, not while it runs. This matters
for shrinking. If faults were picked during the run, removing one would change
all the ones after it, and you could not make a small plan from a big one.

`PlanConfig` also has `MaxDowntime`, `MaxPause`, `WipeChance` and `MaxDropPPM`.
They all have defaults.

`Weights` sets the mix. The numbers are relative, not percent. `{Crash: 3,
Heal: 1}` means three crashes for every heal.

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

## Rules to follow

1. Events run in `(At, Seq)` order and nothing else changes that.
2. A `Ctx` only works inside its own callback.
3. Writes happen after your callback, in the order `Put, Sync, CancelTimer,
   SetTimer, Send`. `Sync` covers the whole callback.
4. `HashInto` and `Equal` must use the same fields.
5. In protocol code:
   * ask for the clock and for random numbers through an interface. The sim
     harness gives you `ctx.Now()` and `ctx.Rand()`. Your production adapter
     gives you `time.Now()` and a real RNG.
   * no goroutines inside the node. In production, goroutines read sockets and
     wait on timers, then push events into a queue. One goroutine takes them
     out and calls the node. The sim does the same thing without threads.
   * no map loops where the order changes what happens. Sort a slice instead.
