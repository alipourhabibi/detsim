package sim

import "testing"

// Turn is the harness-side version of the same guard.
func TestTurnPanicsOutsideCallback(t *testing.T) {
	var turn Turn

	defer func() {
		if recover() == nil {
			t.Fatal("Turn.Ctx() outside a callback did not panic")
		}
	}()
	_ = turn.Ctx()
}

func TestTurnReturnsCtxInsideCallback(t *testing.T) {
	var turn Turn
	ctx := &Ctx{}

	turn.Enter(ctx)
	if got := turn.Ctx(); got != ctx {
		t.Fatal("Turn returned a different Ctx")
	}
	turn.Leave()

	defer func() {
		if recover() == nil {
			t.Fatal("Turn.Ctx() after Leave did not panic")
		}
	}()
	_ = turn.Ctx()
}
