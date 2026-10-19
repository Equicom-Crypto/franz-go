package kgo

import (
	"context"
	"testing"
	"time"
)

// A metadata cursor migration stops/restarts the consumer under sessionChangeMu,
// independently of consumer.mu. Verify the equal-dirty inspection cannot run
// through that stopped-session interval (where there is no session lock to take).
func TestEqualDirtyInspectionWaitsForMetadataRestart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cl, err := NewClient(WithContext(ctx))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	c := &cl.consumer
	tp := newTopicsPartitions()
	tp.storeTopics([]string{"t"})
	c.d.tps = tp
	c.session.Store(c.newConsumerSession(tp))
	_, prior := c.stopSession() // same path as consumerSessionStopper.stop
	finished := make(chan struct{})
	started := make(chan struct{})
	go func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		close(started)
		c.addUninstalledEqualDirty(nil, map[string]map[int32]Offset{"t": {0: NewOffset().At(1)}}, tp)
		close(finished)
	}()
	<-started
	returnedWhileStopped := false
	select {
	case <-finished:
		returnedWhileStopped = true
	case <-time.After(100 * time.Millisecond):
	}
	c.startNewSession(prior).decWorker()
	select {
	case <-finished:
	case <-time.After(3 * time.Second):
		t.Fatal("equal-dirty inspection did not finish after metadata restart")
	}
	if returnedWhileStopped {
		t.Fatal("equal-dirty inspection crossed a metadata stop/restart without the session lifetime guard")
	}
}
