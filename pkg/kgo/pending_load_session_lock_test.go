package kgo

import (
	"context"
	"reflect"
	"sync"
	"testing"
)

// The production session loader owns usingCursors under listOrEpochMu, not
// consumer.mu. Exercise its real completion helper against the new seek helper.
func TestEqualDirtyCursorReadHoldsSessionLock(t *testing.T) {
	cl, err := NewClient()
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	c := &cl.consumer
	topics := newTopicsPartitions()
	tp := newTopicPartitions()
	source := &source{cl: cl}
	// Keep this state-only test from starting fetch workers on a fake source.
	source.fetchState.state.Store(stateContinueWorking)
	cur := &cursor{topic: "t", partition: 0, source: source}
	tp.v.Store(&topicPartitionsData{topic: "t", partitions: []*topicPartition{{cursor: cur}}})
	topics.storeData(topicsPartitionsData{"t": tp})
	session := c.newConsumerSession(topics)
	c.session.Store(session)
	c.usingCursors = make(usedCursors)
	c.usingCursors.use(cur)
	defer func() { session.cancel(); c.session.Store(noConsumerSession) }()
	loaded := loadedOffsets{loaded: []loadedOffset{{topic: "t", partition: 0, cursor: cur, offset: 0, leaderEpoch: 0}}}
	equal := map[string]map[int32]Offset{"t": {0: {at: 0, epoch: 0}}}
	var wg sync.WaitGroup
	wg.Add(2)
	start := make(chan struct{})
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 10000; i++ {
			session.handleListOrEpochResults(loaded)
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 10000; i++ {
			c.mu.Lock()
			c.addUninstalledEqualDirty(nil, equal, topics)
			c.mu.Unlock()
		}
	}()
	close(start)
	wg.Wait()
}

func TestReplaceSetMatchingMatrix(t *testing.T) {
	rows := []struct {
		name                 string
		offset               Offset
		wantType             listOrEpochLoadType
		wantAt, wantRelative int64
	}{
		{"exact-list", NewOffset().At(4), loadTypeList, 4, 0},
		{"exact-epoch", NewOffset().At(4).WithEpoch(1), loadTypeEpoch, 4, 0},
		{"relative-positive", NewOffset().At(4).Relative(3), loadTypeList, 7, 0},
		{"relative-negative-clamp", NewOffset().At(4).Relative(-8), loadTypeList, 0, 0},
		{"symbolic-relative", NewOffset().AtEnd().Relative(-2), loadTypeList, -1, -2},
		{"timestamp", NewOffset().AfterMilli(1234), loadTypeList, 1234, 0},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			for _, previous := range []listOrEpochLoadType{loadTypeList, loadTypeEpoch} {
				var loads listOrEpochLoads
				loads.addLoad("t", 0, previous, offsetLoad{replica: 2, Offset: NewOffset().At(99).WithEpoch(3)})
				loads.addLoad("t", 1, previous, offsetLoad{replica: 2, Offset: NewOffset().At(88).WithEpoch(3)})
				loads.replaceSetMatching(map[string]map[int32]Offset{"t": {0: row.offset, 2: NewOffset().At(5)}})
				got, ok := loads.List["t"][0]
				if row.wantType == loadTypeEpoch {
					got, ok = loads.Epoch["t"][0]
				}
				if !ok || got.at != row.wantAt || got.relative != row.wantRelative || got.afterMilli != row.offset.afterMilli || got.replica != -1 {
					t.Fatalf("got %+v, row %+v", got, row)
				}
				if loads.hasLoad("t", 2) {
					t.Fatal("invented absent partition")
				}
				unselected := loads.List["t"][1]
				if previous == loadTypeEpoch {
					unselected = loads.Epoch["t"][1]
				}
				if unselected.at != 88 || unselected.replica != 2 {
					t.Fatalf("mutated unselected %+v", unselected)
				}
			}
		})
	}
}

// Verify all assignment modes' real stop/filter path for pending loads in both
// session tracking states, with no live broker or source required.
func TestAssignModePendingMatrix(t *testing.T) {
	for _, how := range []assignHow{assignWithoutInvalidating, assignInvalidateAll, assignInvalidateMatching, assignPurgeMatching, assignSetMatching} {
		for _, loading := range []bool{false, true} {
			t.Run(how.String()+map[bool]string{false: "/waiting", true: "/loading"}[loading], func(t *testing.T) {
				// Cancel the client context before construction: background
				// loops read cl.ctx, so it must not be reassigned afterwards.
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				cl, err := NewClient(WithContext(ctx))
				if err != nil {
					t.Fatal(err)
				}
				defer cl.Close()
				c := &cl.consumer
				tp := newTopicsPartitions()
				tp.storeTopics([]string{"t", "u"})
				c.d.tps = tp
				s := c.newConsumerSession(tp)
				var pending listOrEpochLoads
				pending.addLoad("t", 0, loadTypeEpoch, offsetLoad{Offset: NewOffset().At(9).WithEpoch(0), replica: -1})
				pending.addLoad("t", 1, loadTypeList, offsetLoad{Offset: NewOffset().AtEnd(), replica: -1})
				pending.addLoad("u", 0, loadTypeEpoch, offsetLoad{Offset: NewOffset().At(8).WithEpoch(0), replica: -1})
				if loading {
					s.listOrEpochLoadsLoading = pending
				} else {
					s.listOrEpochLoadsWaiting = pending
				}
				want := pendingLoadSnapshot(pending, listOrEpochLoads{})
				c.session.Store(s)
				// New sessions must be inert, so stop finalization's asynchronous
				// load worker returns before touching the synthetic topic data.
				c.mu.Lock()
				assignments := map[string]map[int32]Offset{"t": {0: NewOffset().At(2)}}
				if how == assignWithoutInvalidating {
					assignments = nil
				}
				c.assignPartitions(assignments, how, tp, "")
				got, _ := c.stopSession()
				c.startNewSession(nil).decWorker()
				c.mu.Unlock()
				switch how {
				case assignInvalidateAll:
					want = listOrEpochLoads{}
				case assignInvalidateMatching:
					want.removeLoad("t", 0)
				case assignPurgeMatching:
					want.removeLoad("t", 0)
					want.removeLoad("t", 1)
				case assignSetMatching:
					want.addLoad("t", 0, loadTypeList, offsetLoad{Offset: NewOffset().At(2), replica: -1})
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("got=%+v want=%+v", got, want)
				}
			})
		}
	}
}
