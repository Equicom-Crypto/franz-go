package kfake_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Wire keys for the control hooks below.
const (
	keyFetch                int16 = 1
	keyOffsetForLeaderEpoch int16 = 23
)

// Every wait is bounded. holdWindow is how long a parked observer keeps fetches out before the check looks for one.
const (
	waitLimit  = 15 * time.Second
	holdWindow = 750 * time.Millisecond
)

// resolvedEvent is one call to the position hook, copied out of the callback arguments.
type resolvedEvent struct {
	topic       string
	partition   int32
	offset      int64
	leaderEpoch int32
	hasCause    bool
	dataLoss    *kgo.ErrDataLoss
}

// positionRecorder implements the resolution and record-buffered hooks against a real client. Its log orders the
// events it sees. When block is set, the resolution call parks until release is closed, which holds the client's
// consumer lock, the same lock the production hook runs under.
type positionRecorder struct {
	block    bool
	entered  chan struct{}
	release  chan struct{}
	resolved chan resolvedEvent
	fetched  chan struct{}
	buffered chan struct{}
	enterOne sync.Once
	bufOne   sync.Once

	mu  sync.Mutex
	log []string
}

func newPositionRecorder(block bool) *positionRecorder {
	return &positionRecorder{
		block:    block,
		entered:  make(chan struct{}),
		release:  make(chan struct{}),
		resolved: make(chan resolvedEvent, 64),
		fetched:  make(chan struct{}, 1),
		buffered: make(chan struct{}, 1),
	}
}

func (r *positionRecorder) note(s string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.log = append(r.log, s)
}

// first returns the index of the first log entry equal to s, or -1.
func (r *positionRecorder) first(s string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, e := range r.log {
		if e == s {
			return i
		}
	}
	return -1
}

func (r *positionRecorder) OnFetchPositionResolved(topic string, partition int32, offset int64, leaderEpoch int32, cause error) {
	ev := resolvedEvent{topic: topic, partition: partition, offset: offset, leaderEpoch: leaderEpoch}
	if cause != nil {
		ev.hasCause = true
		errors.As(cause, &ev.dataLoss)
	}
	r.note("resolved")
	r.resolved <- ev
	if r.block {
		r.enterOne.Do(func() { close(r.entered) })
		select {
		case <-r.release:
		case <-time.After(waitLimit):
		}
	}
	r.note("release")
}

func (r *positionRecorder) OnFetchRecordBuffered(*kgo.Record) {
	r.bufOne.Do(func() {
		r.note("buffered")
		r.buffered <- struct{}{}
	})
}

// observeFetches logs every fetch request the cluster receives and lets the request proceed.
func observeFetches(c *kfake.Cluster, r *positionRecorder) {
	c.ControlKey(keyFetch, func(kmsg.Request) (kmsg.Response, error, bool) {
		r.note("fetch")
		select {
		case r.fetched <- struct{}{}:
		default:
		}
		return nil, nil, false
	})
}

func startPositionCluster(t *testing.T, topic string) (*kfake.Cluster, []string) {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c, c.ListenAddrs()
}

func newClient(t *testing.T, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return cl
}

func produce(t *testing.T, seeds []string, topic string, n int) {
	t.Helper()
	cl := newClient(t, kgo.SeedBrokers(seeds...), kgo.DefaultProduceTopic(topic))
	ctx, cancel := context.WithTimeout(context.Background(), waitLimit)
	defer cancel()
	for i := 0; i < n; i++ {
		if err := cl.ProduceSync(ctx, &kgo.Record{Value: []byte("v")}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
}

// pollOffsets polls until want records arrive and returns their offsets in order.
func pollOffsets(t *testing.T, cl *kgo.Client, want int) []int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), waitLimit)
	defer cancel()
	var offs []int64
	for len(offs) < want {
		fetches := cl.PollFetches(ctx)
		fetches.EachError(func(topic string, partition int32, err error) {
			t.Errorf("fetch error on %s/%d: %v", topic, partition, err)
		})
		fetches.EachRecord(func(r *kgo.Record) { offs = append(offs, r.Offset) })
		if ctx.Err() != nil {
			t.Fatalf("polled %d of %d records before the wait limit", len(offs), want)
		}
	}
	return offs
}

func expectOffsets(t *testing.T, got []int64, want ...int64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("offsets %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("offsets %v, want %v", got, want)
		}
	}
}

// gate parks the assignment at its resolution call, checks that no fetch request follows within holdWindow, then
// releases the hook.
func gate(t *testing.T, r *positionRecorder) {
	t.Helper()
	select {
	case <-r.entered:
	case <-time.After(waitLimit):
		t.Fatal("the position hook was never entered")
	}
	select {
	case <-r.fetched:
		t.Fatal("a fetch request was issued before the position hook returned")
	case <-time.After(holdWindow):
	}
	close(r.release)
}

// assertReleasedFirst checks the log: the hook returned before the first fetch and the first buffered record.
func assertReleasedFirst(t *testing.T, r *positionRecorder) {
	t.Helper()
	rel := r.first("release")
	if rel < 0 {
		t.Fatal("the position hook never returned")
	}
	for _, s := range []string{"fetch", "buffered"} {
		i := r.first(s)
		if i < 0 {
			t.Fatalf("no %s event was logged", s)
		}
		if i < rel {
			t.Fatalf("%s logged at %d, before the hook returned at %d", s, i, rel)
		}
	}
}

// awaitResolution returns the first queued hook event.
func awaitResolution(t *testing.T, r *positionRecorder) resolvedEvent {
	t.Helper()
	select {
	case ev := <-r.resolved:
		return ev
	case <-time.After(waitLimit):
		t.Fatal("the position hook was never called")
	}
	return resolvedEvent{}
}

// awaitResolutionAt returns the queued event for offset. It never waits: the call that produced it has returned.
func awaitResolutionAt(t *testing.T, r *positionRecorder, offset int64) resolvedEvent {
	t.Helper()
	for {
		select {
		case ev := <-r.resolved:
			if ev.offset == offset {
				return ev
			}
		default:
			t.Fatalf("no position resolution at offset %d", offset)
		}
	}
}

// answerEpochs replies to OffsetForLeaderEpoch with a log end below the consumed position, which the client reports
// as data loss.
func answerEpochs(c *kfake.Cluster, endOffset int64) {
	c.ControlKey(keyOffsetForLeaderEpoch, func(req kmsg.Request) (kmsg.Response, error, bool) {
		in := req.(*kmsg.OffsetForLeaderEpochRequest)
		out := req.ResponseKind().(*kmsg.OffsetForLeaderEpochResponse)
		for _, it := range in.Topics {
			rt := kmsg.OffsetForLeaderEpochResponseTopic{Topic: it.Topic}
			for _, p := range it.Partitions {
				rt.Partitions = append(rt.Partitions, kmsg.OffsetForLeaderEpochResponseTopicPartition{
					Partition:   p.Partition,
					LeaderEpoch: p.LeaderEpoch,
					EndOffset:   endOffset,
				})
			}
			out.Topics = append(out.Topics, rt)
		}
		return out, nil, true
	})
}

// TestPositionHookGatesListedFetch covers a successful ListOffsets load: the hook runs before the cursor is usable.
func TestPositionHookGatesListedFetch(t *testing.T) {
	const topic = "position-hook-list"
	cluster, seeds := startPositionCluster(t, topic)
	rec := newPositionRecorder(true)
	observeFetches(cluster, rec)
	produce(t, seeds, topic, 3)
	cl := newClient(t, kgo.SeedBrokers(seeds...), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.WithHooks(rec))
	gate(t, rec)
	expectOffsets(t, pollOffsets(t, cl, 3), 0, 1, 2)
	ev := awaitResolution(t, rec)
	if ev.topic != topic || ev.partition != 0 || ev.offset != 0 || ev.hasCause {
		t.Fatalf("resolution %+v, want offset 0 with no cause", ev)
	}
	assertReleasedFirst(t, rec)
}

// TestPositionHookGatesExactAssignment covers an exact offset assigned without epoch validation.
func TestPositionHookGatesExactAssignment(t *testing.T) {
	const topic = "position-hook-exact"
	cluster, seeds := startPositionCluster(t, topic)
	rec := newPositionRecorder(true)
	observeFetches(cluster, rec)
	produce(t, seeds, topic, 4)
	cl := newClient(t, kgo.SeedBrokers(seeds...), kgo.WithHooks(rec),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {0: kgo.NewOffset().At(2)}}))
	gate(t, rec)
	expectOffsets(t, pollOffsets(t, cl, 2), 2, 3)
	ev := awaitResolution(t, rec)
	if ev.offset != 2 || ev.leaderEpoch != -1 || ev.hasCause {
		t.Fatalf("resolution %+v, want exact offset 2 with epoch -1 and no cause", ev)
	}
	assertReleasedFirst(t, rec)
}

// TestPositionHookRepositionsInUsePartition covers SetOffsets on a partition whose cursor is already in use. The
// hook runs inside SetOffsets, so its event is queued before SetOffsets returns.
func TestPositionHookRepositionsInUsePartition(t *testing.T) {
	const topic = "position-hook-setoffsets"
	_, seeds := startPositionCluster(t, topic)
	rec := newPositionRecorder(false)
	produce(t, seeds, topic, 4)
	cl := newClient(t, kgo.SeedBrokers(seeds...), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.WithHooks(rec))
	expectOffsets(t, pollOffsets(t, cl, 4), 0, 1, 2, 3)
	cl.SetOffsets(map[string]map[int32]kgo.EpochOffset{topic: {0: {Epoch: -1, Offset: 1}}})
	ev := awaitResolutionAt(t, rec, 1)
	if ev.topic != topic || ev.partition != 0 || ev.leaderEpoch != -1 || ev.hasCause {
		t.Fatalf("resolution %+v, want SetOffsets to offset 1 with epoch -1 and no cause", ev)
	}
	expectOffsets(t, pollOffsets(t, cl, 3), 1, 2, 3)
}

// TestPositionHookCarriesDataLossCause covers a data-loss result. The notice the client polls must be the same error
// value the hook received as its cause.
func TestPositionHookCarriesDataLossCause(t *testing.T) {
	const topic = "position-hook-dataloss"
	cluster, seeds := startPositionCluster(t, topic)
	rec := newPositionRecorder(false)
	answerEpochs(cluster, 1)
	produce(t, seeds, topic, 4)
	cl := newClient(t, kgo.SeedBrokers(seeds...), kgo.WithHooks(rec),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {0: kgo.NewOffset().At(2).WithEpoch(0)}}))
	ev := awaitResolution(t, rec)
	if !ev.hasCause || ev.dataLoss == nil || ev.offset != 1 {
		t.Fatalf("resolution %+v, want a data-loss resolution at offset 1", ev)
	}
	ctx, cancel := context.WithTimeout(context.Background(), waitLimit)
	defer cancel()
	var notice *kgo.ErrDataLoss
	for notice == nil && ctx.Err() == nil {
		cl.PollFetches(ctx).EachError(func(_ string, _ int32, err error) {
			var dl *kgo.ErrDataLoss
			if errors.As(err, &dl) {
				notice = dl
			}
		})
	}
	if notice == nil {
		t.Fatal("no data-loss notice was polled")
	}
	if notice != ev.dataLoss {
		t.Fatalf("polled notice %p is not the hook cause %p", notice, ev.dataLoss)
	}
}
