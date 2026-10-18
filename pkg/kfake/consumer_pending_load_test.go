package kfake

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// pendingLoadCluster starts a one-broker kfake cluster with topic seeded.
func pendingLoadCluster(t *testing.T, topic string) *Cluster {
	t.Helper()
	c, err := NewCluster(NumBrokers(1), SeedTopics(1, topic))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

// pendingLoadClient starts a plain kgo client against c.
func pendingLoadClient(t *testing.T, c *Cluster, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	opts = append([]kgo.Opt{kgo.SeedBrokers(c.ListenAddrs()...)}, opts...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return cl
}

// pendingLoadProduce produces n string records "v0".."vN-1" to topic.
func pendingLoadProduce(t *testing.T, c *Cluster, topic string, n int) {
	t.Helper()
	cl := pendingLoadClient(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	for i := range n {
		r := &kgo.Record{Topic: topic, Value: fmt.Appendf(nil, "v%d", i)}
		if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
}

// pendingLoadCommit commits offset with leader epoch epoch for the group,
// using a separate admin client so no consumer load is racing it.
func pendingLoadCommit(t *testing.T, c *Cluster, group, topic string, offset int64, epoch int32) {
	t.Helper()
	adm := kadm.NewClient(pendingLoadClient(t, c))
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	os := kadm.Offsets{topic: {0: {Topic: topic, Partition: 0, At: offset, LeaderEpoch: epoch}}}
	resp, err := adm.CommitOffsets(ctx, group, os)
	if err != nil {
		t.Fatalf("commit offset %d epoch %d: %v", offset, epoch, err)
	}
	if err := resp.Error(); err != nil {
		t.Fatalf("commit offset %d epoch %d: %v", offset, epoch, err)
	}
}

// pendingLoadPoll polls exactly n records or fails at the deadline. It returns
// the offsets in arrival order.
func pendingLoadPoll(t *testing.T, cl *kgo.Client, n int, within time.Duration) []int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), within)
	defer cancel()
	var offsets []int64
	for ctx.Err() == nil && len(offsets) < n {
		fs := cl.PollFetches(ctx)
		fs.EachError(func(_ string, _ int32, err error) {
			if ctx.Err() == nil {
				t.Errorf("poll error: %v", err)
			}
		})
		fs.EachRecord(func(r *kgo.Record) {
			offsets = append(offsets, r.Offset)
		})
	}
	return offsets
}

// pendingLoadHoldOFLE holds the first OffsetForLeaderEpoch request for topic
// until release is called. entered closes once the request is parked. release
// is idempotent and runs on every exit path. The hold never touches Fetch.
func pendingLoadHoldOFLE(t *testing.T, c *Cluster) (entered <-chan struct{}, release func()) {
	t.Helper()
	enteredCh := make(chan struct{})
	releaseCh := make(chan struct{})
	var once sync.Once
	releaseOnce := func() { once.Do(func() { close(releaseCh) }) }
	var hits atomic.Int64
	c.ControlKey(int16(kmsg.OffsetForLeaderEpoch), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		if hits.Add(1) != 1 {
			return nil, nil, false // only the first load is held
		}
		c.KeepControl()
		req := kreq.(*kmsg.OffsetForLeaderEpochRequest)
		resp := req.ResponseKind().(*kmsg.OffsetForLeaderEpochResponse)
		close(enteredCh)
		c.SleepControl(func() { <-releaseCh })
		for _, rt := range req.Topics {
			st := kmsg.NewOffsetForLeaderEpochResponseTopic()
			st.Topic = rt.Topic
			for _, rp := range rt.Partitions {
				sp := kmsg.NewOffsetForLeaderEpochResponseTopicPartition()
				sp.Partition = rp.Partition
				pi := c.PartitionInfo(rt.Topic, rp.Partition)
				sp.LeaderEpoch = pi.Epoch
				sp.EndOffset = pi.HighWatermark
				st.Partitions = append(st.Partitions, sp)
			}
			resp.Topics = append(resp.Topics, st)
		}
		return resp, nil, true
	})
	t.Cleanup(releaseOnce)
	return enteredCh, releaseOnce
}

// pendingLoadHoldOFLETruncated holds the first OffsetForLeaderEpoch for
// partition 0 and answers it with a fixed truncation point (leader epoch 0, end
// offset endOffset), so a stale result would move the cursor to endOffset.
// Later requests are served by kfake normally. release is idempotent.
func pendingLoadHoldOFLETruncated(t *testing.T, c *Cluster, endOffset int64) (entered <-chan struct{}, release func()) {
	t.Helper()
	enteredCh := make(chan struct{})
	releaseCh := make(chan struct{})
	var once sync.Once
	releaseOnce := func() { once.Do(func() { close(releaseCh) }) }
	var hits atomic.Int64
	c.ControlKey(int16(kmsg.OffsetForLeaderEpoch), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		if hits.Add(1) != 1 {
			return nil, nil, false
		}
		c.KeepControl()
		req := kreq.(*kmsg.OffsetForLeaderEpochRequest)
		resp := req.ResponseKind().(*kmsg.OffsetForLeaderEpochResponse)
		close(enteredCh)
		c.SleepControl(func() { <-releaseCh })
		for _, rt := range req.Topics {
			st := kmsg.NewOffsetForLeaderEpochResponseTopic()
			st.Topic = rt.Topic
			for _, rp := range rt.Partitions {
				sp := kmsg.NewOffsetForLeaderEpochResponseTopicPartition()
				sp.Partition = rp.Partition
				sp.LeaderEpoch = 0
				sp.EndOffset = endOffset
				st.Partitions = append(st.Partitions, sp)
			}
			resp.Topics = append(resp.Topics, st)
		}
		return resp, nil, true
	})
	t.Cleanup(releaseOnce)
	return enteredCh, releaseOnce
}

// pendingLoadFenceFetch fails exactly one Fetch for partition 0 of topic with
// FENCED_LEADER_EPOCH. The client reacts by marking the cursor unusable and
// queueing an epoch validation. Other fetches are served normally.
func pendingLoadFenceFetch(c *Cluster, topic string) *atomic.Int64 {
	pi := c.PartitionInfo(topic, 0)
	var hits atomic.Int64
	c.ControlKey(int16(kmsg.Fetch), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		if hits.Add(1) != 1 {
			return nil, nil, false
		}
		req := kreq.(*kmsg.FetchRequest)
		resp := req.ResponseKind().(*kmsg.FetchResponse)
		rt := kmsg.NewFetchResponseTopic()
		rt.Topic = topic
		rp := kmsg.NewFetchResponseTopicPartition()
		rp.Partition = 0
		rp.ErrorCode = kerr.FencedLeaderEpoch.Code
		rp.HighWatermark = pi.HighWatermark
		rp.LastStableOffset = pi.LastStableOffset
		rp.LogStartOffset = 0
		rt.Partitions = append(rt.Partitions, rp)
		resp.Topics = append(resp.Topics, rt)
		return resp, nil, true
	})
	return &hits
}

// TestSetOffsetsReplacesPendingInitialEpochLoad covers a partition whose
// initial load never finished. SetOffsets must replace that pending load, so
// the consumer starts at the requested offset and not at the committed one.
func TestSetOffsetsReplacesPendingInitialEpochLoad(t *testing.T) {
	t.Parallel()

	const (
		topic = "pending-initial-epoch"
		group = "pending-initial-epoch-group"
		msgs  = 3
	)

	c := pendingLoadCluster(t, topic)
	pendingLoadProduce(t, c, topic, msgs)
	pendingLoadCommit(t, c, group, topic, msgs, 0)

	entered, release := pendingLoadHoldOFLE(t, c)
	cl := pendingLoadClient(t, c,
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableFetchSessions(),
		kgo.FetchMaxWait(100*time.Millisecond),
	)
	select {
	case <-entered:
	case <-time.After(8 * time.Second):
		t.Fatal("timed out waiting for the initial epoch load")
	}

	cl.SetOffsets(map[string]map[int32]kgo.EpochOffset{
		topic: {0: {Epoch: -1, Offset: 0}},
	})
	release()

	got := pendingLoadPoll(t, cl, msgs, 8*time.Second)
	want := []int64{0, 1, 2}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got offsets %v, want %v", got, want)
	}
}

// TestPendingLoadControlCommittedOffsetStartsWithoutHold is a control: with
// no held load and no seek, a group consumer starts at the committed offset.
func TestPendingLoadControlCommittedOffsetStartsWithoutHold(t *testing.T) {
	t.Parallel()

	const (
		topic = "pending-control-committed"
		group = "pending-control-committed-group"
		msgs  = 3
	)

	c := pendingLoadCluster(t, topic)
	pendingLoadProduce(t, c, topic, msgs)
	pendingLoadCommit(t, c, group, topic, msgs, 0)

	cl := pendingLoadClient(t, c,
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableFetchSessions(),
		kgo.FetchMaxWait(100*time.Millisecond),
	)
	got := pendingLoadPoll(t, cl, 0, 2*time.Second)
	t.Logf("committed offset %d start: polled %v", msgs, got)
}

// TestSetOffsetsReplacesPendingInitialLoadBelowCommit covers a seek below the
// committed offset while the initial epoch load is pending. The commit is 2
// and the seek is 0, so this is a change A case (the pending load is replaced),
// not an equal-dirty case.
func TestSetOffsetsReplacesPendingInitialLoadBelowCommit(t *testing.T) {
	t.Parallel()

	const (
		topic = "pending-equal-dirty"
		group = "pending-equal-dirty-group"
		msgs  = 4
	)

	c := pendingLoadCluster(t, topic)
	pendingLoadProduce(t, c, topic, msgs)
	pendingLoadCommit(t, c, group, topic, 2, 0)

	entered, release := pendingLoadHoldOFLE(t, c)
	cl := pendingLoadClient(t, c,
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableFetchSessions(),
		kgo.FetchMaxWait(100*time.Millisecond),
	)
	select {
	case <-entered:
	case <-time.After(8 * time.Second):
		t.Fatal("timed out waiting for the initial epoch load")
	}
	cl.SetOffsets(map[string]map[int32]kgo.EpochOffset{
		topic: {0: {Epoch: -1, Offset: 0}},
	})
	release()

	got := pendingLoadPoll(t, cl, msgs, 8*time.Second)
	want := []int64{0, 1, 2, 3}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got offsets %v, want %v", got, want)
	}
}

// TestSetOffsetsLeavesUnselectedPendingLoadAlone is a control: a pending load
// for a partition missing from SetOffsets is left untouched.
func TestSetOffsetsLeavesUnselectedPendingLoadAlone(t *testing.T) {
	t.Parallel()

	const (
		topic = "pending-unselected"
		group = "pending-unselected-group"
		msgs  = 3
	)

	c := pendingLoadCluster(t, topic)
	pendingLoadProduce(t, c, topic, msgs)
	pendingLoadCommit(t, c, group, topic, msgs, 0)

	entered, release := pendingLoadHoldOFLE(t, c)
	cl := pendingLoadClient(t, c,
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableFetchSessions(),
		kgo.FetchMaxWait(100*time.Millisecond),
	)
	select {
	case <-entered:
	case <-time.After(8 * time.Second):
		t.Fatal("timed out waiting for the initial epoch load")
	}
	// Partition 1 does not exist on this topic, so the selection is empty:
	// the held load for partition 0 must not be replaced.
	cl.SetOffsets(map[string]map[int32]kgo.EpochOffset{
		topic: {1: {Epoch: -1, Offset: 0}},
	})
	release()

	got := pendingLoadPoll(t, cl, 0, 2*time.Second)
	if len(got) != 0 {
		t.Fatalf("unselected load changed the start: got %v", got)
	}
}

// TestSetOffsetsEqualDirtyUninstalledCursorReassigns checks that a repeated
// identical seek while the replaced load is pending still reads from 0. It does
// not isolate the equal-dirty path: the first seek already replaces the load
// (change A), and the second is the repeat. TestSetOffsetsEqualDirtyWinsOverPendingLoad
// is the isolating case for the equal-dirty path.
func TestSetOffsetsEqualDirtyUninstalledCursorReassigns(t *testing.T) {
	t.Parallel()

	const (
		topic = "pending-equal-uninstalled"
		group = "pending-equal-uninstalled-group"
		msgs  = 4
	)

	c := pendingLoadCluster(t, topic)
	pendingLoadProduce(t, c, topic, msgs)
	pendingLoadCommit(t, c, group, topic, 2, 0)

	entered, release := pendingLoadHoldOFLE(t, c)
	cl := pendingLoadClient(t, c,
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableFetchSessions(),
		kgo.FetchMaxWait(100*time.Millisecond),
	)
	select {
	case <-entered:
	case <-time.After(8 * time.Second):
		t.Fatal("timed out waiting for the initial epoch load")
	}
	req := map[string]map[int32]kgo.EpochOffset{
		topic: {0: {Epoch: -1, Offset: 0}},
	}
	cl.SetOffsets(req)
	cl.SetOffsets(req)
	release()

	got := pendingLoadPoll(t, cl, msgs, 8*time.Second)
	want := []int64{0, 1, 2, 3}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got offsets %v, want %v", got, want)
	}
}

// TestSetOffsetsEqualDirtyWinsOverPendingLoad isolates the equal-dirty path.
// The group commit is (offset 2, epoch 0), and the initial epoch load is held
// with a truncation answer of end offset 1. SetOffsets to the bookkept (0, 2)
// matches the group's dirty offset, so the group skip applies. The held load
// must not install its stale truncation: the consumer reads exactly 2 and 3.
func TestSetOffsetsEqualDirtyWinsOverPendingLoad(t *testing.T) {
	t.Parallel()

	const (
		topic = "pending-equal-dirty-truncated"
		group = "pending-equal-dirty-truncated-group"
		msgs  = 4
	)

	c := pendingLoadCluster(t, topic)
	pendingLoadProduce(t, c, topic, msgs)
	pendingLoadCommit(t, c, group, topic, 2, 0)

	entered, release := pendingLoadHoldOFLETruncated(t, c, 1)
	cl := pendingLoadClient(t, c,
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableFetchSessions(),
		kgo.FetchMaxWait(100*time.Millisecond),
	)
	select {
	case <-entered:
	case <-time.After(8 * time.Second):
		t.Fatal("timed out waiting for the initial epoch load")
	}
	cl.SetOffsets(map[string]map[int32]kgo.EpochOffset{
		topic: {0: {Epoch: 0, Offset: 2}},
	})
	release()

	got := pendingLoadPoll(t, cl, 2, 8*time.Second)
	want := []int64{2, 3}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got offsets %v, want %v", got, want)
	}
}
