package kgo

import "testing"

func TestPendingLoadSnapshot(t *testing.T) {
	waiting := listOrEpochLoads{}
	waiting.addLoad("t", 0, loadTypeList, offsetLoad{replica: -1})
	loading := listOrEpochLoads{}
	loading.addLoad("t", 0, loadTypeEpoch, offsetLoad{replica: -1})
	loading.addLoad("t", 1, loadTypeList, offsetLoad{replica: -1})

	snap := pendingLoadSnapshot(waiting, loading)
	if !snap.hasLoad("t", 0) || !snap.hasLoad("t", 1) {
		t.Fatalf("snapshot must see both waiting and loading loads: %+v", snap)
	}
	if len(waiting.List["t"]) != 1 || waiting.hasLoad("t", 1) {
		t.Fatalf("waiting was changed by the snapshot: %+v", waiting)
	}
	if !loading.hasLoad("t", 1) || len(loading.List["t"]) != 1 || len(loading.Epoch["t"]) != 1 {
		t.Fatalf("loading was changed by the snapshot: %+v", loading)
	}
}
