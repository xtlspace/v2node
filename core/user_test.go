package core

import (
	"testing"

	"github.com/wyx2685/v2node/common/counter"
	"github.com/wyx2685/v2node/core/app/dispatcher"
)

func TestGetUserTrafficSnapshot(t *testing.T) {
	vc := &V2Core{
		users: &UserMap{uidMap: map[string]int{
			"tag-uuid1": 1,
			"tag-uuid2": 2,
		}},
		dispatcher: &dispatcher.DefaultDispatcher{},
	}
	tc := counter.NewTrafficCounter()
	s1 := tc.GetCounter("tag-uuid1")
	s1.UpCounter.Store(60_000)
	s1.DownCounter.Store(40_000)
	s2 := tc.GetCounter("tag-uuid2")
	s2.UpCounter.Store(10_000)
	s2.DownCounter.Store(5_000)
	orphan := tc.GetCounter("tag-orphan")
	orphan.UpCounter.Store(1)
	vc.dispatcher.Counter.Store("tag", tc)

	snapshot := vc.GetUserTrafficSnapshot("tag")
	if len(snapshot) != 2 {
		t.Fatalf("len(snapshot) = %d, want 2", len(snapshot))
	}

	byEmail := make(map[string]UserTrafficSnapshot, len(snapshot))
	for _, s := range snapshot {
		byEmail[s.Email] = s
	}
	if s := byEmail["tag-uuid1"]; s.UID != 1 || s.Upload != 60_000 || s.Download != 40_000 {
		t.Fatalf("unexpected snapshot for uuid1: %+v", s)
	}
	if _, ok := byEmail["tag-orphan"]; ok {
		t.Fatal("stale counter must not be included")
	}
	if _, ok := tc.Counters.Load("tag-orphan"); ok {
		t.Fatal("stale counter should be deleted")
	}
}

func TestCommitUserTraffic(t *testing.T) {
	vc := &V2Core{
		users: &UserMap{uidMap: map[string]int{
			"tag-uuid1": 1,
			"tag-uuid2": 2,
		}},
		dispatcher: &dispatcher.DefaultDispatcher{},
	}
	tc := counter.NewTrafficCounter()
	s1 := tc.GetCounter("tag-uuid1")
	s1.UpCounter.Store(100)
	s1.DownCounter.Store(200)
	s2 := tc.GetCounter("tag-uuid2")
	s2.UpCounter.Store(300)
	s2.DownCounter.Store(400)
	vc.dispatcher.Counter.Store("tag", tc)

	vc.CommitUserTraffic("tag", []UserTrafficSnapshot{
		{Email: "tag-uuid1", Upload: 100, Download: 200},
	})

	if got := s1.UpCounter.Load(); got != 0 {
		t.Fatalf("uid1 upload = %d, want 0", got)
	}
	if got := s1.DownCounter.Load(); got != 0 {
		t.Fatalf("uid1 download = %d, want 0", got)
	}
	if got := s2.UpCounter.Load(); got != 300 {
		t.Fatalf("uid2 upload = %d, want 300", got)
	}
	if got := s2.DownCounter.Load(); got != 400 {
		t.Fatalf("uid2 download = %d, want 400", got)
	}
}

func TestCommitUserTrafficClampsToZero(t *testing.T) {
	vc := &V2Core{
		users:      &UserMap{uidMap: map[string]int{"tag-uuid": 1}},
		dispatcher: &dispatcher.DefaultDispatcher{},
	}
	tc := counter.NewTrafficCounter()
	s := tc.GetCounter("tag-uuid")
	s.UpCounter.Store(10)
	s.DownCounter.Store(20)
	vc.dispatcher.Counter.Store("tag", tc)

	vc.CommitUserTraffic("tag", []UserTrafficSnapshot{
		{Email: "tag-uuid", Upload: 1000, Download: 2000},
	})

	if got := s.UpCounter.Load(); got != 0 {
		t.Fatalf("upload = %d, want 0", got)
	}
	if got := s.DownCounter.Load(); got != 0 {
		t.Fatalf("download = %d, want 0", got)
	}
}
