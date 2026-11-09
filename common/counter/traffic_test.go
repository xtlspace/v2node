package counter

import "testing"

func TestTrafficStorageStats(t *testing.T) {
	tc := NewTrafficCounter()
	s := tc.GetCounter("user")

	s.UpStats.Add(10)
	s.DownStats.Add(20)

	if got := s.UpCounter.Load(); got != 10 {
		t.Fatalf("UpCounter = %d, want 10", got)
	}
	if got := s.DownCounter.Load(); got != 20 {
		t.Fatalf("DownCounter = %d, want 20", got)
	}
	if got := tc.GetUpCount("user"); got != 10 {
		t.Fatalf("GetUpCount = %d, want 10", got)
	}
	if got := tc.GetDownCount("user"); got != 20 {
		t.Fatalf("GetDownCount = %d, want 20", got)
	}
}
