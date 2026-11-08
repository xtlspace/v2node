package node

import (
	"testing"

	vCore "github.com/wyx2685/v2node/core"
)

func testSnapshot() []vCore.UserTrafficSnapshot {
	return []vCore.UserTrafficSnapshot{
		{Email: "tag-a", UID: 1, Upload: 30_000, Download: 20_000},   // 50 kB
		{Email: "tag-b", UID: 2, Upload: 120_000, Download: 80_000},  // 200 kB
		{Email: "tag-c", UID: 3, Upload: 500_000, Download: 300_000}, // 800 kB
	}
}

func TestFilterReportTraffic(t *testing.T) {
	got := filterReportTraffic(testSnapshot(), 100)
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
	if got[0].UID != 2 || got[1].UID != 3 {
		t.Fatalf("unexpected users: %+v", got)
	}

	// node_report_min_traffic uses a strict greater-than comparison.
	exact := []vCore.UserTrafficSnapshot{{Email: "tag", UID: 1, Upload: 100_000}}
	if got := filterReportTraffic(exact, 100); len(got) != 0 {
		t.Fatalf("exact threshold should not be reported: %+v", got)
	}
}

func TestToPanelTraffic(t *testing.T) {
	got := toPanelTraffic([]vCore.UserTrafficSnapshot{
		{Email: "tag", UID: 7, Upload: 10, Download: 20},
	})
	if len(got) != 1 || got[0].UID != 7 || got[0].Upload != 10 || got[0].Download != 20 {
		t.Fatalf("unexpected panel traffic: %+v", got)
	}
}
