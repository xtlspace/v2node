package node

import (
	"testing"

	panel "github.com/wyx2685/v2node/api/v2board"
)

func toUserMap(users []panel.UserInfo) map[string]panel.UserInfo {
	m := make(map[string]panel.UserInfo, len(users))
	for _, u := range users {
		m[u.Uuid] = u
	}
	return m
}

func TestMergeUserList(t *testing.T) {
	current := []panel.UserInfo{
		{Id: 1, Uuid: "a"},
		{Id: 2, Uuid: "b"},
	}
	added := []panel.UserInfo{{Id: 3, Uuid: "c"}}
	deleted := []panel.UserInfo{{Id: 2, Uuid: "b"}}
	modified := []panel.UserInfo{{Id: 1, Uuid: "a", SpeedLimit: 100}}

	got := toUserMap(mergeUserList(current, added, deleted, modified))

	if _, ok := got["b"]; ok {
		t.Fatal("deleted user b should not be present")
	}
	if got["c"].Id != 3 {
		t.Fatalf("added user c = %+v", got["c"])
	}
	if got["a"].SpeedLimit != 100 {
		t.Fatalf("modified user a = %+v", got["a"])
	}
}

func TestMergeUserListKeepsFailedChanges(t *testing.T) {
	current := []panel.UserInfo{
		{Id: 1, Uuid: "a"},
		{Id: 2, Uuid: "b"},
	}
	// deleted/added are empty because the operations failed and must be retried.
	got := toUserMap(mergeUserList(current, nil, nil, nil))

	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
	if _, ok := got["a"]; !ok {
		t.Fatal("user a should be kept")
	}
	if _, ok := got["b"]; !ok {
		t.Fatal("user b should be kept for retry")
	}
}

func TestCompareUserList(t *testing.T) {
	old := []panel.UserInfo{
		{Id: 1, Uuid: "a"},
		{Id: 2, Uuid: "b"},
		{Id: 3, Uuid: "c"},
	}
	new := []panel.UserInfo{
		{Id: 2, Uuid: "b"},
		{Id: 4, Uuid: "d"},
	}
	deleted, added, modified := compareUserList(old, new)

	deletedMap := toUserMap(deleted)
	if len(deletedMap) != 2 {
		t.Fatalf("deleted = %+v", deleted)
	}
	if _, ok := deletedMap["a"]; !ok {
		t.Fatalf("deleted missing a: %+v", deleted)
	}
	if _, ok := deletedMap["c"]; !ok {
		t.Fatalf("deleted missing c: %+v", deleted)
	}
	if len(added) != 1 || added[0].Uuid != "d" {
		t.Fatalf("added = %+v", added)
	}
	if len(modified) != 0 {
		t.Fatalf("modified = %+v", modified)
	}
}
