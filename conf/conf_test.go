package conf

import (
	"os"
	"path/filepath"
	"testing"
)

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestPolicyDefaults(t *testing.T) {
	c := New()
	if err := c.LoadFromPath(writeConfig(t, `{"Log":{"Level":"info"}}`)); err != nil {
		t.Fatal(err)
	}
	want := PolicyConfig{
		Handshake:    DefaultHandshake,
		ConnIdle:     DefaultConnIdle,
		UplinkOnly:   DefaultUplinkOnly,
		DownlinkOnly: DefaultDownlinkOnly,
		BufferSize:   DefaultBufferSize,
	}
	if c.Policy != want {
		t.Fatalf("Policy = %+v, want %+v", c.Policy, want)
	}
}

func TestPolicyPartialOverride(t *testing.T) {
	c := New()
	if err := c.LoadFromPath(writeConfig(t, `{"Policy":{"BufferSize":512}}`)); err != nil {
		t.Fatal(err)
	}
	if c.Policy.BufferSize != 512 {
		t.Fatalf("BufferSize = %d, want 512", c.Policy.BufferSize)
	}
	if c.Policy.Handshake != DefaultHandshake ||
		c.Policy.ConnIdle != DefaultConnIdle ||
		c.Policy.UplinkOnly != DefaultUplinkOnly ||
		c.Policy.DownlinkOnly != DefaultDownlinkOnly {
		t.Fatalf("other policy fields lost defaults: %+v", c.Policy)
	}
}

func TestPolicyFullOverride(t *testing.T) {
	c := New()
	if err := c.LoadFromPath(writeConfig(t, `{"Policy":{"Handshake":10,"ConnIdle":300,"UplinkOnly":5,"DownlinkOnly":6,"BufferSize":-1}}`)); err != nil {
		t.Fatal(err)
	}
	want := PolicyConfig{
		Handshake:    10,
		ConnIdle:     300,
		UplinkOnly:   5,
		DownlinkOnly: 6,
		BufferSize:   -1,
	}
	if c.Policy != want {
		t.Fatalf("Policy = %+v, want %+v", c.Policy, want)
	}
}
