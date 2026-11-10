package dispatcher

import (
	"sync/atomic"
	"testing"

	"github.com/xtls/xray-core/common/buf"
)

type nopWriter struct {
	written int32
}

func (w *nopWriter) WriteMultiBuffer(mb buf.MultiBuffer) error {
	w.written = mb.Len()
	return nil
}

func TestManagedWriterCounters(t *testing.T) {
	var userValue, ipValue atomic.Int64
	w := &nopWriter{}
	mw := &ManagedWriter{
		writer:      w,
		userCounter: &userValue,
		ipCounter:   &ipValue,
	}

	payload := []byte("hello")
	if err := mw.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes(payload)}); err != nil {
		t.Fatal(err)
	}
	if got := w.written; got != int32(len(payload)) {
		t.Fatalf("written = %d, want %d", got, len(payload))
	}
	// Small payloads stay in the per-connection buffer.
	if got := userValue.Load(); got != 0 {
		t.Fatalf("user counter before flush = %d, want 0", got)
	}
	mw.Flush()
	if got := userValue.Load(); got != int64(len(payload)) {
		t.Fatalf("user counter = %d, want %d", got, len(payload))
	}
	if got := ipValue.Load(); got != int64(len(payload)) {
		t.Fatalf("ip counter = %d, want %d", got, len(payload))
	}
}

func TestTrafficWriterBatching(t *testing.T) {
	var userValue, ipValue atomic.Int64
	tw := &trafficWriter{
		user:   &userValue,
		ip:     &ipValue,
		writer: &nopWriter{},
	}

	payload := []byte("hello")
	if err := tw.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes(payload)}); err != nil {
		t.Fatal(err)
	}
	if userValue.Load() != 0 || ipValue.Load() != 0 {
		t.Fatal("small payload should stay buffered")
	}

	tw.Flush()
	if got := userValue.Load(); got != int64(len(payload)) {
		t.Fatalf("user counter = %d, want %d", got, len(payload))
	}
	if got := ipValue.Load(); got != int64(len(payload)) {
		t.Fatalf("ip counter = %d, want %d", got, len(payload))
	}
}

func TestManagedWriterFlushesPeer(t *testing.T) {
	var upValue, downValue atomic.Int64
	tw := &trafficWriter{
		user:   &downValue,
		writer: &nopWriter{},
	}
	mw := &ManagedWriter{
		writer:      &nopWriter{},
		userCounter: &upValue,
		peer:        tw,
	}

	tw.pending.Store(10)
	mw.pending.Store(5)
	mw.Flush()

	if got := upValue.Load(); got != 5 {
		t.Fatalf("up counter = %d, want 5", got)
	}
	if got := downValue.Load(); got != 10 {
		t.Fatalf("peer counter = %d, want 10", got)
	}
}
