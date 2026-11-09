package dispatcher

import (
	"sync/atomic"
	"testing"

	"github.com/wyx2685/v2node/common/counter"
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
		userCounter: &counter.XrayTrafficCounter{V: &userValue},
		ipCounter:   &ipValue,
	}

	payload := []byte("hello")
	if err := mw.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes(payload)}); err != nil {
		t.Fatal(err)
	}
	if got := userValue.Load(); got != int64(len(payload)) {
		t.Fatalf("user counter = %d, want %d", got, len(payload))
	}
	if got := ipValue.Load(); got != int64(len(payload)) {
		t.Fatalf("ip counter = %d, want %d", got, len(payload))
	}
	if got := w.written; got != int32(len(payload)) {
		t.Fatalf("written = %d, want %d", got, len(payload))
	}
}
