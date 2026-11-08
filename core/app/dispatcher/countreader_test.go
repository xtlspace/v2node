package dispatcher

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/wyx2685/v2node/common/counter"
	"github.com/xtls/xray-core/common/buf"
)

type counterTestReader struct {
	mb      buf.MultiBuffer
	err     error
	timeout time.Duration
}

func (r *counterTestReader) ReadMultiBuffer() (buf.MultiBuffer, error) {
	return r.mb, r.err
}

func (r *counterTestReader) ReadMultiBufferTimeout(timeout time.Duration) (buf.MultiBuffer, error) {
	r.timeout = timeout
	return r.mb, r.err
}

func TestCounterReaderForwardsTimeout(t *testing.T) {
	payload := []byte("hello")
	src := &counterTestReader{mb: buf.MultiBuffer{buf.FromBytes(payload)}}
	var counter atomic.Int64
	var ipCounter atomic.Int64
	r := &CounterReader{
		Reader:    src,
		Counter:   &counter,
		IPCounter: &ipCounter,
	}

	timeout := 123 * time.Millisecond
	mb, err := r.ReadMultiBufferTimeout(timeout)
	if err != nil {
		t.Fatal(err)
	}
	if src.timeout != timeout {
		t.Fatalf("forwarded timeout = %v, want %v", src.timeout, timeout)
	}
	if got := mb.Len(); got != int32(len(payload)) {
		t.Fatalf("mb.Len() = %d, want %d", got, len(payload))
	}
	if got := counter.Load(); got != int64(len(payload)) {
		t.Fatalf("counter = %d, want %d", got, len(payload))
	}
	if got := ipCounter.Load(); got != int64(len(payload)) {
		t.Fatalf("ip counter = %d, want %d", got, len(payload))
	}
}

func TestDualCounterAdd(t *testing.T) {
	var userValue, ipValue atomic.Int64
	c := &dualCounter{
		user: &counter.XrayTrafficCounter{V: &userValue},
		ip:   &ipValue,
	}
	c.Add(10)
	c.Add(5)
	if got := userValue.Load(); got != 15 {
		t.Fatalf("user counter = %d, want 15", got)
	}
	if got := ipValue.Load(); got != 15 {
		t.Fatalf("ip counter = %d, want 15", got)
	}
}

func TestCounterReaderTimeoutDoesNotCount(t *testing.T) {
	src := &counterTestReader{err: buf.ErrReadTimeout}
	var counter atomic.Int64
	r := &CounterReader{
		Reader:  src,
		Counter: &counter,
	}

	if _, err := r.ReadMultiBufferTimeout(10 * time.Millisecond); err == nil {
		t.Fatal("expected timeout error")
	}
	if got := counter.Load(); got != 0 {
		t.Fatalf("counter = %d, want 0", got)
	}
}

func TestCounterReaderReadMultiBufferCounts(t *testing.T) {
	payload := []byte("world")
	src := &counterTestReader{mb: buf.MultiBuffer{buf.FromBytes(payload)}}
	var counter atomic.Int64
	r := &CounterReader{
		Reader:  src,
		Counter: &counter,
	}

	if _, err := r.ReadMultiBuffer(); err != nil {
		t.Fatal(err)
	}
	if got := counter.Load(); got != int64(len(payload)) {
		t.Fatalf("counter = %d, want %d", got, len(payload))
	}
}
