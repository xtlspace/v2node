package rate

import (
	"testing"
	"time"

	"github.com/xtls/xray-core/common/buf"
)

type stubTimeoutReader struct {
	mb buf.MultiBuffer
}

func (r *stubTimeoutReader) ReadMultiBuffer() (buf.MultiBuffer, error) {
	return r.mb, nil
}

func (r *stubTimeoutReader) ReadMultiBufferTimeout(time.Duration) (buf.MultiBuffer, error) {
	return r.mb, nil
}

func TestRateLimitReaderPassesThrough(t *testing.T) {
	payload := []byte("hello world")
	src := &stubTimeoutReader{mb: buf.MultiBuffer{buf.FromBytes(payload)}}
	r := NewRateLimitReader(src, NewDynamicBucket(1<<30))

	var _ buf.TimeoutReader = r

	mb, err := r.ReadMultiBuffer()
	if err != nil {
		t.Fatal(err)
	}
	if got := mb.Len(); got != int32(len(payload)) {
		t.Fatalf("ReadMultiBuffer() len = %d, want %d", got, len(payload))
	}

	mbTimeout, err := r.ReadMultiBufferTimeout(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if got := mbTimeout.Len(); got != int32(len(payload)) {
		t.Fatalf("ReadMultiBufferTimeout() len = %d, want %d", got, len(payload))
	}
}
