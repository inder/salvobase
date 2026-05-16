package wire

import "sync"

// bucketBufPool is a tiered buffer pool. Get(n) rounds n up to the smallest
// bucket that fits and serves it from that bucket's sync.Pool. Requests larger
// than the biggest bucket fall through to a one-shot allocation that is not
// returned to any pool.
//
// Buffers are stored as *[]byte so the pool sees a fixed-size pointer and the
// underlying array does not escape. The exact *[]byte that Get returns must be
// threaded back through Put so the pool reuses the slice header instead of
// allocating a fresh one on every release.
type bucketBufPool struct {
	buckets []int
	pools   []sync.Pool
}

// newBucketBufPool builds a pool with the given size tiers. The tiers should be
// listed in ascending order; Get walks them in array order and stops at the
// first fit.
func newBucketBufPool(buckets []int) *bucketBufPool {
	bp := &bucketBufPool{
		buckets: buckets,
		pools:   make([]sync.Pool, len(buckets)),
	}
	for i, size := range buckets {
		size := size
		bp.pools[i].New = func() any {
			b := make([]byte, size)
			return &b
		}
	}
	return bp
}

// Get returns a buffer of at least n bytes plus a handle that the caller must
// hand back to Put. The returned slice has len == cap == bucket size; callers
// slice down to n before writing it out.
//
// If n exceeds the largest bucket, Get allocates a fresh slice and returns a
// nil handle — Put is a no-op in that case and the GC reclaims the buffer.
func (bp *bucketBufPool) Get(n int) ([]byte, *[]byte) {
	for i, size := range bp.buckets {
		if n <= size {
			h, ok := bp.pools[i].Get().(*[]byte)
			if !ok || h == nil {
				b := make([]byte, size)
				h = &b
			}
			return *h, h
		}
	}
	return make([]byte, n), nil
}

// Put returns the buffer to its pool. handle must be the *[]byte returned by
// Get; a nil handle means the buffer was an over-bucket allocation and should
// not be pooled.
func (bp *bucketBufPool) Put(handle *[]byte) {
	if handle == nil {
		return
	}
	c := cap(*handle)
	for i, size := range bp.buckets {
		if c == size {
			bp.pools[i].Put(handle)
			return
		}
	}
}
