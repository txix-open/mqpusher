package main

import (
	"io"
	"sync/atomic"
)

type ReaderCounter struct {
	io.Reader
	count int64
}

func (rc *ReaderCounter) Read(buf []byte) (int, error) {
	n, err := rc.Reader.Read(buf)
	atomic.AddInt64(&rc.count, int64(n))
	return n, err
}

func (rc *ReaderCounter) Count() int64 {
	return atomic.LoadInt64(&rc.count)
}

func NewReaderCounter(r io.Reader) *ReaderCounter {
	return &ReaderCounter{
		Reader: r,
	}
}
