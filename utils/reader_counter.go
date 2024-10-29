package utils

import (
	"io"
	"sync/atomic"

	"github.com/pkg/errors"
)

type baseReader interface {
	io.ReaderAt
	io.ReadCloser
}

type ReaderCounter struct {
	baseReader
	count *atomic.Uint64
}

func NewReaderCounter(r baseReader) ReaderCounter {
	return ReaderCounter{
		baseReader: r,
		count:      new(atomic.Uint64),
	}
}

func (r ReaderCounter) Read(buf []byte) (int, error) {
	n, err := r.baseReader.Read(buf)
	if err != nil {
		return 0, errors.WithMessage(err, "base reader read")
	}
	r.count.Add(uint64(n)) // nolint:gosec
	return n, nil
}

func (r ReaderCounter) Count() uint64 {
	return r.count.Load()
}
