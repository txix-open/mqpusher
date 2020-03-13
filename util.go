package main

import (
	"compress/gzip"
	"io"
	"os"
	"sync/atomic"

	"github.com/pkg/errors"
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

func makeReaders(path string) (*os.File, *gzip.Reader, *ReaderCounter, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, nil, errors.WithMessage(err, "open file")
	}

	readerCounter := NewReaderCounter(file)
	gzipReader, err := gzip.NewReader(readerCounter)
	if err != nil {
		_ = file.Close()
		return nil, nil, nil, errors.WithMessage(err, "open gzip reader")
	}

	return file, gzipReader, readerCounter, nil
}
