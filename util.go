package main

import (
	"archive/zip"
	"compress/gzip"
	"fmt"
	"io"
	"mime"
	"os"
	"path/filepath"
	"sync/atomic"

	"go.uber.org/multierr"
)

type BaseReader interface {
	io.ReaderAt
	io.ReadCloser
}

type ReaderCounter struct {
	BaseReader
	count int64
}

func (rc *ReaderCounter) Read(buf []byte) (int, error) {
	n, err := rc.BaseReader.Read(buf)
	atomic.AddInt64(&rc.count, int64(n))
	return n, err
}

func (rc *ReaderCounter) Count() int64 {
	return atomic.LoadInt64(&rc.count)
}

func NewReaderCounter(r BaseReader) *ReaderCounter {
	return &ReaderCounter{
		BaseReader: r,
	}
}

func makeReaders(path string) (os.FileInfo, io.Reader, *ReaderCounter, func() error, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("open file: %v", err)
	}
	info, err := file.Stat()
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("stat file: %v", err)
	}

	readerCounter := NewReaderCounter(file)
	var compressedReader io.ReadCloser
	var closeReaders func() error

	mimeType := mime.TypeByExtension(filepath.Ext(path))
	switch mimeType {
	case "application/zip":
		reader, err := zip.NewReader(readerCounter, info.Size())
		if err != nil {
			_ = file.Close()
			return nil, nil, nil, nil, fmt.Errorf("open zip reader: %v", err)
		}
		if len(reader.File) != 1 {
			_ = file.Close()
			return nil, nil, nil, nil, fmt.Errorf("open zip: support only 1 file per archive, got %d", len(reader.File))
		}
		zipFileReader, err := reader.File[0].Open()
		if err != nil {
			_ = file.Close()
			return nil, nil, nil, nil, fmt.Errorf("open zip reader: %v", err)
		}
		compressedReader = zipFileReader
	case "application/gzip":
		gzipReader, err := gzip.NewReader(readerCounter)
		if err != nil {
			_ = file.Close()
			return nil, nil, nil, nil, fmt.Errorf("open gzip reader: %v", err)
		}
		compressedReader = gzipReader
	}

	closeReaders = func() error {
		var err error
		if compressedReader != nil {
			err2 := compressedReader.Close()
			if err2 != nil {
				err = multierr.Append(err, fmt.Errorf("close compressed reader: %v", err2))
			}
		}
		if file != nil {
			err2 := file.Close()
			if err2 != nil {
				err = multierr.Append(err, fmt.Errorf("close file: %v", err2))
			}
		}

		return err
	}

	var reader io.ReadCloser = readerCounter
	if compressedReader != nil {
		reader = compressedReader
	}

	return info, reader, readerCounter, closeReaders, nil
}
