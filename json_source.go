package main

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"os"
	"sync/atomic"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
)

const (
	maxScannerBuf = 1 << 20 // 1 MB
)

type JsonDataSource struct {
	cfg           JsonSource
	file          *os.File
	gzipReader    *gzip.Reader
	readerCounter *ReaderCounter
	scanner       *bufio.Scanner
	fileSize      int64
	processedRows int64
}

func (s *JsonDataSource) GetData() (interface{}, error) {
	ok := s.scanner.Scan()
	if !ok {
		if err := s.scanner.Err(); err != nil {
			return nil, err
		} else {
			return nil, nil
		}
	}
	b := s.scanner.Bytes()
	var data interface{}
	err := json.Unmarshal(b, &data)
	if err != nil {
		return nil, fmt.Errorf("unmarshaling row: %v", err)
	}

	atomic.AddInt64(&s.processedRows, 1)
	return data, nil
}

func (s *JsonDataSource) Progress() (int64, float32) {
	current := atomic.LoadInt64(&s.processedRows)
	return current, float32(s.readerCounter.Count()) / float32(s.fileSize) * 100
}

func (s *JsonDataSource) Close() error {
	var err error
	if s.gzipReader != nil {
		err2 := s.gzipReader.Close()
		if err2 != nil {
			err = multierr.Append(err, errors.WithMessage(err2, "close gzip reader"))
		}
	}
	if s.file != nil {
		err2 := s.file.Close()
		if err2 != nil {
			err = multierr.Append(err, errors.WithMessage(err2, "close file"))
		}
	}

	return err
}

func NewJsonDataSource(cfg JsonSource) (DataSource, error) {
	file, gzipReader, readerCounter, err := makeReaders(cfg.Filename)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(gzipReader)
	scanner.Buffer(make([]byte, maxScannerBuf), maxScannerBuf)

	return &JsonDataSource{
		cfg:           cfg,
		file:          file,
		fileSize:      info.Size(),
		readerCounter: readerCounter,
		gzipReader:    gzipReader,
		scanner:       scanner,
	}, nil
}
