package source

import (
	"bufio"
	"fmt"
	"sync/atomic"

	"github.com/integration-system/mqpusher/conf"
	"github.com/integration-system/mqpusher/util"
)

const (
	maxScannerBuf = 1 << 20 // 1 MB
)

type JsonDataSource struct {
	cfg           conf.JsonSource
	readerCounter *util.ReaderCounter
	scanner       *bufio.Scanner
	closeReaders  func() error
	fileSize      int64
	processedRows int64
}

func (s *JsonDataSource) GetData() (any, error) {
	ok := s.scanner.Scan()
	if !ok {
		if err := s.scanner.Err(); err != nil {
			return nil, err
		} else {
			return nil, nil
		}
	}
	b := s.scanner.Bytes()
	var data any
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
	return s.closeReaders()
}

func NewJsonDataSource(cfg conf.JsonSource) (DataSource, error) {
	fileInfo, reader, readerCounter, closeReaders, err := util.MakeReaders(cfg.Filename)
	if err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, maxScannerBuf), maxScannerBuf)

	return &JsonDataSource{
		cfg:           cfg,
		readerCounter: readerCounter,
		scanner:       scanner,
		closeReaders:  closeReaders,
		fileSize:      fileInfo.Size(),
	}, nil
}
