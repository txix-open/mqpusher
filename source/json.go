package source

import (
	"bufio"
	"context"
	"os"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/txix-open/isp-kit/json"
	"github.com/txix-open/mqpusher/domain"
)

const (
	maxScannerBuf = 1 << 20 // 1 MB
)

type jsonDataSource struct {
	scanner   *bufio.Scanner
	closeFile func() error

	readCounter      *atomic.Uint64
	readBytesCounter *atomic.Uint64
	fileSize         float64
	isPlainTextMode  bool
}

func NewJson(filePath string, isPlainTextMode bool) (jsonDataSource, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return jsonDataSource{}, errors.WithMessagef(err, "open file '%s'", filePath)
	}
	info, err := file.Stat()
	if err != nil {
		return jsonDataSource{}, errors.WithMessage(err, "file stat")
	}

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, maxScannerBuf), maxScannerBuf)

	return jsonDataSource{
		scanner:          scanner,
		closeFile:        file.Close,
		readCounter:      new(atomic.Uint64),
		readBytesCounter: new(atomic.Uint64),
		fileSize:         float64(info.Size()),
		isPlainTextMode:  isPlainTextMode,
	}, nil
}

func (j jsonDataSource) GetData(_ context.Context) (*domain.Payload, error) {
	ok := j.scanner.Scan()
	if !ok {
		err := j.scanner.Err()
		if err != nil {
			return nil, errors.WithMessage(err, "scan")
		}
		return nil, domain.ErrNoData
	}
	bytes := j.scanner.Bytes()
	payload := &domain.Payload{
		Data: bytes,
	}

	if !j.isPlainTextMode {
		var data any
		err := json.Unmarshal(bytes, &data)
		if err != nil {
			return nil, errors.WithMessage(err, "unmarshal row")
		}
		payload.Data = data
	}

	j.readBytesCounter.Add(uint64(len(bytes)))
	j.readCounter.Add(1)

	return payload, nil
}

//nolint:mnd
func (j jsonDataSource) Progress() domain.Progress {
	readDataPercent := float64(j.readBytesCounter.Load()) / j.fileSize * 100
	return domain.Progress{
		ReadDataCount:   j.readCounter.Load(),
		ReadDataPercent: &readDataPercent,
	}
}

func (j jsonDataSource) Close(_ context.Context) error {
	err := j.closeFile()
	if err != nil {
		return errors.WithMessage(err, "close file")
	}
	return nil
}
