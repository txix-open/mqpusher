package source

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/txix-open/isp-kit/json"
	"github.com/txix-open/mqpusher/domain"
	"github.com/txix-open/mqpusher/utils"
)

type multipleJsonDataSources struct {
	fileManager      *utils.FileManager
	readCounter      *atomic.Uint64
	readBytesCounter *atomic.Uint64
	filesSize        float64
	isPlainTextMode  bool
}

func NewMultipleJson(dirPath string, isPlainTextMode bool) (multipleJsonDataSources, error) {
	dir, err := os.Open(dirPath)
	if err != nil {
		return multipleJsonDataSources{}, errors.WithMessagef(err, "open dir '%s'", dirPath)
	}
	defer func() {
		_ = dir.Close()
	}()

	files, err := dir.ReadDir(0)
	if err != nil {
		return multipleJsonDataSources{}, errors.WithMessagef(err, "read dir '%s'", dirPath)
	}

	var (
		fileNames = make([]string, 0)
		filesSize float64
	)
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		info, err := file.Info()
		if err != nil {
			return multipleJsonDataSources{}, errors.WithMessagef(err, "file '%s' info", file.Name())
		}
		fileNames = append(fileNames, filepath.Join(dirPath, file.Name()))
		filesSize += float64(info.Size())
	}

	return multipleJsonDataSources{
		fileManager:      utils.NewFileManager(fileNames),
		readCounter:      new(atomic.Uint64),
		readBytesCounter: new(atomic.Uint64),
		filesSize:        filesSize,
		isPlainTextMode:  isPlainTextMode,
	}, nil
}

func (m multipleJsonDataSources) GetData(_ context.Context) (*domain.Payload, error) {
	filePath, ok := m.fileManager.Next()
	if !ok {
		return nil, domain.ErrNoData
	}

	bytes, err := os.ReadFile(filePath)
	if err != nil {
		return nil, errors.WithMessagef(err, "read file '%s'", filePath)
	}
	payload := &domain.Payload{
		RequestId: m.requestIdFromPath(filePath),
		Data:      bytes,
	}

	if !m.isPlainTextMode {
		var data any
		err = json.Unmarshal(bytes, &data)
		if err != nil {
			return nil, errors.WithMessagef(err, "unmarshal data from file '%s'", filePath)
		}
		payload.Data = data
	}

	m.readBytesCounter.Add(uint64(len(bytes)))
	m.readCounter.Add(1)

	return payload, nil
}

//nolint:mnd
func (m multipleJsonDataSources) Progress() domain.Progress {
	readDataPercent := float64(m.readBytesCounter.Load()) / m.filesSize * 100
	return domain.Progress{
		ReadDataCount:   m.readCounter.Load(),
		ReadDataPercent: &readDataPercent,
	}
}

func (m multipleJsonDataSources) Close(_ context.Context) error { return nil }

func (m multipleJsonDataSources) requestIdFromPath(filePath string) string {
	_, fileName := filepath.Split(filePath)
	requestId, _ := strings.CutSuffix(fileName, ".json")
	return requestId
}
