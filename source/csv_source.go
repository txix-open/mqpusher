package source

import (
	"encoding/csv"
	"github.com/integration-system/mqpusher/conf"
	"github.com/integration-system/mqpusher/util"
	"io"
	"sync/atomic"
)

type CsvDataSource struct {
	cfg           conf.CsvSource
	readerCounter *util.ReaderCounter
	csvReader     *csv.Reader
	closeReaders  func() error
	columns       []string
	fileSize      int64
	processedRows int64
}

func (s *CsvDataSource) GetData() (interface{}, error) {
	row, err := s.csvReader.Read()
	if err == io.EOF {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	data := make(map[string]interface{}, len(s.columns))
	for i := range s.columns {
		data[s.columns[i]] = row[i]
	}

	atomic.AddInt64(&s.processedRows, 1)
	return data, nil
}

func (s *CsvDataSource) Progress() (int64, float32) {
	current := atomic.LoadInt64(&s.processedRows)
	return current, float32(s.readerCounter.Count()) / float32(s.fileSize) * 100
}

func (s *CsvDataSource) Close() error {
	return s.closeReaders()
}

func NewCsvDataSource(cfg conf.CsvSource) (DataSource, error) {
	fileInfo, reader, readerCounter, closeReaders, err := util.MakeReaders(cfg.Filename)
	if err != nil {
		return nil, err
	}
	csvReader := csv.NewReader(reader)
	csvReader.Comma = ';'

	csvReader.ReuseRecord = true
	row, err := csvReader.Read()
	if err != nil {
		return nil, err
	}
	columns := make([]string, len(row))
	copy(columns, row)

	return &CsvDataSource{
		cfg:           cfg,
		readerCounter: readerCounter,
		csvReader:     csvReader,
		closeReaders:  closeReaders,
		columns:       columns,
		fileSize:      fileInfo.Size(),
	}, nil
}
