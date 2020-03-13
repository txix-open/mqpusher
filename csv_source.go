package main

import (
	"compress/gzip"
	"encoding/csv"
	"io"
	"os"
	"sync/atomic"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
)

type CsvDataSource struct {
	cfg           CsvSource
	file          *os.File
	gzipReader    *gzip.Reader
	csvReader     *csv.Reader
	columns       []string
	readerCounter *ReaderCounter
	fileSize      int64
	processedRows int64
}

func (s *CsvDataSource) GetRow() (map[string]interface{}, error) {
	row, err := s.csvReader.Read()
	if err == io.EOF {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	result := make(map[string]interface{}, len(s.columns))
	for i := range s.columns {
		result[s.columns[i]] = row[i]
	}

	atomic.AddInt64(&s.processedRows, 1)
	return result, nil
}

func (s *CsvDataSource) Progress() (int64, float32) {
	current := atomic.LoadInt64(&s.processedRows)
	return current, float32(s.readerCounter.Count()) / float32(s.fileSize) * 100
}

func (s *CsvDataSource) Close() error {
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

func NewCsvDataSource(cfg CsvSource) (DataSource, error) {
	file, gzipReader, readerCounter, err := makeReaders(cfg.Filename)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	csvReader := csv.NewReader(gzipReader)
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
		columns:       columns,
		file:          file,
		fileSize:      info.Size(),
		readerCounter: readerCounter,
		gzipReader:    gzipReader,
		csvReader:     csvReader,
	}, nil
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
