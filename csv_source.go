package main

import (
	"compress/gzip"
	"encoding/csv"
	"io"
	"os"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
)

type DataSource interface {
	GetRow() (map[string]interface{}, error)
	Close() error
}

type CsvDataSource struct {
	cfg        CsvSource
	file       *os.File
	gzipReader *gzip.Reader
	csvReader  *csv.Reader
	columns    []string
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

	return result, nil
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
	file, gzipReader, csvReader, err := makeReaders(cfg.Filename, ';')
	if err != nil {
		return nil, err
	}

	csvReader.ReuseRecord = true
	row, err := csvReader.Read()
	if err != nil {
		return nil, err
	}
	columns := make([]string, len(row))
	copy(columns, row)

	return &CsvDataSource{
		cfg:        cfg,
		columns:    columns,
		file:       file,
		gzipReader: gzipReader,
		csvReader:  csvReader,
	}, nil
}

func makeReaders(path string, csvSep rune) (*os.File, *gzip.Reader, *csv.Reader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, nil, errors.WithMessage(err, "open file")
	}

	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		_ = file.Close()
		return nil, nil, nil, errors.WithMessage(err, "open gzip reader")
	}

	csvReader := csv.NewReader(gzipReader)
	csvReader.Comma = csvSep

	return file, gzipReader, csvReader, nil
}
