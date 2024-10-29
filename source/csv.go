package source

import (
	"context"
	"encoding/csv"
	"io"
	"os"
	"sync/atomic"
	"unicode/utf8"

	"github.com/pkg/errors"
	"github.com/txix-open/mqpusher/conf"
	"github.com/txix-open/mqpusher/domain"
	"github.com/txix-open/mqpusher/utils"
)

type csvDataSource struct {
	csvReader *csv.Reader
	closeFile func() error

	readCounter   *atomic.Uint64
	readerCounter utils.ReaderCounter

	columns  []string
	fileSize float64
}

func NewCsv(cfg conf.CsvDataSource) (csvDataSource, error) {
	file, err := os.Open(cfg.FilePath)
	if err != nil {
		return csvDataSource{}, errors.WithMessagef(err, "open file '%s'", cfg.FilePath)
	}
	info, err := file.Stat()
	if err != nil {
		return csvDataSource{}, errors.WithMessage(err, "file stat")
	}
	readerCounter := utils.NewReaderCounter(file)

	csvReader := csv.NewReader(readerCounter)
	sep, _ := utf8.DecodeRuneInString(cfg.Sep)
	csvReader.Comma = sep
	csvReader.ReuseRecord = true
	csvReader.LazyQuotes = true

	row, err := csvReader.Read()
	if err != nil {
		return csvDataSource{}, errors.WithMessage(err, "read csv row")
	}
	columns := make([]string, len(row))
	copy(columns, row)

	return csvDataSource{
		csvReader:     csvReader,
		closeFile:     file.Close,
		readCounter:   new(atomic.Uint64),
		readerCounter: readerCounter,
		columns:       columns,
		fileSize:      float64(info.Size()),
	}, nil
}

func (c csvDataSource) GetData(_ context.Context) (*domain.Payload, error) {
	v, err := c.csvReader.Read()
	switch {
	case errors.Is(err, io.EOF):
		return nil, domain.ErrNoData
	case err != nil:
		return nil, errors.WithMessage(err, "csv reader read")
	}

	data := make(map[string]any)
	for i, column := range c.columns {
		data[column] = v[i]
	}

	c.readCounter.Add(1)

	return &domain.Payload{Data: data}, nil
}

func (c csvDataSource) Progress() domain.Progress {
	readDataPercent := float64(c.readerCounter.Count()) / c.fileSize * 100
	return domain.Progress{
		ReadDataCount:   c.readCounter.Load(),
		ReadDataPercent: &readDataPercent,
	}
}

func (c csvDataSource) Close(_ context.Context) error {
	err := c.closeFile()
	if err != nil {
		return errors.WithMessage(err, "close file")
	}
	return nil
}
