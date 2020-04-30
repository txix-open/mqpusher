package main

import (
	"github.com/integration-system/isp-event-lib/mq"
	"github.com/integration-system/isp-lib/v2/structure"
)

type (
	Config struct {
		Source Source
		Target Target
		Script Script
	}

	Source struct {
		Csv  *CsvSource
		Json *JsonSource
		DB   *DBSource
	}
	CsvSource struct {
		Filename string `valid:"required~Required"`
	}
	JsonSource struct {
		Filename string `valid:"required~Required"`
	}
	DBSource struct {
		Database structure.DBConfiguration `valid:"required~Required"`
		Query    string                    `valid:"required~Required"`
		Cursor   bool
		Parallel int
	}

	Target struct {
		Rabbit    structure.RabbitConfig `valid:"required~Required"`
		Publisher mq.PublisherCfg        `valid:"required~Required"`
	}
	Script struct {
		Filename string
	}
)
