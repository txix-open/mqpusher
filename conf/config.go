package conf

import (
	"github.com/integration-system/isp-event-lib/mq"
	"github.com/integration-system/isp-lib/v2/structure"
	"time"
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
		Mq   *MqSource
	}
	CsvSource struct {
		Filename string `valid:"required~Required"`
	}
	JsonSource struct {
		Filename string `valid:"required~Required"`
	}
	DBSource struct {
		Database           structure.DBConfiguration `valid:"required~Required"`
		Query              string
		Cursor             bool
		Parallel           int
		ConcurrentDBSource *ConcurrentDBSource `yaml:"concurrent"`
	}
	MqSource struct {
		Rabbit       structure.RabbitConfig `valid:"required~Required"`
		Consumer     mq.CommonConsumerCfg
		CloseTimeout time.Duration
	}

	ConcurrentDBSource struct {
		Table    string `valid:"required~Required"`
		Select   string
		IdColumn string
		Where    string
	}

	Target struct {
		Rabbit    structure.RabbitConfig `valid:"required~Required"`
		Publisher mq.PublisherCfg        `valid:"required~Required"`
		Async     bool
	}
	Script struct {
		Filename string
	}
)