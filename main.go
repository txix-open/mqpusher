package main

import (
	"flag"
	"io/ioutil"
	"os"
	"runtime/debug"
	"time"

	"github.com/asaskevich/govalidator"
	"github.com/integration-system/isp-event-lib/mq"
	log "github.com/integration-system/isp-log"
	"github.com/integration-system/mqpusher/script"
	jsoniter "github.com/json-iterator/go"
	"github.com/streadway/amqp"
	"gopkg.in/yaml.v2"
)

const (
	publisherName = "publisher_name"
)

type DataSource interface {
	GetData() (interface{}, error)
	Progress() (int64, float32)
	Close() error
}

var (
	csvFilepath    = ""
	jsonFilepath   = ""
	configFilepath = ""
	scriptFilepath = ""
)

var json = jsoniter.ConfigFastest

func main() {
	debug.SetGCPercent(1500)
	flag.StringVar(&configFilepath, "config", "/etc/mqpusher/config.yml", "config file path")
	flag.StringVar(&csvFilepath, "csv_file", "", "csv source file path")
	flag.StringVar(&jsonFilepath, "json_file", "", "json source file path")
	flag.StringVar(&scriptFilepath, "script", "", "script file path")
	flag.CommandLine.SetOutput(os.Stdout)
	flag.Parse()

	// Configuration
	cfg := Config{}
	b, err := ioutil.ReadFile(configFilepath)
	if err != nil {
		log.Errorf(0, "reading config: %v", err)
		return
	}
	err = yaml.Unmarshal(b, &cfg)
	if err != nil {
		log.Errorf(0, "parsing config: %v", err)
		return
	}

	if csvFilepath != "" {
		cfg.Source.Csv = &CsvSource{Filename: csvFilepath}
	}
	if jsonFilepath != "" {
		cfg.Source.Json = &JsonSource{Filename: jsonFilepath}
	}
	if scriptFilepath != "" {
		cfg.Script = Script{Filename: scriptFilepath}
	}

	_, err = govalidator.ValidateStruct(cfg)
	if err != nil {
		log.Errorf(0, "invalid config: %v", govalidator.ErrorsByField(err))
		return
	}

	// Publisher
	mqClient := mq.NewRabbitClient()
	publishers := map[string]mq.PublisherCfg{
		publisherName: cfg.Target.Publisher,
	}
	mqClient.ReceiveConfiguration(cfg.Target.Rabbit,
		mq.WithPublishers(publishers),
	)
	defer mqClient.Close()
	time.Sleep(500 * time.Millisecond) // REMOVE: wait for publisher initialization

	// Script
	var convert func(interface{}) (interface{}, error)

	if cfg.Script.Filename != "" {
		b, err := ioutil.ReadFile(cfg.Script.Filename)
		if err != nil {
			log.Errorf(0, "reading script: %v", err)
			return
		}
		scr, err := script.Create(b)
		if err != nil {
			log.Errorf(0, "parsing script: %v", err)
			return
		}
		convert = func(data interface{}) (interface{}, error) {
			val, err := script.Default().Execute(scr, data)
			if err != nil {
				return nil, err
			}
			return val, nil
		}
	}

	// Handling
	var source DataSource
	defer func() {
		if source == nil {
			return
		}
		err := source.Close()
		if err != nil {
			log.Errorf(0, "closing source: %v", err)
		}
	}()

	switch {
	case cfg.Source.Csv != nil:
		source, err = NewCsvDataSource(*cfg.Source.Csv)
		if err != nil {
			log.Errorf(0, "creating csv source: %v", err)
			return
		}
	case cfg.Source.Json != nil:
		source, err = NewJsonDataSource(*cfg.Source.Json)
		if err != nil {
			log.Errorf(0, "creating json source: %v", err)
			return
		}
	case cfg.Source.DB != nil:
		source, err = NewDbDataSource(*cfg.Source.DB)
		if err != nil {
			log.Errorf(0, "creating db source: %v", err)
			return
		}
	default:
		log.Error(0, "no source specified")
		return
	}

	started := time.Now()
	defer func() {
		totalCount, _ := source.Progress()
		log.Infof(0, "total processed rows %d, elapsed time: %s", totalCount, time.Since(started).String())
	}()

	convertCh := make(chan interface{}, 3000) // TODO cap
	publishCh := make(chan []byte, 3000)      // TODO cap

	go func() {
		const printProgressInterval = 30 * time.Second
		var count int64
		for range time.NewTicker(printProgressInterval).C {
			newTotal, percent := source.Progress()
			diff := newTotal - count
			log.Infof(0, "processed %d rows in %s; approximately %0.2f%% done", diff, printProgressInterval, percent)
			count = newTotal
		}
	}()

	go func() {
		defer func() {
			close(convertCh)
		}()

		for {
			row, err := source.GetData()
			if err != nil {
				log.Errorf(0, "error reading row: %v", err)
				return
			} else if row == nil {
				return
			}
			convertCh <- row
		}
	}()

	go func() {
		defer func() {
			close(publishCh)
		}()

		for row := range convertCh {
			if convert != nil {
				row, err = convert(row)
				if err != nil {
					log.Errorf(0, "error executing script: %v", err)
					return
				}
			}

			data, err := json.Marshal(row)
			if err != nil {
				log.Errorf(0, "error marshaling row: %v", err)
				return
			}
			publishCh <- data
		}
	}()

	for data := range publishCh {
		err := mqClient.GetPublisher(publisherName).Publish(amqp.Publishing{
			ContentType:  "application/json",
			Body:         data,
			DeliveryMode: amqp.Persistent,
		})
		if err != nil {
			log.Errorf(0, "error publishing row: %v", err)
			return
		}
	}

	log.Info(0, "successfully finished")
}
