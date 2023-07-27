package main

import (
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/asaskevich/govalidator"
	"github.com/integration-system/isp-event-lib/mq"
	log "github.com/integration-system/isp-log"
	"github.com/integration-system/isp-script"
	"github.com/integration-system/mqpusher/conf"
	"github.com/integration-system/mqpusher/source"
	jsoniter "github.com/json-iterator/go"
	"github.com/panjf2000/ants/v2"
	"github.com/spf13/cast"
	"github.com/streadway/amqp"
	"gopkg.in/yaml.v2"
)

const (
	publisherName = "publisher_name"
)

var (
	csvFilepath    = ""
	jsonFilepath   = ""
	configFilepath = ""
	scriptFilepath = ""
	logInterval    = 0
	pushString     = false
)

func main() {
	flag.StringVar(&configFilepath, "config", "/etc/mqpusher/config.yml", "config file path")
	flag.StringVar(&csvFilepath, "csv_file", "", "csv source file path")
	flag.StringVar(&jsonFilepath, "json_file", "", "json source file path")
	flag.StringVar(&scriptFilepath, "script", "", "script file path")
	flag.BoolVar(&pushString, "string", false, "push data as string instead json")
	flag.IntVar(&logInterval, "log", 30, "log interval in seconds")
	flag.CommandLine.SetOutput(os.Stdout)
	flag.Parse()

	// Configuration
	cfg := conf.Config{}
	b, err := os.ReadFile(configFilepath)
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
		cfg.Source.Csv = &conf.CsvSource{Filename: csvFilepath}
	}
	if jsonFilepath != "" {
		cfg.Source.Json = &conf.JsonSource{Filename: jsonFilepath}
	}
	if scriptFilepath != "" {
		cfg.Script = conf.Script{Filename: scriptFilepath}
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

	publisher := mqClient.GetPublisher(publisherName)
	if publisher == nil {
		log.Errorf(0, "mq publisher is not initialized")
		return
	}
	publish := func(v any) error {
		var err error
		var body []byte
		if pushString {
			body = []byte(cast.ToString(v))
		} else {
			body, err = jsoniter.ConfigFastest.Marshal(v)
		}
		if err != nil {
			return err
		}

		return publisher.Publish(amqp.Publishing{
			Body:         body,
			DeliveryMode: amqp.Persistent,
		})
	}

	// Script
	var convert func(any) (any, error)

	if cfg.Script.Filename != "" {
		b, err := os.ReadFile(cfg.Script.Filename)
		if err != nil {
			log.Errorf(0, "reading script: %v", err)
			return
		}
		scr, err := scripts.NewScript([]byte("(function() {\n"), b, []byte("\n})();"))
		if err != nil {
			log.Errorf(0, "parsing script: %v", err)
			return
		}
		scriptEngine := scripts.NewEngine()
		convert = func(data any) (any, error) {
			val, err := scriptEngine.Execute(scr, data,
				scripts.WithTimeout(5*time.Second),
				scripts.WithFieldNameMapper(jsonFieldNameMapper{}),
				scripts.WithSet("sha256", Sha256),
				scripts.WithSet("sha512", Sha512),
				scripts.WithSet("generateUUIDv4", UUIDv4),
				scripts.WithSet("time", map[string]any{
					"format": FormatDate,
					"parse":  ParseDate,
				}),
				scripts.WithDefaultToolkit(),
			)
			if err != nil {
				return nil, err
			}
			return val, nil
		}
	}

	// Handling
	var ds source.DataSource
	defer func() {
		if ds == nil {
			return
		}
		err := ds.Close()
		if err != nil {
			log.Errorf(0, "closing source: %v", err)
		}
	}()

	switch {
	case cfg.Source.Csv != nil:
		ds, err = source.NewCsvDataSource(*cfg.Source.Csv)
		if err != nil {
			log.Errorf(0, "creating csv source: %v", err)
			return
		}
	case cfg.Source.Json != nil:
		ds, err = source.NewJsonDataSource(*cfg.Source.Json)
		if err != nil {
			log.Errorf(0, "creating json source: %v", err)
			return
		}
	case cfg.Source.DB != nil:
		if cfg.Source.DB.ConcurrentDBSource != nil {
			ds, err = source.NewConcurrentDbDataSource(*cfg.Source.DB, *cfg.Source.DB.ConcurrentDBSource)
		} else {
			ds, err = source.NewDbDataSource(*cfg.Source.DB)
		}
		if err != nil {
			log.Errorf(0, "creating db source: %v", err)
			return
		}
	case cfg.Source.Mq != nil:
		ds = source.NewMqSource(*cfg.Source.Mq)
	default:
		log.Error(0, "no source specified")
		return
	}

	started := time.Now()
	defer func() {
		if err := recover(); err != nil {
			log.Errorf(0, "panic: %v", err)
		}
		totalCount, _ := ds.Progress()
		log.Infof(0, "total processed rows %d, elapsed time: %s", totalCount, time.Since(started).String())
	}()

	if logInterval > 0 {
		printProgressInterval := time.Duration(logInterval) * time.Second
		ticker := time.NewTicker(printProgressInterval)
		defer ticker.Stop()

		go func() {
			var count int64
			for range ticker.C {
				newTotal, percent := ds.Progress()
				diff := newTotal - count
				log.Infof(0, "processed %d rows in %s; approximately %0.2f%% done", diff, printProgressInterval, percent)
				count = newTotal
			}
		}()
	}

	syncSubmit := func(row any) error {
		if convert != nil {
			row, err = convert(row)
			if err != nil {
				return fmt.Errorf("error executing script: %v", err)
			}
		}
		if row == nil {
			return nil
		}
		err = publish(row)
		if err != nil {
			return fmt.Errorf("error publishing row: %v", err)
		}

		return nil
	}
	submit := syncSubmit
	wg := sync.WaitGroup{}
	const poolSize = 300
	var asyncErrorsCh chan error
	if cfg.Target.Async {
		asyncErrorsCh = make(chan error, poolSize)
		p, err := ants.NewPoolWithFunc(poolSize, func(arg any) {
			defer wg.Done()
			submitErr := syncSubmit(arg)
			if submitErr != nil {
				asyncErrorsCh <- submitErr
			}
		}, ants.WithPreAlloc(true))
		if err != nil {
			panic(err)
		}
		defer p.Release()

		submit = func(row any) error {
			wg.Add(1)
			return p.Invoke(row)
		}
	}

	for {
		row, err := ds.GetData()
		if err != nil {
			log.Errorf(0, "error reading row: %v", err)
			return
		} else if row == nil {
			break
		}
		err = submit(row)
		if err != nil {
			log.Errorf(0, "error submitting row: %v", err)
			return
		}

		select {
		case err := <-asyncErrorsCh:
			if err != nil {
				log.Errorf(0, "error submitting row: %v", err)
				return
			}
		default:
		}
	}

	wg.Wait()
	log.Info(0, "successfully finished")
}
