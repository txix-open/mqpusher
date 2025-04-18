package command

import (
	"context"
	"os"
	"strings"

	"github.com/pkg/errors"
	"github.com/txix-open/isp-kit/log"
	"github.com/txix-open/isp-kit/validator"
	"github.com/txix-open/mqpusher/action"
	"github.com/txix-open/mqpusher/conf"
	"github.com/txix-open/mqpusher/domain"
	"github.com/txix-open/mqpusher/rmq"
	"github.com/txix-open/mqpusher/script"
	"github.com/txix-open/mqpusher/source"
	"github.com/urfave/cli/v3"
	"go.uber.org/zap/zapcore"
)

const (
	sourceFlag      = "source"
	logMsgFlag      = "log-msg"
	logIntervalFlag = "log-interval"
	filePathFlag    = "filepath"
	csvSepFlag      = "sep"
	scriptFlag      = "script"
	syncFlag        = "sync"
	plainTextFlag   = "plain-text"
)

const (
	csvSrc  = "csv"
	jsonSrc = "json"
	dbSrc   = "db"
	rmqSrc  = "rmq"
)

func Publish() *cli.Command {
	return &cli.Command{
		Name:  "publish",
		Usage: "Publish data to a single RabbitMQ queue",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     sourceFlag,
				Aliases:  []string{"s"},
				Required: true,
				Usage:    "Data source type (available: csv, json, db, rmq)",
			},
			&cli.StringFlag{
				Name:    filePathFlag,
				Aliases: []string{"f"},
				Usage:   "Path to data source file (used for csv and json data sources)",
			},
			&cli.StringFlag{
				Name:  scriptFlag,
				Usage: "Path to file with data conversion script",
			},
			&cli.DurationFlag{
				Name:  logIntervalFlag,
				Usage: "Progress logging interval",
			},
			&cli.StringFlag{
				Name:  csvSepFlag,
				Usage: "Custom csv separator",
			},
			&cli.BoolFlag{
				Name:    logMsgFlag,
				Aliases: []string{"l"},
				Usage:   "Enable logging of messages published to the queue",
				Value:   false,
			},
			&cli.BoolFlag{
				Name:  syncFlag,
				Usage: "Enable synchronous publication of data to the target queue",
				Value: false,
			},
			&cli.BoolFlag{
				Name:  plainTextFlag,
				Usage: "Enable 'plainText' sending mode, where simply read bytes from a file or string are sent without deserialization, i.e. as is (this mode is incompatible with the following data sources: csv, db; it also disables script)", // nolint:lll
				Value: false,
			},
		},
		Action: publish,
	}
}

func publish(ctx context.Context, cmd *cli.Command) error {
	sourceType := strings.ToLower(cmd.String(sourceFlag))
	cfg, err := loadAndUpdateConfig(cmd, sourceType)
	if err != nil {
		return errors.WithMessage(err, "load and update config")
	}

	logLevel, err := zapcore.ParseLevel(cfg.LogLevel)
	if err != nil {
		return errors.WithMessage(err, "parse log level")
	}
	logger, err := log.New(log.WithLevel(logLevel))
	if err != nil {
		return errors.WithMessage(err, "new logger")
	}

	dataSource, err := defineDataSource(ctx, sourceType, cfg, logger)
	if err != nil {
		return errors.WithMessage(err, "define source")
	}
	defer func() {
		err := dataSource.Close(ctx)
		if err != nil {
			logger.Error(ctx, errors.WithMessage(err, "close data source"))
		}
	}()

	target, err := rmq.NewPublisher(ctx, cfg.Target, logger)
	if err != nil {
		return errors.WithMessage(err, "new rmq publisher")
	}
	defer target.Close()

	publishAction := action.NewPublish(dataSource, target)

	isModeConflict := cfg.ScriptPath != "" && cfg.IsPlainTextMode
	if isModeConflict {
		return errors.New("plain text mode is incompatible with script mode")
	}
	if cfg.ScriptPath != "" {
		converter, err := script.NewConverter(cfg.ScriptPath)
		if err != nil {
			return errors.WithMessage(err, "new converter script")
		}
		publishAction = publishAction.WithConverter(converter)
	}
	if cfg.ProgressLogInterval > 0 {
		publishAction = publishAction.LogProgress(cfg.ProgressLogInterval, logger)
	}

	err = publishAction.Do(ctx, cfg.Target.ShouldPublishSync)
	if err != nil {
		return errors.WithMessage(err, "do publish action")
	}

	return nil
}

// nolint:ireturn
func defineDataSource(ctx context.Context, sourceType string, cfg conf.Config, logger log.Logger) (domain.DataSource, error) {
	switch sourceType {
	case csvSrc:
		src, err := source.NewCsv(*cfg.DataSources.Csv)
		if err != nil {
			return nil, errors.WithMessage(err, "new csv data source")
		}
		return src, nil
	case jsonSrc:
		path := cfg.DataSources.Json.FilePath
		if isDir(path) {
			src, err := source.NewMultipleJson(path, cfg.IsPlainTextMode)
			if err != nil {
				return nil, errors.WithMessage(err, "new multiple json data source")
			}
			return src, nil
		}
		src, err := source.NewJson(path, cfg.IsPlainTextMode)
		if err != nil {
			return nil, errors.WithMessage(err, "new json data source")
		}
		return src, nil
	case dbSrc:
		src, err := source.NewDataBase(ctx, *cfg.DataSources.DataBase, logger)
		if err != nil {
			return nil, errors.WithMessage(err, "new db data source")
		}
		return src, nil
	case rmqSrc:
		src, err := source.NewRabbitMq(ctx, *cfg.DataSources.RabbitMq, logger, cfg.IsPlainTextMode)
		if err != nil {
			return nil, errors.WithMessage(err, "new rabbitmq data source")
		}
		return src, nil
	default:
		return nil, errors.Errorf("unsupported data source '%s'", sourceType)
	}
}

func isDir(filepath string) bool {
	info, err := os.Stat(filepath)
	if err != nil {
		return false
	}
	return info.IsDir()
}

func loadAndUpdateConfig(cmd *cli.Command, sourceType string) (conf.Config, error) {
	isDev := strings.ToLower(os.Getenv("APP_MODE")) == "dev"
	cfg, err := conf.LoadConfig(isDev)
	if err != nil {
		return conf.Config{}, errors.WithMessage(err, "load config")
	}

	var (
		sourcePath        = strings.TrimSpace(cmd.String(filePathFlag))
		scriptPath        = strings.TrimSpace(cmd.String(scriptFlag))
		csvSep            = strings.TrimSpace(cmd.String(csvSepFlag))
		logInterval       = cmd.Duration(logIntervalFlag)
		enableMsgLogs     = cmd.Bool(logMsgFlag)
		shouldPublishSync = cmd.Bool(syncFlag)
		isPlainTextMode   = cmd.Bool(plainTextFlag)
	)

	switch sourceType {
	case jsonSrc:
		updateJsonSrcCfg(&cfg.DataSources, sourcePath)
	case csvSrc:
		updateCsvSrcCfg(&cfg.DataSources, sourcePath, csvSep)
	}

	if scriptPath != "" {
		cfg.ScriptPath = scriptPath
	}
	if logInterval > 0 {
		cfg.ProgressLogInterval = logInterval
	}

	cfg.Target.EnableMessageLogs = enableMsgLogs
	cfg.Target.ShouldPublishSync = shouldPublishSync
	cfg.IsPlainTextMode = isPlainTextMode

	err = validator.Default.ValidateToError(cfg)
	if err != nil {
		return conf.Config{}, errors.WithMessage(err, "validate config")
	}
	return cfg, nil
}

func updateJsonSrcCfg(dataSrc *conf.DataSources, srcPath string) {
	if srcPath != "" {
		dataSrc.Json = &conf.JsonDataSource{FilePath: srcPath}
	}
}

func updateCsvSrcCfg(dataSrc *conf.DataSources, srcPath string, sep string) {
	if srcPath == "" && sep == "" {
		return
	}

	if dataSrc.Csv == nil {
		dataSrc.Csv = new(conf.CsvDataSource)
	}
	if srcPath != "" {
		dataSrc.Csv.FilePath = srcPath
	}
	if sep != "" {
		dataSrc.Csv.Sep = sep
	}
}
