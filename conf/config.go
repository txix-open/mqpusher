package conf

import (
	_ "embed"
	"os"
	"path"
	"time"

	"github.com/pkg/errors"
	"github.com/txix-open/isp-kit/config"
	"github.com/txix-open/isp-kit/dbx"
	"github.com/txix-open/isp-kit/grmqx"
	"github.com/txix-open/isp-kit/validator"
)

//go:embed config.yml
var EmbedConfig []byte

type Config struct {
	LogLevel            string `validate:"required,oneof=debug info error fatal"`
	ScriptPath          string
	DataSources         DataSources
	Target              Target
	ProgressLogInterval time.Duration
	IsPlainTextMode     bool
}

type DataSources struct {
	DataBase *DbDataSource
	RabbitMq *RabbitMqDataSource
	Csv      *CsvDataSource
	Json     *JsonDataSource
}

type DbDataSource struct {
	Client          dbx.Config
	Table           string   `validate:"required"`
	Parallel        int      `validate:"required,min=1"`
	BatchSize       uint64   `validate:"required,min=100"`
	PrimaryKey      []string `validate:"required,min=1"`
	SelectedColumns []string
	WhereClause     string
}

type RabbitMqDataSource struct {
	Client         grmqx.Connection
	Consumer       grmqx.Consumer
	ConsumeTimeout time.Duration
}

type CsvDataSource struct {
	FilePath string `validate:"required"`
	Sep      string `validate:"required"`
}

type JsonDataSource struct {
	FilePath string `validate:"required"`
}

type Target struct {
	Client            grmqx.Connection
	Publisher         grmqx.Publisher
	Rps               int `validate:"required,min=1"`
	EnableMessageLogs bool
	ShouldPublishSync bool
}

func LoadConfig(isDev bool) (Config, error) {
	cfgPath, err := getConfigFilePath(isDev)
	if err != nil {
		return Config{}, errors.WithMessage(err, "get config file path")
	}
	cfgReader, err := config.New(
		config.WithExtraSource(config.NewYamlConfig(cfgPath)),
		config.WithValidator(validator.Default),
	)
	if err != nil {
		return Config{}, errors.WithMessage(err, "config new")
	}
	cfg := Config{}
	if err := cfgReader.Read(&cfg); err != nil {
		return Config{}, errors.WithMessage(err, "config read")
	}
	return cfg, nil
}

func getConfigFilePath(isDev bool) (string, error) {
	cfgPath := os.Getenv("APP_CONFIG_PATH")
	if cfgPath != "" {
		return cfgPath, nil
	}
	if isDev {
		return "./conf/config.yml", nil
	}
	return RelativePathFromBin("config.yml")
}

func RelativePathFromBin(part string) (string, error) {
	ex, err := os.Executable()
	if err != nil {
		return "", errors.WithMessage(err, "get executable path")
	}
	return path.Join(path.Dir(ex), part), nil
}
