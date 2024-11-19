package action

import (
	"context"
	"os"

	"github.com/pkg/errors"
	"github.com/txix-open/isp-kit/log"
	"github.com/txix-open/mqpusher/conf"
)

type generateConfigAction struct {
	logger log.Logger
}

func NewGenerateConfig(logger log.Logger) generateConfigAction {
	return generateConfigAction{logger: logger}
}

func (g generateConfigAction) Do(ctx context.Context) error {
	path, err := conf.RelativePathFromBin("config.yml")
	if err != nil {
		return errors.WithMessage(err, "relative path from bin")
	}

	file, err := os.Create(path)
	if err != nil {
		return errors.WithMessage(err, "create config file")
	}
	defer func() {
		err := file.Close()
		if err != nil {
			g.logger.Error(ctx, errors.WithMessage(err, "close config file"))
		}
	}()

	_, err = file.Write(conf.EmbedConfig)
	if err != nil {
		return errors.WithMessage(err, "write config file")
	}

	return nil
}
