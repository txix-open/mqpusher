package command

import (
	"context"

	"github.com/pkg/errors"
	"github.com/txix-open/isp-kit/log"
	"github.com/txix-open/mqpusher/action"
	"github.com/urfave/cli/v3"
)

func GenerateConfig() *cli.Command {
	return &cli.Command{
		Name:    "generate-config",
		Aliases: []string{"gen-cfg", "generate-cfg", "gen-config"},
		Usage:   "Generate a config file next to utility bin",
		Action:  generateConfig,
	}
}

func generateConfig(ctx context.Context, _ *cli.Command) error {
	logger, err := log.New()
	if err != nil {
		return errors.WithMessage(err, "new logger")
	}
	err = action.NewGenerateConfig(logger).Do(ctx)
	if err != nil {
		return errors.WithMessage(err, "do generate config action")
	}
	return nil
}
