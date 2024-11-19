package main

import (
	"context"
	"log"
	"os"

	"github.com/txix-open/mqpusher/command"
	"github.com/urfave/cli/v3"
)

const (
	version = "2.0.0"
)

func main() {
	cmd := &cli.Command{
		Name:    "mqpusher",
		Version: version,
		Usage:   "The mqpusher tool is designed to transfer data from various source to a single RabbitMQ queue",
		Commands: []*cli.Command{
			command.Publish(),
			command.GenerateConfig(),
		},
	}
	err := cmd.Run(context.Background(), os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
