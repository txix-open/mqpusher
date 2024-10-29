package rmq

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/rabbitmq/amqp091-go"
	publisher2 "github.com/txix-open/grmq/publisher"
	"github.com/txix-open/isp-kit/grmqx"
	"github.com/txix-open/isp-kit/json"
	"github.com/txix-open/isp-kit/log"
	"github.com/txix-open/isp-kit/retry"
	"github.com/txix-open/mqpusher/conf"
	"go.uber.org/ratelimit"
)

const (
	maxRetryElapsedTime = 5 * time.Second
)

type publisher struct {
	rmqCli  *grmqx.Client
	rmqPub  *publisher2.Publisher
	limiter ratelimit.Limiter
}

func NewPublisher(ctx context.Context, cfg conf.Target, logger log.Logger) (publisher, error) {
	var rmqPub *publisher2.Publisher
	if cfg.EnableMessageLogs {
		rmqPub = cfg.Publisher.DefaultPublisher(grmqx.PublisherLog(logger))
	} else {
		rmqPub = cfg.Publisher.DefaultPublisher()
	}

	rmqCli := grmqx.New(logger)
	err := rmqCli.Upgrade(ctx, grmqx.NewConfig(
		cfg.Client.Url(),
		grmqx.WithPublishers(rmqPub),
	))
	if err != nil {
		return publisher{}, errors.WithMessage(err, "upgrade rmq cli")
	}

	return publisher{
		rmqCli:  rmqCli,
		rmqPub:  rmqPub,
		limiter: ratelimit.New(cfg.Rps),
	}, nil
}

func (p publisher) Publish(ctx context.Context, data any) error {
	var err error
	body, isPlainText := data.([]byte)
	if !isPlainText {
		body, err = json.Marshal(data)
		if err != nil {
			return errors.WithMessage(err, "marshal payload")
		}
	}

	err = retry.NewExponentialBackoff(maxRetryElapsedTime).Do(ctx, func() error {
		_ = p.limiter.Take()
		return p.rmqPub.Publish(ctx, &amqp091.Publishing{Body: body})
	})
	if err != nil {
		return errors.WithMessagef(err, "publish message to '%s'", p.rmqPub.RoutingKey)
	}

	return nil
}

func (p publisher) Close() {
	p.rmqCli.Close()
}
