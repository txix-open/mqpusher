package source

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/txix-open/grmq/consumer"
	"github.com/txix-open/isp-kit/grmqx"
	"github.com/txix-open/isp-kit/json"
	"github.com/txix-open/isp-kit/log"
	"github.com/txix-open/isp-kit/requestid"
	"github.com/txix-open/mqpusher/conf"
	"github.com/txix-open/mqpusher/domain"
)

type rabbitMqDataSource struct {
	cli      *grmqx.Client
	logger   log.Logger
	dataChan chan domain.Payload
	errChan  chan error

	readCounter     *atomic.Uint64
	consumeTimeout  time.Duration
	isPlainTextMode bool
}

func NewRabbitMq(ctx context.Context, cfg conf.RabbitMqDataSource, logger log.Logger, isPlainTextMode bool) (rabbitMqDataSource, error) {
	dataSource := rabbitMqDataSource{
		cli:             grmqx.New(logger),
		logger:          logger,
		dataChan:        make(chan domain.Payload),
		errChan:         make(chan error),
		readCounter:     new(atomic.Uint64),
		consumeTimeout:  5 * time.Second,
		isPlainTextMode: isPlainTextMode,
	}
	if cfg.ConsumeTimeout > 0 {
		dataSource.consumeTimeout = cfg.ConsumeTimeout
	}

	err := dataSource.cli.Upgrade(ctx, grmqx.NewConfig(
		cfg.Client.Url(),
		grmqx.WithConsumers(cfg.Consumer.DefaultConsumer(dataSource)),
		grmqx.WithDeclarations(grmqx.TopologyFromConsumers(cfg.Consumer)),
	))
	if err != nil {
		return rabbitMqDataSource{}, errors.WithMessage(err, "upgrade rmq cli")
	}

	return dataSource, nil
}

func (r rabbitMqDataSource) GetData(ctx context.Context) (*domain.Payload, error) {
	select {
	case v, ok := <-r.dataChan:
		if !ok {
			return nil, domain.ErrNoData
		}
		r.readCounter.Add(1)
		return &v, nil
	case err := <-r.errChan:
		return nil, err
	case <-time.After(r.consumeTimeout):
		close(r.dataChan)
		r.logger.Info(ctx, "consume timeout")
		return nil, domain.ErrNoData
	}
}

func (r rabbitMqDataSource) Progress() domain.Progress {
	return domain.Progress{
		ReadDataCount:   r.readCounter.Load(),
		ReadDataPercent: nil,
	}
}

func (r rabbitMqDataSource) Handle(ctx context.Context, delivery *consumer.Delivery) {
	bytes := delivery.Source().Body
	payload := domain.Payload{
		RequestId: requestid.FromContext(ctx),
		Data:      bytes,
	}
	if !r.isPlainTextMode {
		var data any
		err := json.Unmarshal(bytes, &data)
		if err != nil {
			r.errChan <- errors.WithMessagef(err, "unmarshal delivery body; request id = %s", payload.RequestId)
			err = delivery.Retry()
			if err != nil {
				r.errChan <- errors.WithMessagef(err, "retry delivery; request id = %s", payload.RequestId)
			}
			return
		}
		payload.Data = data
	}

	r.dataChan <- payload
	err := delivery.Ack()
	if err != nil {
		r.errChan <- errors.WithMessagef(err, "ack delivery; request id = %s", payload.RequestId)
	}
}

func (r rabbitMqDataSource) Close(_ context.Context) error {
	r.cli.Close()
	return nil
}
