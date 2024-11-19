package action

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/panjf2000/ants/v2"
	"github.com/pkg/errors"
	"github.com/txix-open/isp-kit/log"
	"github.com/txix-open/mqpusher/domain"
)

const (
	poolSize = 300
)

type converter interface {
	Convert(data any) (any, error)
}

type publisher interface {
	Publish(ctx context.Context, data any) error
}

type publishAction struct {
	dataSource domain.DataSource
	converter  converter
	target     publisher

	publishedCounter *atomic.Uint64

	logInterval time.Duration
	logger      log.Logger
}

func NewPublish(dataSource domain.DataSource, target publisher) publishAction {
	return publishAction{
		dataSource:       dataSource,
		converter:        nil,
		target:           target,
		publishedCounter: new(atomic.Uint64),
		logInterval:      0,
		logger:           nil,
	}
}

func (p publishAction) WithConverter(converter converter) publishAction {
	p.converter = converter
	return p
}

func (p publishAction) LogProgress(logInterval time.Duration, logger log.Logger) publishAction {
	p.logInterval = logInterval
	p.logger = logger
	return p
}

func (p publishAction) Do(ctx context.Context, shouldPublishSync bool) error {
	if p.logInterval > 0 {
		done := make(chan struct{})
		defer close(done)
		go p.logProgress(ctx, done)
	}
	if shouldPublishSync {
		return p.doSync(ctx)
	}
	return p.doAsync(ctx)
}

const (
	readLogField        = "read"
	doneReadingLogField = "done reading"
	publishedLogField   = "published"
	intervalLogField    = "interval"
	mpsLogField         = "mps"
)

func (p publishAction) logProgress(ctx context.Context, done <-chan struct{}) {
	ticker := time.NewTicker(p.logInterval)
	defer ticker.Stop()

	var (
		readDataCount      uint64
		publishedDataCount uint64
		isDone             = false
	)
	for !isDone {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			isDone = true
		case <-done:
			isDone = true
		}

		progress := p.dataSource.Progress()
		newPublishedDataCount := p.publishedCounter.Load()
		publishedDelta := newPublishedDataCount - publishedDataCount

		logFields := []log.Field{
			log.String(intervalLogField, p.logInterval.String()),
			log.Any(readLogField, progress.ReadDataCount-readDataCount),
			log.Any(publishedLogField, publishedDelta),
			log.Any(mpsLogField, float64(publishedDelta)/p.logInterval.Seconds()),
		}
		if progress.ReadDataPercent != nil {
			logFields = append(logFields, log.String(doneReadingLogField, fmt.Sprintf("%0.2f%%", *progress.ReadDataPercent)))
		}
		p.logger.Info(ctx, "progress...", logFields...)

		readDataCount = progress.ReadDataCount
		publishedDataCount = newPublishedDataCount
	}
}

func (p publishAction) doAsync(ctx context.Context) error {
	var (
		wg      = new(sync.WaitGroup)
		errChan = make(chan error, poolSize)
	)

	pool, err := ants.NewPoolWithFunc(poolSize, func(v any) {
		defer wg.Done()
		err := p.submit(ctx, v)
		if err != nil {
			errChan <- errors.WithMessage(err, "submit")
		}
	}, ants.WithPreAlloc(true))
	if err != nil {
		return errors.WithMessage(err, "new pool with func")
	}
	defer pool.Release()

	err = p.do(ctx, func(ctx context.Context, v any) error {
		wg.Add(1)
		err = pool.Invoke(v)
		if err != nil {
			return errors.WithMessage(err, "pool invoke")
		}

		select {
		case err := <-errChan:
			return errors.WithMessage(err, "err chan")
		default:
			return nil
		}
	})
	if err != nil {
		return errors.WithMessage(err, "do")
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case err := <-errChan:
		return errors.WithMessage(err, "err chan outer")
	case <-ctx.Done():
		return errors.WithMessage(ctx.Err(), "ctx done")
	case <-done:
		return nil
	}
}

func (p publishAction) doSync(ctx context.Context) error {
	return p.do(ctx, p.submit)
}

type submitFunc func(ctx context.Context, v any) error

func (p publishAction) do(ctx context.Context, submitFn submitFunc) error {
	for {
		v, err := p.dataSource.GetData(ctx)
		switch {
		case errors.Is(err, domain.ErrNoData):
			return nil
		case err != nil:
			return errors.WithMessage(err, "get data")
		}

		if v.RequestId != "" {
			ctx = log.ToContext(ctx, log.String("requestId", v.RequestId)) // nolint:fatcontext
		}
		err = submitFn(ctx, v.Data)
		if err != nil {
			return errors.WithMessage(err, "submit data")
		}
	}
}

func (p publishAction) submit(ctx context.Context, v any) error {
	var err error
	if p.converter != nil {
		v, err = p.converter.Convert(v)
		if err != nil {
			return errors.WithMessage(err, "convert data with script")
		}
	}

	if v == nil {
		return nil
	}

	err = p.target.Publish(ctx, v)
	if err != nil {
		return errors.WithMessage(err, "publish data to target")
	}

	p.publishedCounter.Add(1)

	return nil
}
