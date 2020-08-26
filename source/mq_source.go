package source

import (
	"sync"
	"time"

	"github.com/integration-system/isp-event-lib/mq"
	"github.com/integration-system/isp-lib/v2/atomic"
	"github.com/integration-system/mqpusher/conf"
)

const (
	defaultTimeout = 5 * time.Second
)

type mqDataSource struct {
	cli            *mq.RabbitMqClient
	consumeTimeout time.Duration
	dataChan       chan interface{}
	errChan        chan error
	closed         bool
	consumed       *atomic.AtomicInt
	lock           sync.RWMutex
}

func (m *mqDataSource) GetData() (interface{}, error) {
	select {
	case v, ok := <-m.dataChan:
		if !ok {
			return nil, nil
		}
		m.consumed.IncAndGet()
		return v, nil
	case err := <-m.errChan:
		return nil, err
	case <-time.After(m.consumeTimeout):
		m.lock.Lock()
		m.closed = true
		var lastValue interface{}
		select {
		case lastValue = <-m.dataChan:
			m.consumed.IncAndGet()
		default:
		}
		close(m.dataChan)
		m.lock.Unlock()
		return lastValue, nil
	}
}

func (m *mqDataSource) Progress() (int64, float32) {
	return int64(m.consumed.Get()), 0
}

func (m *mqDataSource) Close() error {
	m.cli.Close()
	return nil
}

func NewMqSource(cfg conf.MqSource) DataSource {
	cli := mq.NewRabbitClient()
	if cfg.CloseTimeout <= 0 {
		cfg.CloseTimeout = defaultTimeout
	}
	mqDs := &mqDataSource{
		cli:            cli,
		consumeTimeout: cfg.CloseTimeout,
		closed:         false,
		dataChan:       make(chan interface{}),
		errChan:        make(chan error, 1024),
		consumed:       atomic.NewAtomicInt(0),
	}
	cli.ReceiveConfiguration(cfg.Rabbit, mq.WithConsumers(map[string]mq.ConsumerCfg{
		"consumer": mq.ByOneConsumerCfg{
			CommonConsumerCfg: cfg.Consumer,
			Callback: func(delivery mq.Delivery) {
				defer func() {
					err := delivery.Release()
					if err != nil {
						select {
						case mqDs.errChan <- err:
						default:
						}
					}
				}()

				var v interface{}
				if err := json.Unmarshal(delivery.GetMessage(), &v); err != nil {
					select {
					case mqDs.errChan <- err:
					default:
					}
					delivery.Nack(true)
					return
				}

				mqDs.lock.RLock()
				defer mqDs.lock.RUnlock()
				if mqDs.closed {
					delivery.Nack(true)
					return
				}

				select {
				case mqDs.dataChan <- v:
					delivery.Ack()
				default:
					delivery.Nack(true)
				}
			},
			ErrorHandler: func(err error) {
				select {
				case mqDs.errChan <- err:
				default:
				}
			},
		},
	}))
	return mqDs
}
