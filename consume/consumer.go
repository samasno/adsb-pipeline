package consume

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/samasno/adsb-pipeline/natconn"
)

type Consumer struct {
	consumer jetstream.Consumer
	cc       jetstream.ConsumeContext
	ctx      context.Context
	cancel   context.CancelFunc
}

func NewConsumer(ctx context.Context, stream jetstream.Stream, conf jetstream.ConsumerConfig) (*Consumer, error) {
	if stream == nil {
		return nil, fmt.Errorf("jetstream instance required")
	}

	consumer, err := natconn.NewConsumer(ctx, stream, conf)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)

	c := &Consumer{
		consumer: consumer,
		ctx:      ctx,
		cancel:   cancel,
	}

	return c, nil
}

func (c *Consumer) Consume() error {
	var err error
	c.cc, err = c.consumer.Consume(c.consumeOne)
	return err
}

func (c *Consumer) consumeOne(msg jetstream.Msg) {}

func (c *Consumer) Stop() {
	if c.cancel != nil {
		c.cancel()
	}

	if c.cc != nil {
		c.cc.Stop()
	}
}
