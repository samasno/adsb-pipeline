package consume

import (
	"context"
	"log"

	"github.com/nats-io/nats.go/jetstream"
)

type LiveApi interface {
	Broadcast([]byte)
	Close(context.Context) error
}

type LiveApiConsumer struct {
	*Consumer
	liveapi LiveApi
}

func NewLiveApiConsumer(ctx context.Context, api LiveApi, stream jetstream.Stream, conf jetstream.ConsumerConfig) (*LiveApiConsumer, error) {
	c := &LiveApiConsumer{
		liveapi: api,
	}
	consumer, err := NewConsumer(ctx, stream, conf, c.Callback)
	if err != nil {
		return nil, err
	}

	c.Consumer = consumer

	return c, nil
}

func (c *LiveApiConsumer) Callback(msg jetstream.Msg) {
	c.liveapi.Broadcast(msg.Data())
	if err := msg.Ack(); err != nil {
		log.Println(err)
	}
}

func (c *LiveApiConsumer) Stop() {
	c.Consumer.Stop()
	c.liveapi.Close(c.ctx)
}
