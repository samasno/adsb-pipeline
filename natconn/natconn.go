package natconn

import (
	"context"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func ConnectNats(ctx context.Context, natsaddr string) (*nats.Conn, jetstream.JetStream, error) {
	nc, err := nats.Connect(natsaddr)
	if err != nil {
		return nil, nil, err
	}

	js, err := jetstream.New(nc)
	if err != nil {
		defer nc.Close()
		return nil, nil, err
	}

	return nc, js, nil
}

func FetchStream(ctx context.Context, js jetstream.JetStream, conf *jetstream.StreamConfig) (jetstream.Stream, error) {
	if conf == nil {
		return nil, fmt.Errorf("stream config required")
	}

	stream, err := js.Stream(ctx, conf.Name)
	if err != nil && !errors.Is(err, jetstream.ErrStreamNotFound) {
		return nil, err
	}

	if err == nil {
		return stream, nil
	}

	stream, err = js.CreateStream(ctx, *conf)
	return stream, err
}

func NewConsumer(ctx context.Context, stream jetstream.Stream, conf jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	return stream.CreateOrUpdateConsumer(ctx, conf)
}
