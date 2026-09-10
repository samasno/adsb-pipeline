package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/samasno/adsb-pipeline/consume"
	"github.com/samasno/adsb-pipeline/natconn"
)

func main() {
	natsUrl := os.Getenv("NATS_URL")
	if natsUrl == "" {
		natsUrl = "127.0.0.1:4222"
	}

	streamName := os.Getenv("NATS_STREAM_NAME")
	if streamName == "" {
		streamName = "adsb"
	}

	subject := os.Getenv("NATS_SUBJECT")
	if subject == "" {
		subject = "adsb.sbs1"
	}

	streamConf := jetstream.StreamConfig{
		Name:     streamName,
		Subjects: []string{subject},
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	nc, js, err := natconn.ConnectNats(ctx, natsUrl)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer nc.Close()

	stream, err := natconn.FetchStream(ctx, js, &streamConf)
	if err != nil {
		log.Fatal(err)
	}

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	consumerConf := jetstream.ConsumerConfig{
		Durable:       "sbs-writer",
		Description:   "writes sbs data to timescaledb",
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: subject,
		AckWait:       time.Second * 2,
	}

	consumer, err := consume.NewSBSTimescaleConsumer(ctx, stream, consumerConf)
	if err != nil {
		log.Fatal(err)
	}

	cc, err := consumer.Consume()
	if err != nil {
		log.Fatal(err)
	}

	<-shutdown
	cc.Stop()
}
