package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/samasno/adsb-pipeline/ingest"
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

	sourceUrl := os.Getenv("ADSB_SOURCE")
	if sourceUrl == "" {
		sourceUrl = "127.0.0.1:30003"
	}

	conf := jetstream.StreamConfig{
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

	_, err = natconn.FetchStream(ctx, js, &conf)
	if err != nil {
		log.Fatal(err)
	}

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	worker := ingest.SBSIngest(ctx, sourceUrl, js, subject)

	select {
	case <-shutdown:
		cancel()
		log.Println("worker closed")
		os.Exit(0)
	case err = <-worker.Error():
		log.Fatal(err)
	}
}
