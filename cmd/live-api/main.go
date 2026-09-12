package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/samasno/adsb-pipeline/consume"
	live_api "github.com/samasno/adsb-pipeline/live-api"
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

	srvAddr := os.Getenv("SERVER_ADDR")
	if srvAddr == "" {
		srvAddr = "0.0.0.0:8080"
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
		Durable:       "sbs-live-updates",
		Description:   "reads and pushes sbs updates through web sockets",
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverLastPolicy,
		FilterSubject: subject,
		AckWait:       time.Second * 2,
	}

	liveApi := live_api.NewLiveApiHandler()
	if err != nil {
		log.Fatal(err)
	}

	srv := http.Server{Addr: srvAddr}
	go func() {
		http.HandleFunc("GET /sbs", liveApi.HandleWS)
		http.Handle("GET /", live_api.Static())
		err := srv.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Println(err)
		}
	}()

	consumer, err := consume.NewLiveApiConsumer(ctx, liveApi, stream, consumerConf)
	if err != nil {
		log.Fatal(err)
	}

	err = consumer.Consume()
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("sbs live server running at %s\n", srvAddr)

	<-shutdown
	consumer.Stop()
	srv.Shutdown(ctx)
}
