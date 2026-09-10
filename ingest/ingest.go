package ingest

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type SBSIngestWorker struct {
	js      jetstream.JetStream
	ctx     context.Context
	src     string
	subject string
	errc    chan error
}

func SBSIngest(ctx context.Context, addr string, js jetstream.JetStream, subject string) *SBSIngestWorker {
	w := &SBSIngestWorker{
		src:     addr,
		subject: subject,
		ctx:     ctx,
		errc:    make(chan error, 1),
		js:      js,
	}

	go w.sbsIngest()

	return w
}

func (w *SBSIngestWorker) sbsIngest() {
	conn, err := net.Dial("tcp", w.src)
	if err != nil {
		w.errc <- err
		return
	}
	defer conn.Close()

	r := bufio.NewReader(conn)
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		select {
		case <-w.ctx.Done():
			w.errc <- w.ctx.Err()
			return
		default:
		}

		msg := scanner.Text()
		event, err := ParseSBSMessage(msg)
		if err != nil {
			continue
		}
		eventB, err := json.Marshal(event)
		if err != nil {
			log.Println(err)
			continue
		}

		_, err = w.js.Publish(w.ctx, w.subject, eventB)
		if err != nil {
			log.Println(err)
			w.errc <- err
			return
		}
	}

	w.errc <- scanner.Err()
}

func (w *SBSIngestWorker) Error() chan error {
	return w.errc
}

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

type PositionEvent struct {
	ICAO      string   `json:"icao"`
	Callsign  *string  `json:"call_sign"`
	Altitude  *int     `json:"altitude"`
	Latitude  *float64 `json:"latitude"`
	Longitude *float64 `json:"longitude"`
	Speed     *float64 `json:"speed"`
	Track     *float64 `json:"track"`
}

func ParseSBSMessage(line string) (*PositionEvent, error) {
	fields := strings.Split(strings.TrimSpace(line), ",")
	if len(fields) < 22 || fields[0] != "MSG" {
		return nil, fmt.Errorf("telemetry: unexpected SBS line: %q", line)
	}

	return &PositionEvent{
		ICAO:      fields[4],
		Callsign:  nonEmpty(fields[10]),
		Altitude:  parseIntPtr(fields[11]),
		Speed:     parseFloatPtr(fields[12]),
		Track:     parseFloatPtr(fields[13]),
		Latitude:  parseFloatPtr(fields[14]),
		Longitude: parseFloatPtr(fields[15]),
	}, nil
}

func nonEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func parseIntPtr(s string) *int {
	if s == "" {
		return nil
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return nil
	}
	return &v
}

func parseFloatPtr(s string) *float64 {
	if s == "" {
		return nil
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil
	}
	return &v
}
