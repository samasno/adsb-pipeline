package consume

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/samasno/adsb-pipeline/ingest"
	"github.com/samasno/adsb-pipeline/natconn"
)

type SBSWriteWorker struct {
	db       *sql.DB
	consumer jetstream.Consumer
	cc       jetstream.ConsumeContext
	ctx      context.Context
	cancel   context.CancelFunc
}

const SBSWriterName = "sbs-writer"
const ConsumerFetchWait = time.Second * 3

func NewSBSTimescaleConsumer(ctx context.Context, stream jetstream.Stream, conf jetstream.ConsumerConfig) (*SBSWriteWorker, error) {
	if stream == nil {
		return nil, fmt.Errorf("jetstream instance required")
	}

	db, err := ConnectToTimescaleDB(ctx)
	if err != nil {
		return nil, err
	}

	consumer, err := natconn.NewConsumer(ctx, stream, conf)
	if err != nil {
		defer db.Close()
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)

	c := &SBSWriteWorker{
		db:       db,
		consumer: consumer,
		ctx:      ctx,
		cancel:   cancel,
	}

	return c, nil
}

func (c *SBSWriteWorker) Consume() error {
	var err error
	c.cc, err = c.consumer.Consume(c.consumeOne)
	return err
}

func (c *SBSWriteWorker) Stop() {
	c.cancel()
	c.cc.Stop()
	c.db.Close()
}

func (c *SBSWriteWorker) consumeOne(msg jetstream.Msg) {
	err := c.InsertPositionEvent(msg.Data())
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		log.Println(err)
		switch pgErr.Code {
		case "23502", "42P01":
			msg.Term()
		default:
			msg.Nak()
		}
		return
	}

	if err != nil {
		log.Println(err)
		msg.Nak()
		return
	}

	msg.Ack()
}

func (c *SBSWriteWorker) InsertPositionEvent(payload []byte) error {
	data := ingest.PositionEvent{}
	err := json.Unmarshal(payload, &data)
	if err != nil {
		return err
	}

	_, err = c.db.ExecContext(
		c.ctx,
		`INSERT INTO positions (icao,ts,lat,lon,altitude) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING;`,
		data.ICAO, time.UnixMilli(*data.DateTimeUTC), *data.Latitude, *data.Longitude, *data.Altitude,
	)

	return err
}

func ConnectToTimescaleDB(ctx context.Context) (*sql.DB, error) {
	dsn := os.Getenv("TIMESCALEDB_URL")
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}

	err = db.PingContext(ctx)
	return db, err
}
