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
)

type SBSWriteWorker struct {
	*Consumer
	db *sql.DB
}

func NewSBSTimescaleConsumer(ctx context.Context, stream jetstream.Stream, conf jetstream.ConsumerConfig) (*SBSWriteWorker, error) {
	if stream == nil {
		return nil, fmt.Errorf("jetstream instance required")
	}

	db, err := ConnectToTimescaleDB(ctx)
	if err != nil {
		return nil, err
	}

	c := &SBSWriteWorker{
		db: db,
	}

	consumer, err := NewConsumer(ctx, stream, conf, c.Callback)
	if err != nil {
		defer db.Close()
		return nil, err
	}

	c.Consumer = consumer

	return c, nil
}

func (c *SBSWriteWorker) Stop() {
	c.Consumer.Stop()
	if c.db != nil {
		c.db.Close()
	}
}

func (c *SBSWriteWorker) Callback(msg jetstream.Msg) {
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
		data.ICAO, time.UnixMilli(*data.DateTimeUTC), data.Latitude, data.Longitude, data.Altitude,
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
