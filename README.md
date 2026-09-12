# adsb-pipeline

A real-time ADS-B ingestion pipeline: it reads the live SBS-1 (BaseStation)
feed produced by a [dump1090](https://github.com/antirez/dump1090) receiver,
publishes each aircraft position to a NATS JetStream stream, and fans it out
to two independent consumers — one durably persists every position to
TimescaleDB, the other pushes live updates over a WebSocket to a browser demo
that plots aircraft on a map as they move.

A bundled mock ADS-B server means the whole stack runs and produces
realistic-looking traffic with `docker compose up` alone — no SDR hardware or
real receiver required to see it working.

## Architecture

```
                                   NATS JetStream
                              stream: adsb  subject: adsb.sbs1
                                        │
  dump1090 (or mock-adsb) ── ingest ───▶│
   SBS-1 text feed, :30003   (parse,    │
                              publish)  │
                                        ├──▶ write-consumer ──▶ TimescaleDB
                                        │    (durable, at-least-      `positions`
                                        │     once, idempotent)       hypertable
                                        │
                                        └──▶ live-api ──▶ WebSocket ──▶ browser
                                             (durable,                  (Leaflet
                                              broadcast)                  map)
```

`ingest` is the only writer to the stream. `write-consumer` and `live-api`
are independent durable JetStream consumers reading the same subject — each
gets its own copy of every message, and each can be stopped, restarted, or
fall behind without affecting the other.

## Components

| Path | Binary | Role |
|---|---|---|
| [`cmd/ingest`](cmd/ingest) / [`ingest`](ingest) | `ingest` | Dials the SBS-1 TCP source, parses each `MSG` line into a `PositionEvent`, publishes it as JSON to JetStream. Creates the stream if it doesn't already exist. |
| [`cmd/write-consumer`](cmd/write-consumer) / [`consume/dbwriter.go`](consume/dbwriter.go) | `write-consumer` | Durable consumer (`sbs-writer`) that inserts each position into TimescaleDB. Inserts are `ON CONFLICT DO NOTHING` on `(icao, ts)`, so a redelivered message is a no-op rather than a duplicate row. |
| [`cmd/live-api`](cmd/live-api) / [`live-api`](live-api) | `live-api` | Durable consumer (`sbs-live-updates`) that broadcasts every message verbatim to all connected WebSocket clients (`GET /sbs`), and serves the embedded static demo frontend at `/`. |
| [`cmd/mock-server`](cmd/mock-server) | `mock-server` | Stands in for a real dump1090 receiver: simulates a configurable number of aircraft flying continuous paths and serves them over the same SBS-1 TCP format on `:30003`. |
| [`cmd/mock-client`](cmd/mock-client) | `mock-client` | Small debug tool — connects to an SBS-1 source and logs each raw line to stdout, for eyeballing the feed independent of the rest of the pipeline. |
| [`natconn`](natconn) | — | Shared helpers: connect to NATS, fetch-or-create a JetStream stream, create/update a consumer. |
| [`consume`](consume) | — | Shared `Consumer` wrapper around `jetstream.Consumer` with a pluggable per-message callback; `SBSWriteWorker` and `LiveApiConsumer` both embed it. |
| [`migrations/timescaledb`](migrations/timescaledb) | — | `golang-migrate`-style SQL: enables the `timescaledb` extension and creates `positions` as a hypertable on `ts`. |

## Message format

`ingest` parses SBS-1's `MSG` lines and republishes them as JSON. This same
shape is what both consumers read, and what the frontend receives over the
WebSocket:

```json
{
  "icao": "A1B2C3",
  "call_sign": "UAL123",
  "altitude": 34000,
  "latitude": 30.3119,
  "longitude": -95.4561,
  "speed": 412.5,
  "track": 87.3,
  "timestamp": 1737849600000
}
```

Fields other than `icao` are pointers, so a field absent from a given SBS
message round-trips as JSON `null` instead of a misleading zero value —
altitude `0` and "no altitude in this message" are different things.

## Running it

Requires Docker.

```bash
git clone git@github.com:samasno/adsb-pipeline.git
cd adsb-pipeline
docker compose up
```

This starts everything: the mock ADS-B generator, NATS (JetStream enabled),
TimescaleDB with migrations applied automatically, `ingest`, both consumers,
and the live API. Once it's up:

- **Live map:** [http://localhost:8080](http://localhost:8080)
- **Position history:** query TimescaleDB directly —
  `psql postgres://postgres:postgres@localhost:5432/adsb` (password
  `postgres`), `SELECT * FROM positions ORDER BY ts DESC LIMIT 10;`
- **Raw NATS feed:** point any NATS client at `localhost:4222`, subject
  `adsb.sbs1`

### Using a real receiver instead of the mock

Point `ingest` at a real dump1090 instance's SBS-1 port (default `30003`)
instead of the bundled mock by overriding `ADSB_SOURCE`, and drop the
`mock-adsb` service. In `docker-compose.yaml`, that means removing the
`mock-adsb` block and changing the `ingestion` service's environment to:

```yaml
environment:
  - ADSB_SOURCE=<receiver-host>:30003
```

## Configuration

All configuration is environment variables, with sane local-dev defaults
baked in — nothing needs to be set to run `docker compose up` as-is.

| Variable | Used by | Default | Meaning |
|---|---|---|---|
| `NATS_URL` | ingest, write-consumer, live-api | `127.0.0.1:4222` | NATS server address |
| `NATS_STREAM_NAME` | ingest, write-consumer, live-api | `adsb` | JetStream stream name |
| `NATS_SUBJECT` | ingest, write-consumer, live-api | `adsb.sbs1` | Subject positions are published/consumed on |
| `ADSB_SOURCE` | ingest | `127.0.0.1:30003` | Host:port of the SBS-1 source (dump1090 or mock) |
| `TIMESCALEDB_URL` | write-consumer | — (required) | Postgres/TimescaleDB DSN |
| `SERVER_ADDR` | live-api | `0.0.0.0:8080` | Address the WebSocket/HTTP server binds to |
| `NUM_PLANES` | mock-server | `10` | Number of simulated aircraft |

## Local development (without Docker)

Requires Go 1.25+, and NATS/TimescaleDB reachable per the variables above.

```bash
go run ./cmd/mock-server        # simulated SBS-1 feed on :30003
go run ./cmd/ingest              # requires NATS reachable
go run ./cmd/write-consumer      # requires NATS + TIMESCALEDB_URL
go run ./cmd/live-api            # requires NATS; UI on :8080
```

`go run ./cmd/mock-client` is a quick way to sanity-check the raw SBS-1
stream (mock or real) without any of the rest of the pipeline running.

## Design notes

- **At-least-once, not exactly-once.** JetStream guarantees a message is
  delivered at least once, never that it's delivered exactly once. Rather
  than fight that, `write-consumer`'s insert is idempotent (`ON CONFLICT
  DO NOTHING` on the `(icao, ts)` primary key), so a redelivery is simply a
  no-op instead of a duplicate row or a special case to handle.
- **Ack/Nak/Term are distinct outcomes.** A successful insert Acks. A
  transient failure (anything not matching a known-permanent Postgres error
  code) Naks, so JetStream redelivers it. A `not_null_violation` (`23502`)
  or `undefined_table` (`42P01`) — a malformed payload or a schema that will
  never accept this row no matter how many times it's retried — Terms
  instead, so a permanently bad message doesn't get redelivered forever.
- **Two consumers, one subject, no coordination between them.** `write-consumer`
  and `live-api` are both independent durable JetStream consumers. Either
  can be redeployed, restarted, or fall behind without the other noticing —
  there's no shared state or direct dependency between the storage path and
  the live-update path.
- **TimescaleDB hypertable.** `positions` is partitioned on `ts` via
  `create_hypertable`, which is what a time-series table like this — narrow
  rows, append-mostly, queried by time range — is actually for, as opposed
  to a plain unpartitioned Postgres table.
