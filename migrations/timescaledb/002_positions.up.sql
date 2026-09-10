CREATE TABLE positions (
    icao TEXT NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    altitude INTEGER,
    PRIMARY KEY (icao, ts)
);

SELECT create_hypertable('positions', 'ts');
