package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
)

func main() {
	numPlanes := 10
	numPlanesEnv := os.Getenv("NUM_PLANES")
	if numPlanesEnv != "" {
		n, err := strconv.Atoi(numPlanesEnv)
		if err == nil {
			numPlanes = n
		}
	}

	conf := sConfig{
		numPlanes: numPlanes,
		radius:    1,
		lat:       30.3119,
		lon:       -95.4561,
	}

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGTERM, os.Interrupt)

	s, err := newServer("0.0.0.0:30003", conf)
	if err != nil {
		panic(err.Error())
	}

	<-shutdown

	s.shutdown()
}

type server struct {
	lis       net.Listener
	mu        sync.Mutex
	planes    map[string]*Plane
	ctx       context.Context
	ctxCancel context.CancelFunc
	closed    bool
}

type sConfig struct {
	numPlanes int
	lat       float64
	lon       float64
	radius    float64
}

// new server
func newServer(addr string, conf sConfig) (*server, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	log.Println("mock adsb listening at ", lis.Addr().String())

	ctx, cancel := context.WithCancel(context.Background())

	s := &server{
		planes:    make(map[string]*Plane),
		lis:       lis,
		ctx:       ctx,
		ctxCancel: cancel,
	}

	for range conf.numPlanes {
		s.newPlane(conf.lat, conf.lon, conf.radius)
	}

	go s.listen()

	return s, nil
}

func (s *server) listen() {
	log.Println("mock adsb serving")
	for {
		conn, err := s.lis.Accept()
		if errors.Is(err, net.ErrClosed) {
			log.Println("server closed")
			return
		}

		if err != nil {
			log.Println(err.Error())
			return
		}

		s.mu.Lock()
		if s.closed {
			defer s.mu.Unlock()
			conn.Close()
			return
		}
		log.Printf("accepted connection from %s\n", conn.RemoteAddr().String())

		for _, plane := range s.planes {
			go s.handleConn(plane, conn)
		}

		s.mu.Unlock()
	}
}

func (s *server) newPlane(lat, lon, rad float64) *Plane {
	plat, plon := randomLatLonNear(lat, lon, rad)
	p := &Plane{
		ICAO: randomICAO24(),
		ALT:  randomAltitude(),
		HD:   randomHeading(),
		LAT:  plat,
		LON:  plon,
	}

	go p.fly(s.ctx, lat, lon, rad)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.planes[p.ICAO] = p

	return p
}

func (s *server) handleConn(plane *Plane, c net.Conn) {
	defer c.Close()

	t := time.NewTicker(time.Second * 2)
	defer t.Stop()

	log.Printf("forwarding sbs from plane %s to connection at %s\n", plane.ICAO, c.RemoteAddr().String())
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-t.C:
			err := c.SetWriteDeadline(time.Now().Add(time.Second))
			if err != nil {
				log.Println(err)
				return
			}

			_, err = c.Write([]byte(plane.sbs()))
			if err != nil {
				log.Println("bad write", err.Error())
				return
			}
		}
	}
}

func (s *server) shutdown() error {
	s.mu.Lock()

	if s.closed {
		return nil
	}
	s.closed = true
	println("closed")
	s.mu.Unlock()

	s.ctxCancel()
	return s.lis.Close()
}

type Plane struct {
	ICAO string
	LAT  float64
	LON  float64
	ALT  int
	HD   float64
	mu   sync.RWMutex
}

func (p *Plane) tick(centerLat, centerLon, radiusDeg float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	const speedDegPerTick = 0.001
	rad := p.HD * (math.Pi / 180)
	newLat := p.LAT + speedDegPerTick*math.Cos(rad)
	newLon := p.LON + speedDegPerTick*math.Sin(rad)

	dLat := newLat - centerLat
	dLon := newLon - centerLon
	if dLat*dLat+dLon*dLon > radiusDeg*radiusDeg {
		// reflect heading ~180 degrees with some jitter so it doesn't get stuck
		p.HD = math.Mod(p.HD+180+rand.Float64()*40-20, 360)
		rad = p.HD * (math.Pi / 180)
		newLat = p.LAT + speedDegPerTick*math.Cos(rad)
		newLon = p.LON + speedDegPerTick*math.Sin(rad)
	}

	p.LAT = newLat
	p.LON = newLon

}

func (p *Plane) fly(ctx context.Context, centerLat, centerLon, rad float64) {
	t := time.NewTicker(time.Second * 2)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.tick(centerLat, centerLon, rad)
		}
	}
}

func (p *Plane) sbs() string {
	now := time.Now()
	dateStr := now.Format("2006/01/02")
	timeStr := now.Format("15:04:05.000")

	return fmt.Sprintf(
		"MSG,3,1,1,%s,1,%s,%s,%s,%s,,%d,,,%.5f,%.5f,,,,,,,\r\n",
		p.ICAO, dateStr, timeStr, dateStr, timeStr,
		p.ALT, p.LAT, p.LON,
	)
}

func randomICAO24() string {
	return fmt.Sprintf("%06X", rand.Intn(1<<24))
}

func randomAltitude() int {
	return rand.Intn(20000) + 1000
}

func randomHeading() float64 {
	return rand.Float64() * 360
}

func randomLatLonNear(centerLat, centerLon, radiusDeg float64) (float64, float64) {
	lat := centerLat + (rand.Float64()*2-1)*radiusDeg
	lon := centerLon + (rand.Float64()*2-1)*radiusDeg
	return lat, lon
}
