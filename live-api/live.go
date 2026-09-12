package live_api

import (
	"context"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{} // use default options
type LiveApiHandler struct {
	hub LiveApiHub
}

func NewLiveApiHandler() *LiveApiHandler {
	return &LiveApiHandler{
		hub: *NewLiveApiHub(),
	}
}

func (a *LiveApiHandler) HandleWS(w http.ResponseWriter, r *http.Request) {
	a.hub.mu.RLock()
	closed := a.hub.closed
	a.hub.mu.RUnlock()

	if closed {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return
	}

	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	client := a.hub.addClient(ws)

	go client.readLoop()
	go client.sendLoop()
}

func (a *LiveApiHandler) Broadcast(data []byte) {
	a.hub.broadcast(data)
}

func (a *LiveApiHandler) Close(ctx context.Context) error {
	return a.hub.close(ctx)
}

type LiveApiHub struct {
	hub    map[chan []byte]*Client
	mu     sync.RWMutex
	closed bool
}

func NewLiveApiHub() *LiveApiHub {
	api := &LiveApiHub{
		hub: make(map[chan []byte]*Client),
	}

	return api
}

func (a *LiveApiHub) addClient(w *websocket.Conn) *Client {
	ch := make(chan []byte, 128)
	client := &Client{
		sendC: ch,
		conn:  w,
	}

	client.deregister = sync.OnceFunc(func() { a.deregisterClient(client) })

	a.mu.Lock()
	defer a.mu.Unlock()
	a.hub[ch] = client

	return client
}

func (a *LiveApiHub) deregisterClient(client *Client) {
	a.mu.Lock()
	defer a.mu.Unlock()

	delete(a.hub, client.sendC)
	close(client.sendC)
	client.conn.Close()
}

func (a *LiveApiHub) broadcast(data []byte) {
	a.mu.RLock()
	for c, client := range a.hub {
		select {
		case c <- data:
		default:
			defer client.deregister()
		}
	}
	a.mu.RUnlock()
}

func (a *LiveApiHub) close(ctx context.Context) error {
	a.mu.Lock()
	a.closed = true
	clients := make([]*Client, 0, len(a.hub))
	for _, c := range a.hub {
		clients = append(clients, c)
	}
	a.mu.Unlock()

	var wg sync.WaitGroup
	for _, c := range clients {
		wg.Add(1)
		go func(c *Client) {
			defer wg.Done()
			c.conn.SetWriteDeadline(time.Now().Add(time.Second))
			c.conn.WriteControl(
				websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseGoingAway, "server shutting down"),
				time.Now().Add(time.Second),
			)
			c.deregister()
		}(c)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type Client struct {
	conn       *websocket.Conn
	sendC      chan []byte
	deregister func()
}

func (c *Client) sendLoop() {
	defer c.deregister()
	for msg := range c.sendC {
		err := c.conn.WriteMessage(websocket.TextMessage, msg)
		if err != nil {
			log.Println(err)
			break
		}
	}
}

func (c *Client) readLoop() {
	defer c.deregister()
	for {
		_, _, err := c.conn.ReadMessage()
		if err != nil {
			log.Print(err)
			break
		}
	}
}
