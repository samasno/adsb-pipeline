package main

import (
	"bufio"
	"errors"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	c, err := net.Dial("tcp", "127.0.0.1:30003")
	if err != nil {
		panic(err)
	}
	defer c.Close()

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	r := bufio.NewReader(c)
	scanner := bufio.NewScanner(r)

	for {
		select {
		case <-shutdown:
			return
		default:
			if scanner.Scan() {
				msg := scanner.Text()
				log.Println(msg)
			}

			err := scanner.Err()
			if errors.Is(err, io.EOF) {
				log.Println("scan complete")
				return
			}

			if err != nil {
				log.Fatal(err.Error())
			}
		}
	}
}
