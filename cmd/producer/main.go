// Demo producer: publishes -value once, or one record per stdin line.
//
//	spool-producer -stream orders -key order-123 -value order.created
//	seq 1 100 | spool-producer -stream orders
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
)

func main() {
	addr := flag.String("addr", "http://localhost:8080", "broker address")
	stream := flag.String("stream", "", "stream name (required)")
	key := flag.String("key", "", "optional record key")
	value := flag.String("value", "", "record value; if empty, reads lines from stdin")
	flag.Parse()
	if *stream == "" {
		log.Fatal("producer: -stream is required")
	}

	if *value != "" {
		publish(*addr, *stream, *key, *value)
		return
	}
	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		if line := sc.Text(); line != "" {
			publish(*addr, *stream, *key, line)
		}
	}
	if err := sc.Err(); err != nil {
		log.Fatalf("producer: %v", err)
	}
}

func publish(addr, stream, key, value string) {
	body, _ := json.Marshal(map[string]string{"key": key, "value": value})
	resp, err := http.Post(fmt.Sprintf("%s/streams/%s/records", addr, stream),
		"application/json", bytes.NewReader(body))
	if err != nil {
		log.Fatalf("producer: %v", err)
	}
	defer resp.Body.Close()
	var out struct {
		Partition int    `json:"partition"`
		Offset    int64  `json:"offset"`
		Error     string `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		log.Fatalf("producer: decode response: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		log.Fatalf("producer: %s: %s", resp.Status, out.Error)
	}
	fmt.Printf("published partition=%d offset=%d\n", out.Partition, out.Offset)
}
