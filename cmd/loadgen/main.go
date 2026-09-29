// Loadgen publishes records from concurrent producers and reports
// throughput, publish latency percentiles, failures, and (with -verify)
// whether every acknowledged offset is readable with the expected content.
//
//	spool-loadgen -stream bench -producers 8 -records 2500 -size 256 -verify
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
)

type ack struct {
	partition int
	offset    int64
}

type result struct {
	ack     ack
	value   string
	latency time.Duration
	err     error
}

func main() {
	addr := flag.String("addr", "http://localhost:8080", "broker address")
	stream := flag.String("stream", "bench", "stream name")
	producers := flag.Int("producers", 4, "concurrent producers")
	records := flag.Int("records", 1000, "records per producer")
	size := flag.Int("size", 256, "value size in bytes")
	verify := flag.Bool("verify", false, "read back and verify every acknowledged offset")
	flag.Parse()

	client := &http.Client{Timeout: 30 * time.Second}
	payload := strings.Repeat("x", *size)
	total := *producers * *records
	results := make([]result, total)

	start := time.Now()
	var wg sync.WaitGroup
	for p := 0; p < *producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < *records; i++ {
				value := fmt.Sprintf("%d-%d-%s", p, i, payload)
				body, _ := json.Marshal(map[string]string{"value": value})
				t0 := time.Now()
				a, err := publish(client, *addr, *stream, body)
				results[p**records+i] = result{a, value, time.Since(t0), err}
			}
		}(p)
	}
	wg.Wait()
	elapsed := time.Since(start)

	var latencies []time.Duration
	failed := 0
	acked := map[ack]string{}
	for _, r := range results {
		if r.err != nil {
			failed++
			continue
		}
		if _, dup := acked[r.ack]; dup {
			log.Fatalf("FAIL: duplicate acknowledged offset %d on partition %d", r.ack.offset, r.ack.partition)
		}
		acked[r.ack] = r.value
		latencies = append(latencies, r.latency)
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	fmt.Printf("environment: %s/%s, %d CPUs, go %s\n", runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), runtime.Version())
	fmt.Printf("workload: %d producers x %d records, %d-byte values, stream=%s\n", *producers, *records, *size, *stream)
	fmt.Printf("elapsed: %v\n", elapsed.Round(time.Millisecond))
	fmt.Printf("throughput: %.0f records/s\n", float64(len(latencies))/elapsed.Seconds())
	if len(latencies) > 0 {
		fmt.Printf("publish latency: p50=%v p95=%v p99=%v\n",
			pct(latencies, 50).Round(time.Microsecond),
			pct(latencies, 95).Round(time.Microsecond),
			pct(latencies, 99).Round(time.Microsecond))
	}
	fmt.Printf("failed requests: %d/%d\n", failed, total)

	if *verify {
		missing := verifyAcked(client, *addr, *stream, acked)
		fmt.Printf("verified: %d acknowledged offsets readable, %d missing\n", len(acked)-missing, missing)
		if missing > 0 {
			os.Exit(1)
		}
	}
	if failed > 0 {
		os.Exit(1)
	}
}

func publish(client *http.Client, addr, stream string, body []byte) (ack, error) {
	resp, err := client.Post(addr+"/streams/"+stream+"/records", "application/json", bytes.NewReader(body))
	if err != nil {
		return ack{}, err
	}
	defer resp.Body.Close()
	var out struct {
		Partition int    `json:"partition"`
		Offset    int64  `json:"offset"`
		Error     string `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return ack{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return ack{}, fmt.Errorf("status %d: %s", resp.StatusCode, out.Error)
	}
	return ack{out.Partition, out.Offset}, nil
}

func pct(sorted []time.Duration, p int) time.Duration {
	idx := p * len(sorted) / 100
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func verifyAcked(client *http.Client, addr, stream string, acked map[ack]string) int {
	partitions := streamPartitions(client, addr, stream)
	got := map[ack]string{}
	for p := 0; p < partitions; p++ {
		offset := int64(0)
		for {
			url := fmt.Sprintf("%s/streams/%s/partitions/%d/records?offset=%d&limit=1000", addr, stream, p, offset)
			resp, err := client.Get(url)
			if err != nil {
				log.Fatalf("verify read: %v", err)
			}
			var out struct {
				Records []struct {
					Offset int64  `json:"offset"`
					Value  string `json:"value"`
				} `json:"records"`
				NextOffset int64 `json:"next_offset"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
				log.Fatalf("verify decode: %v", err)
			}
			resp.Body.Close()
			if len(out.Records) == 0 {
				break
			}
			for _, r := range out.Records {
				got[ack{p, r.Offset}] = r.Value
			}
			offset = out.NextOffset
		}
	}
	missing := 0
	for a, want := range acked {
		if got[a] != want {
			missing++
		}
	}
	return missing
}

func streamPartitions(client *http.Client, addr, stream string) int {
	resp, err := client.Get(addr + "/streams/" + stream)
	if err != nil {
		log.Fatalf("stream meta: %v", err)
	}
	defer resp.Body.Close()
	var out struct {
		Partitions int `json:"partitions"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil || out.Partitions < 1 {
		log.Fatalf("stream meta: %v (partitions=%d)", err, out.Partitions)
	}
	return out.Partitions
}
