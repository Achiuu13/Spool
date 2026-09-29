// Demo consumer: polls a partition, prints records, and commits progress.
//
// Follows the at-least-once order: read -> process -> commit next offset.
// If it crashes after processing but before committing, the record is
// delivered again on restart, so processing must tolerate duplicates.
//
//	spool-consumer -stream orders -group analytics
//	spool-consumer -stream orders -group analytics -from-beginning
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"
)

func main() {
	addr := flag.String("addr", "http://localhost:8080", "broker address")
	stream := flag.String("stream", "", "stream name (required)")
	group := flag.String("group", "", "consumer group (required)")
	partition := flag.Int("partition", 0, "partition")
	fromBeginning := flag.Bool("from-beginning", false, "replay from offset 0, ignoring committed progress")
	once := flag.Bool("once", false, "drain available records and exit instead of polling forever")
	interval := flag.Duration("poll", time.Second, "poll interval when caught up")
	flag.Parse()
	if *stream == "" || *group == "" {
		log.Fatal("consumer: -stream and -group are required")
	}

	base := fmt.Sprintf("%s/groups/%s/streams/%s/partitions/%d/offset", *addr, *group, *stream, *partition)
	next := int64(0)
	if !*fromBeginning {
		next = fetchCommitted(base)
	}
	log.Printf("consumer: group=%s starting at offset %d", *group, next)

	for {
		records := fetch(*addr, *stream, *partition, next)
		if len(records) == 0 {
			if *once {
				return
			}
			time.Sleep(*interval)
			continue
		}
		for _, r := range records {
			// "Processing" is printing; a real consumer does its work here.
			fmt.Printf("offset=%d key=%q value=%q\n", r.Offset, r.Key, r.Value)
			next = r.Offset + 1
			commit(base, next)
		}
	}
}

type record struct {
	Offset int64  `json:"offset"`
	Key    string `json:"key"`
	Value  string `json:"value"`
}

func fetchCommitted(base string) int64 {
	resp, err := http.Get(base)
	if err != nil {
		log.Fatalf("consumer: %v", err)
	}
	defer resp.Body.Close()
	var out struct {
		NextOffset int64  `json:"next_offset"`
		Error      string `json:"error"`
	}
	json.NewDecoder(resp.Body).Decode(&out)
	if resp.StatusCode != http.StatusOK {
		log.Fatalf("consumer: get offset: %s: %s", resp.Status, out.Error)
	}
	return out.NextOffset
}

func fetch(addr, stream string, partition int, offset int64) []record {
	url := fmt.Sprintf("%s/streams/%s/partitions/%d/records?offset=%d&limit=100",
		addr, stream, partition, offset)
	resp, err := http.Get(url)
	if err != nil {
		log.Fatalf("consumer: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil // stream not created yet
	}
	var out struct {
		Records []record `json:"records"`
		Error   string   `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		log.Fatalf("consumer: decode: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		log.Fatalf("consumer: read: %s: %s", resp.Status, out.Error)
	}
	return out.Records
}

func commit(base string, next int64) {
	body, _ := json.Marshal(map[string]int64{"next_offset": next})
	req, _ := http.NewRequest(http.MethodPut, base, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatalf("consumer: commit: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.Fatalf("consumer: commit: %s", resp.Status)
	}
}
