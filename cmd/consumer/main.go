// Demo consumer: joins a consumer group, heartbeats, polls its assigned
// partitions, prints records, and commits fenced progress per partition.
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
	"os"
	"os/signal"
	"slices"
	"sync/atomic"
	"syscall"
	"time"
)

type assignment struct {
	MemberID   string `json:"member_id"`
	Generation int64  `json:"generation"`
	Partitions []int  `json:"partitions"`
	Error      string `json:"error"`
}

type client struct {
	addr, stream, group string
	http                *http.Client
}

func main() {
	addr := flag.String("addr", "http://localhost:8080", "broker address")
	stream := flag.String("stream", "", "stream name (required)")
	group := flag.String("group", "", "consumer group (required)")
	fromBeginning := flag.Bool("from-beginning", false, "replay from offset 0, ignoring committed progress")
	once := flag.Bool("once", false, "drain available records and exit instead of polling forever")
	interval := flag.Duration("poll", time.Second, "poll interval when caught up")
	heartbeatEvery := flag.Duration("heartbeat", 3*time.Second, "heartbeat interval")
	flag.Parse()
	if *stream == "" || *group == "" {
		log.Fatal("consumer: -stream and -group are required")
	}

	c := &client{addr: *addr, stream: *stream, group: *group, http: &http.Client{Timeout: 10 * time.Second}}

	a := c.join()
	log.Printf("consumer: member=%s generation=%d partitions=%v", a.MemberID, a.Generation, a.Partitions)

	var current atomic.Pointer[assignment]
	current.Store(&a)
	defer c.leave(current.Load().MemberID)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		for range time.Tick(*heartbeatEvery) {
			hb, err := c.heartbeat(current.Load().MemberID)
			if err != nil {
				log.Printf("consumer: session expired, rejoining: %v", err)
				hb = c.join()
			}
			if prev := current.Load(); hb.Generation != prev.Generation {
				log.Printf("consumer: rebalanced, generation=%d partitions=%v", hb.Generation, hb.Partitions)
			}
			current.Store(&hb)
		}
	}()

	next := map[int]int64{}
	for {
		select {
		case <-stop:
			return
		default:
		}
		a := *current.Load()
		progressed := false
		for _, p := range a.Partitions {
			if _, ok := next[p]; !ok {
				if *fromBeginning {
					next[p] = 0
				} else {
					next[p] = c.committed(p)
				}
			}
			records := c.fetch(p, next[p])
			for _, r := range records {
				fmt.Printf("partition=%d offset=%d key=%q value=%q\n", p, r.Offset, r.Key, r.Value)
				next[p] = r.Offset + 1
				if err := c.commit(p, next[p], a.MemberID); err != nil {
					// Fenced: we no longer own this partition. Drop local
					// state; the new owner resumes from the last commit.
					log.Printf("consumer: commit fenced on partition %d: %v", p, err)
					delete(next, p)
					break
				}
				progressed = true
			}
		}
		// Forget partitions we no longer own so a future reassignment
		// re-reads the committed offset.
		for p := range next {
			if !slices.Contains(a.Partitions, p) {
				delete(next, p)
			}
		}
		if !progressed {
			if *once {
				return
			}
			select {
			case <-stop:
				return
			case <-time.After(*interval):
			}
		}
	}
}

func (c *client) join() assignment {
	url := fmt.Sprintf("%s/groups/%s/streams/%s/members", c.addr, c.group, c.stream)
	resp, err := c.http.Post(url, "application/json", nil)
	if err != nil {
		log.Fatalf("consumer: join: %v", err)
	}
	defer resp.Body.Close()
	var a assignment
	json.NewDecoder(resp.Body).Decode(&a)
	if resp.StatusCode != http.StatusOK {
		log.Fatalf("consumer: join: %s: %s", resp.Status, a.Error)
	}
	return a
}

func (c *client) heartbeat(memberID string) (assignment, error) {
	url := fmt.Sprintf("%s/groups/%s/streams/%s/members/%s/heartbeat", c.addr, c.group, c.stream, memberID)
	req, _ := http.NewRequest(http.MethodPut, url, nil)
	resp, err := c.http.Do(req)
	if err != nil {
		return assignment{}, err
	}
	defer resp.Body.Close()
	var a assignment
	json.NewDecoder(resp.Body).Decode(&a)
	if resp.StatusCode != http.StatusOK {
		return assignment{}, fmt.Errorf("%s: %s", resp.Status, a.Error)
	}
	return a, nil
}

func (c *client) leave(memberID string) {
	url := fmt.Sprintf("%s/groups/%s/streams/%s/members/%s", c.addr, c.group, c.stream, memberID)
	req, _ := http.NewRequest(http.MethodDelete, url, nil)
	if resp, err := c.http.Do(req); err == nil {
		resp.Body.Close()
	}
}

func (c *client) committed(partition int) int64 {
	url := fmt.Sprintf("%s/groups/%s/streams/%s/partitions/%d/offset", c.addr, c.group, c.stream, partition)
	resp, err := c.http.Get(url)
	if err != nil {
		log.Fatalf("consumer: %v", err)
	}
	defer resp.Body.Close()
	var out struct {
		NextOffset int64 `json:"next_offset"`
	}
	json.NewDecoder(resp.Body).Decode(&out)
	return out.NextOffset
}

type record struct {
	Offset int64  `json:"offset"`
	Key    string `json:"key"`
	Value  string `json:"value"`
}

func (c *client) fetch(partition int, offset int64) []record {
	url := fmt.Sprintf("%s/streams/%s/partitions/%d/records?offset=%d&limit=100",
		c.addr, c.stream, partition, offset)
	resp, err := c.http.Get(url)
	if err != nil {
		log.Fatalf("consumer: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil
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

func (c *client) commit(partition int, next int64, memberID string) error {
	url := fmt.Sprintf("%s/groups/%s/streams/%s/partitions/%d/offset", c.addr, c.group, c.stream, partition)
	body, _ := json.Marshal(map[string]any{"next_offset": next, "member_id": memberID})
	req, _ := http.NewRequest(http.MethodPut, url, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		log.Fatalf("consumer: commit: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var out struct {
			Error string `json:"error"`
		}
		json.NewDecoder(resp.Body).Decode(&out)
		return fmt.Errorf("%s: %s", resp.Status, out.Error)
	}
	return nil
}
