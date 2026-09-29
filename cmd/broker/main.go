// Broker executable: serves the Spool HTTP API over a local data directory.
// With -peers it runs as one node of a replicated cluster; without, it is a
// standalone single-node broker.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/Achiuu13/spool/internal/api"
	"github.com/Achiuu13/spool/internal/broker"
	"github.com/Achiuu13/spool/internal/groups"
	"github.com/Achiuu13/spool/internal/replication"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	dataDir := flag.String("data", "data", "data directory")
	sync := flag.Bool("sync", true, "fsync each append before acknowledging (durable mode)")
	partitions := flag.Int("partitions", 3, "partition count for newly created streams")
	sessionTimeout := flag.Duration("session-timeout", 10*time.Second, "consumer-group session timeout")
	nodeID := flag.String("node-id", "", "cluster node name (cluster mode)")
	peers := flag.String("peers", "", "cluster members as id=url,... including this node (enables cluster mode)")
	advertise := flag.String("advertise", "", "this node's client-facing base URL (defaults to its -peers entry)")
	electionTimeout := flag.Duration("election-timeout", 300*time.Millisecond, "leader election timeout base")
	heartbeat := flag.Duration("heartbeat-interval", 75*time.Millisecond, "leader replication interval")
	flag.Parse()

	if *peers == "" {
		runStandalone(*addr, *dataDir, *sync, *partitions, *sessionTimeout)
		return
	}

	peerMap, selfURL, err := parsePeers(*peers, *nodeID)
	if err != nil {
		log.Fatalf("spool: %v", err)
	}
	if *advertise == "" {
		*advertise = selfURL
	}

	// Derived state is rebuilt from the replicated operation log as entries
	// commit, so it starts empty on every boot. The oplog under raft/ is the
	// durable source of truth.
	stateDir := filepath.Join(*dataDir, "state")
	if err := os.RemoveAll(stateDir); err != nil {
		log.Fatalf("spool: reset derived state: %v", err)
	}
	b, err := broker.Open(broker.Options{
		DataDir:           stateDir,
		SyncEveryAppend:   false, // durability comes from the fsynced oplog
		DefaultPartitions: *partitions,
		Groups:            groups.Options{SessionTimeout: *sessionTimeout},
	})
	if err != nil {
		log.Fatalf("spool: %v", err)
	}

	node, err := replication.Open(replication.Config{
		ID:                *nodeID,
		Peers:             peerMap,
		Advertise:         *advertise,
		Dir:               filepath.Join(*dataDir, "raft"),
		ElectionTimeout:   *electionTimeout,
		HeartbeatInterval: *heartbeat,
		Apply:             b.ApplyOp,
	})
	if err != nil {
		log.Fatalf("spool: %v", err)
	}
	node.Start()

	mux := http.NewServeMux()
	mux.Handle("/raft/", node.Handler())
	mux.Handle("/", api.NewReplicated(b, node))
	serve(*addr, mux, func() {
		node.Stop()
		if err := b.Close(); err != nil {
			log.Printf("spool: close: %v", err)
		}
	})
}

func runStandalone(addr, dataDir string, sync bool, partitions int, sessionTimeout time.Duration) {
	b, err := broker.Open(broker.Options{
		DataDir:           dataDir,
		SyncEveryAppend:   sync,
		DefaultPartitions: partitions,
		Groups:            groups.Options{SessionTimeout: sessionTimeout},
	})
	if err != nil {
		log.Fatalf("spool: %v", err)
	}
	serve(addr, api.New(b), func() {
		if err := b.Close(); err != nil {
			log.Printf("spool: close: %v", err)
		}
	})
}

func serve(addr string, handler http.Handler, cleanup func()) {
	srv := &http.Server{
		Addr:         addr,
		Handler:      handler,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	go func() {
		log.Printf("spool: listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("spool: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	srv.Shutdown(ctx)
	cleanup()
}

// parsePeers splits "n1=http://a,n2=http://b,n3=http://c" into the peer map
// (excluding self) and this node's own URL.
func parsePeers(spec, self string) (map[string]string, string, error) {
	if self == "" {
		return nil, "", fmt.Errorf("-node-id is required with -peers")
	}
	peers := map[string]string{}
	selfURL := ""
	for _, part := range strings.Split(spec, ",") {
		id, url, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || id == "" || url == "" {
			return nil, "", fmt.Errorf("invalid -peers entry %q (want id=url)", part)
		}
		if id == self {
			selfURL = url
		} else {
			peers[id] = url
		}
	}
	if selfURL == "" {
		return nil, "", fmt.Errorf("-peers must include this node (%s)", self)
	}
	return peers, selfURL, nil
}
