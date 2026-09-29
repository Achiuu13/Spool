// Broker executable: serves the Spool HTTP API over a local data directory.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Achiuu13/spool/internal/api"
	"github.com/Achiuu13/spool/internal/broker"
	"github.com/Achiuu13/spool/internal/groups"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	dataDir := flag.String("data", "data", "data directory")
	sync := flag.Bool("sync", true, "fsync each append before acknowledging (durable mode)")
	partitions := flag.Int("partitions", 3, "partition count for newly created streams")
	sessionTimeout := flag.Duration("session-timeout", 10*time.Second, "consumer-group session timeout")
	flag.Parse()

	b, err := broker.Open(broker.Options{
		DataDir:           *dataDir,
		SyncEveryAppend:   *sync,
		DefaultPartitions: *partitions,
		Groups:            groups.Options{SessionTimeout: *sessionTimeout},
	})
	if err != nil {
		log.Fatalf("spool: %v", err)
	}

	srv := &http.Server{
		Addr:         *addr,
		Handler:      api.New(b),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Printf("spool: listening on %s (data=%s sync=%v)", *addr, *dataDir, *sync)
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
	if err := b.Close(); err != nil {
		log.Printf("spool: close: %v", err)
	}
}
