package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/anthonychiu/spool/internal/broker"
	spoollog "github.com/anthonychiu/spool/internal/log"
)

func newServer(t *testing.T) (*httptest.Server, string) {
	t.Helper()
	dir := t.TempDir()
	b, err := broker.Open(broker.Options{DataDir: dir, SyncEveryAppend: true})
	if err != nil {
		t.Fatalf("broker.Open: %v", err)
	}
	t.Cleanup(func() { b.Close() })
	ts := httptest.NewServer(New(b))
	t.Cleanup(ts.Close)
	return ts, dir
}

func publish(t *testing.T, ts *httptest.Server, stream, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(ts.URL+"/streams/"+stream+"/records", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	return resp
}

func get(t *testing.T, url string) *http.Response {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	return resp
}

func decode(t *testing.T, resp *http.Response, v any) {
	t.Helper()
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		t.Fatalf("decode: %v", err)
	}
}

func TestPublishAndRead(t *testing.T) {
	ts, _ := newServer(t)
	for i := 0; i < 3; i++ {
		resp := publish(t, ts, "orders", fmt.Sprintf(`{"key":"k","value":"v%d"}`, i))
		var out struct {
			Partition int   `json:"partition"`
			Offset    int64 `json:"offset"`
		}
		decode(t, resp, &out)
		if resp.StatusCode != 200 || out.Offset != int64(i) || out.Partition != 0 {
			t.Fatalf("publish %d: status=%d out=%+v", i, resp.StatusCode, out)
		}
	}

	resp := get(t, ts.URL+"/streams/orders/partitions/0/records?offset=1&limit=100")
	var out struct {
		Records []struct {
			Offset int64  `json:"offset"`
			Value  string `json:"value"`
		} `json:"records"`
		NextOffset int64 `json:"next_offset"`
	}
	decode(t, resp, &out)
	if len(out.Records) != 2 || out.Records[0].Value != "v1" || out.NextOffset != 3 {
		t.Fatalf("read = %+v", out)
	}
}

func TestReadPastEnd(t *testing.T) {
	ts, _ := newServer(t)
	publish(t, ts, "orders", `{"value":"v0"}`).Body.Close()
	resp := get(t, ts.URL+"/streams/orders/partitions/0/records?offset=99")
	var out struct {
		Records    []any `json:"records"`
		NextOffset int64 `json:"next_offset"`
	}
	decode(t, resp, &out)
	if resp.StatusCode != 200 || len(out.Records) != 0 || out.NextOffset != 99 {
		t.Fatalf("read past end: status=%d out=%+v", resp.StatusCode, out)
	}
}

func TestOffsetCommitRoundTrip(t *testing.T) {
	ts, _ := newServer(t)
	publish(t, ts, "orders", `{"value":"v0"}`).Body.Close()

	url := ts.URL + "/groups/analytics/streams/orders/partitions/0/offset"
	resp := get(t, url)
	var got struct {
		NextOffset int64 `json:"next_offset"`
	}
	decode(t, resp, &got)
	if got.NextOffset != 0 {
		t.Fatalf("fresh group next_offset = %d, want 0", got.NextOffset)
	}

	req, _ := http.NewRequest(http.MethodPut, url, strings.NewReader(`{"next_offset":43}`))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT offset: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("PUT offset: status=%d", resp.StatusCode)
	}

	resp = get(t, url)
	decode(t, resp, &got)
	if got.NextOffset != 43 {
		t.Fatalf("next_offset = %d, want 43", got.NextOffset)
	}
}

func TestRejectInvalidStreamName(t *testing.T) {
	ts, _ := newServer(t)
	resp := publish(t, ts, "..%2Fetc", `{"value":"v"}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestRejectMalformedBody(t *testing.T) {
	ts, _ := newServer(t)
	for _, body := range []string{"", "{", `{"key":"k"}`} {
		resp := publish(t, ts, "orders", body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("body %q: status = %d, want 400", body, resp.StatusCode)
		}
	}
}

func TestRejectOversizedRecord(t *testing.T) {
	ts, _ := newServer(t)
	big := strings.Repeat("x", spoollog.MaxRecordBytes+1)
	resp := publish(t, ts, "orders", `{"value":"`+big+`"}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge && resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 413 or 400", resp.StatusCode)
	}
}

func TestReadUnknownStream(t *testing.T) {
	ts, _ := newServer(t)
	resp := get(t, ts.URL+"/streams/ghost/partitions/0/records?offset=0")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

func TestRejectBadQueryParams(t *testing.T) {
	ts, _ := newServer(t)
	publish(t, ts, "orders", `{"value":"v"}`).Body.Close()
	for _, q := range []string{"offset=-1", "offset=abc", "offset=0&limit=0", "offset=0&limit=5000"} {
		resp := get(t, ts.URL+"/streams/orders/partitions/0/records?"+q)
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("query %q: status = %d, want 400", q, resp.StatusCode)
		}
	}
}

func TestHealthEndpoints(t *testing.T) {
	ts, _ := newServer(t)
	for _, p := range []string{"/health/live", "/health/ready"} {
		resp := get(t, ts.URL+p)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("%s: status = %d", p, resp.StatusCode)
		}
	}
}
