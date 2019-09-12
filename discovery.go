package meta

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/go-cleanhttp"

	"google.golang.org/grpc/naming"
)

var discoveryClient = initDiscoveryClient()

func initDiscoveryClient() *http.Client {
	addr := os.Getenv("META_SOCKET")
	if addr == "" {
		addr = "/run/meta.sock"
	}
	transport := cleanhttp.DefaultTransport()
	transport.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
		return new(net.Dialer).DialContext(ctx, "unix", addr)
	}
	return &http.Client{Transport: transport}
}

func UnmarshalConfig(value interface{}) error {
	var body io.Reader
	if cfg := os.Getenv("META_CONFIG"); cfg != "" {
		body = strings.NewReader(cfg)
	} else {
		resp, err := discoveryClient.Get("http://meta/v1/config")
		if err == nil && resp.StatusCode != http.StatusOK {
			message, _ := ioutil.ReadAll(resp.Body)
			err = fmt.Errorf("%s: %s", resp.Status, string(message))
		}
		if err != nil {
			return fmt.Errorf("lib/service: config error: %s", err)
		}
		body = resp.Body
		defer resp.Body.Close()
	}
	if err := json.NewDecoder(body).Decode(value); err != nil {
		return fmt.Errorf("lib/service: couldn't decode config response: %s", err)
	}
	return nil
}

func Resolver() naming.Resolver {
	return metaResolver{}
}

type metaResolver struct{}

func (metaResolver) Resolve(service string) (naming.Watcher, error) {
	return &metaWatcher{
		service:  service,
		addrs:    make(map[string]struct{}),
		cancelCh: make(chan struct{}),
	}, nil
}

type metaWatcher struct {
	service string
	index   uint64
	addrs   map[string]struct{}

	cancelMu sync.Mutex
	cancelCh chan struct{}
}

func (w *metaWatcher) Next() ([]*naming.Update, error) {
	log.Printf("lib/service: resolving %s (index %d), %d existing addresses", w.service, w.index, len(w.addrs))
	if w.addrs == nil {
		// don't retry the first Next
		return w.next()
	}
	var retryCount uint
retry:
	updates, err := w.next()
	if err != nil && retryCount < 6 {
		log.Printf("lib/service: retrying %s: %s", w.service, err)
		time.Sleep(time.Second << retryCount)
		retryCount += 1
		goto retry
	}
	return updates, err
}

func (w *metaWatcher) next() ([]*naming.Update, error) {
	resp, err := discoveryClient.Get(fmt.Sprintf("http://meta/v1/services/%s?index=%d", url.QueryEscape(w.service), w.index))
	if err == nil && resp.StatusCode != http.StatusOK {
		message, _ := ioutil.ReadAll(resp.Body)
		err = fmt.Errorf("%s: %s", resp.Status, string(message))
	}
	if err != nil {
		return nil, fmt.Errorf("lib/service: discovery error: %s", err)
	}
	var addrs map[string]struct{}
	if err := json.NewDecoder(resp.Body).Decode(&addrs); err != nil {
		return nil, fmt.Errorf("lib/service: couldn't decode discovery response: %s", err)
	}
	var updates []*naming.Update
	for addr := range w.addrs {
		if _, exists := addrs[addr]; !exists {
			updates = append(updates, &naming.Update{Op: naming.Delete, Addr: addr})
		}
	}
	for addr := range addrs {
		if _, exists := w.addrs[addr]; !exists {
			updates = append(updates, &naming.Update{Op: naming.Add, Addr: addr})
		}
	}
	w.addrs = addrs
	w.index, _ = strconv.ParseUint(resp.Header.Get("X-Index"), 10, 64)
	return updates, nil
}

func (w *metaWatcher) Close() {
	w.cancelMu.Lock()
	defer w.cancelMu.Unlock()
	select {
	default:
		close(w.cancelCh)
	case <-w.cancelCh:
	}
}
