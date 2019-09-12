package meta

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/http"

	"github.com/hashicorp/go-cleanhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/naming"
)

func WithResolver(r naming.Resolver) grpc.DialOption {
	return grpc.WithBalancer(grpc.RoundRobin(r))
}

func Dial(service string, opts ...grpc.DialOption) *grpc.ClientConn {
	opts = append([]grpc.DialOption{grpc.WithInsecure(), WithResolver(Resolver())}, opts...)
	cc, err := grpc.Dial(service, opts...)
	if err != nil {
		panic(fmt.Errorf("couldn't dial service %q: %s", service, err))
	}
	return cc
}

func Transport() http.RoundTripper {
	transport := cleanhttp.DefaultPooledTransport()
	transport.Dial = dialRaw
	return transport
}

func dialRaw(network, address string) (net.Conn, error) {
	service, _, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	resp, err := discoveryClient.Get("http://meta/v1/services/" + service)
	if err == nil && resp.StatusCode != http.StatusOK {
		err = errors.New(resp.Status)
	}
	var entries []string
	if err == nil {
		err = json.NewDecoder(resp.Body).Decode(&entries)
	}
	if err != nil {
		return nil, fmt.Errorf("couldn't dial service %s: %s", service, err)
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("couldn't dial service %s: no healthy instances", service)
	}
	entry := entries[rand.Intn(len(entries))]
	return net.Dial(network, entry)
}
