package main

import (
	"flag"
	"fmt"
	"github.com/Big-Kotik/transparent-data-bridge-api/bridge/api/v1"
	"github.com/Big-Kotik/transparentb-data-bridge-relay/internal"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
	"os/signal"
)

var port = flag.Int64("port", 10000, "port for server")

func main() {

	flag.Parse()

	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", *port))
	if err != nil {
		log.Fatal(err)
	}

	relay := internal.NewRelayServer()

	srv := grpc.NewServer()
	v1.RegisterTransparentDataBridgeServiceServer(srv, relay)
	v1.RegisterTransparentDataRelayServiceServer(srv, relay)

	go srv.Serve(lis)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals)

	<-signals

	srv.GracefulStop()
}
