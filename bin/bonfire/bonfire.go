package main

import (
	"context"
	"fmt"
	"net"
	"net/http"

	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/kurin/blazer/bonfire"
	"github.com/kurin/blazer/internal/pyre"
	"google.golang.org/grpc"
)

func main() {
	bf := &bonfire.Bonfire{}
	server := grpc.NewServer()
	pyre.RegisterPyreServiceServer(server, bf)
	l, err := net.Listen("tcp", "0.0.0.0:8823")
	if err != nil {
		fmt.Println(err)
	}
	go func() {
		fmt.Println(server.Serve(l))
	}()
	ctx := context.Background()
	mux := runtime.NewServeMux()
	if err := pyre.RegisterPyreServiceHandlerFromEndpoint(ctx, mux, "localhost:8823", []grpc.DialOption{grpc.WithInsecure()}); err != nil {
		fmt.Println(err)
	}
	fmt.Println("ok")
	fmt.Println(http.ListenAndServe(":8822", mux))
}
