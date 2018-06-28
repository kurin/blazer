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
	bf := &bonfire.Bonfire{
		Root: "http://localhost:8822",
	}
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
	rootMux := http.NewServeMux()
	mux := runtime.NewServeMux(pyre.ServeMuxOptions()...)
	if err := pyre.RegisterPyreServiceHandlerFromEndpoint(ctx, mux, "localhost:8823", []grpc.DialOption{grpc.WithInsecure()}); err != nil {
		fmt.Println(err)
	}
	rootMux.Handle("/b2api/v1/", mux)
	rootMux.Handle("/b2api/v1/b2_upload_file/", &bonfire.SimpleFileServer{})
	fmt.Println("ok")
	fmt.Println(http.ListenAndServe(":8822", rootMux))
}
