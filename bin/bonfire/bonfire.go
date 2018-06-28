package main

import (
	"context"
	"fmt"
	"net/http"

	"github.com/kurin/blazer/bonfire"
	"github.com/kurin/blazer/internal/pyre"
)

func main() {
	ctx := context.Background()
	srv := pyre.New("http://localhost:8822")
	mux := http.NewServeMux()

	if err := pyre.RegisterServerOnMux(ctx, srv, mux); err != nil {
		fmt.Println(err)
		return
	}

	fs := bonfire.FS("/tmp/b2")
	pyre.RegisterLargeFileManagerOnMux(fs, mux)
	pyre.RegisterSimpleFileManagerOnMux(fs, mux)
	fmt.Println("ok")
	fmt.Println(http.ListenAndServe("localhost:8822", mux))
}
