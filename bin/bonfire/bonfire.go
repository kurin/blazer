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
	mux := http.NewServeMux()

	if err := pyre.RegisterServerOnMux(ctx, &pyre.Server{
		Account:   bonfire.Localhost(8822),
		LargeFile: bonfire.Localhost(8822),
		Simple:    bonfire.Localhost(8822),
		Bucket:    &bonfire.LocalBucket{Port: 8822},
	}, mux); err != nil {
		fmt.Println(err)
		return
	}

	fs := bonfire.FS("/tmp/b2")
	pyre.RegisterLargeFileManagerOnMux(fs, mux)
	pyre.RegisterSimpleFileManagerOnMux(fs, mux)
	fmt.Println("ok")
	fmt.Println(http.ListenAndServe("localhost:8822", mux))
}
