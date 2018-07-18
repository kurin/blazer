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

	lfm, err := bonfire.New("/tmp/b2")
	if err != nil {
		fmt.Println(err)
		return
	}

	if err := pyre.RegisterServerOnMux(ctx, &pyre.Server{
		Account: lfm,
		File:    lfm,
		Bucket:  lfm,
		List:    lfm,
	}, mux); err != nil {
		fmt.Println(err)
		return
	}

	pyre.RegisterLargeFileManagerOnMux(lfm, mux)
	pyre.RegisterSimpleFileManagerOnMux(lfm, mux)
	pyre.RegisterDownloadManagerOnMux(lfm, mux)
	fmt.Println("ok")
	fmt.Println(http.ListenAndServe("localhost:8822", mux))
}
