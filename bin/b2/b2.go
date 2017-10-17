// b2 is a command-line tool for interacting with the Backblaze B2 service.  It
// mimics https://github.com/Backblaze/B2_Command_Line_Tool as much as it is
// able.
package main

import (
	"context"
	"flag"
	"os"

	"github.com/google/subcommands"

	_ "github.com/kurin/blazer/bin/b2/authorizeaccount"
)

func main() {
	flag.Parse()
	ctx := context.Background()
	os.Exit(int(subcommands.Execute(ctx)))
}
