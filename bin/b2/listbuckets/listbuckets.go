package authorizeaccount

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/google/subcommands"
	"github.com/kurin/blazer/b2"
)

func init() {
	subcommands.Register(command{}, "bucket")
}

type command struct{}

func (command) Name() string           { return "list-buckets" }
func (command) Synopsis() string       { return "List B2 buckets." }
func (command) Usage() string          { return "list-buckets\n" }
func (command) SetFlags(*flag.FlagSet) {}

func (command) Execute(ctx context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	b2File := filepath.Join(os.Getenv("HOME"), ".blazer-b2")
	data, err := ioutil.ReadFile(b2File)
	if err != nil {
		fmt.Println(err)
		return subcommands.ExitFailure
	}
	var ai authInfo
	if err := json.Unmarshal(data, &ai); err != nil {
		fmt.Println(err)
		fmt.Println("try running authorize-account")
		return subcommands.ExitFailure
	}

	client, err := b2.NewClient(ctx, ai.AuthID, ai.AuthKey)
	if err != nil {
		fmt.Println(err)
		return subcommands.ExitFailure
	}
	buckets, err := client.ListBuckets(ctx)
	if err != nil {
		fmt.Println(err)
		return subcommands.ExitFailure
	}
	for _, bucket := range buckets {
		fmt.Println(bucket.Name())
	}
	return subcommands.ExitSuccess
}

type authInfo struct {
	AuthID  string
	AuthKey string
}
