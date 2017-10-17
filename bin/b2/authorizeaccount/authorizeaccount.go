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
	subcommands.Register(command{}, "account")
}

type command struct{}

func (command) Name() string           { return "authorize-account" }
func (command) Synopsis() string       { return "Authorize a B2 account." }
func (command) Usage() string          { return "authorize-account [<accountId>] [<applicationKey>]\n" }
func (command) SetFlags(*flag.FlagSet) {}

func (command) Execute(ctx context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	args := f.Args()
	if len(args) != 2 {
		f.Usage()
		return subcommands.ExitUsageError
	}

	if _, err := b2.NewClient(ctx, args[0], args[1]); err != nil {
		fmt.Println(err)
		return subcommands.ExitFailure
	}

	b2File := filepath.Join(os.Getenv("HOME"), ".blazer-b2")
	data, err := json.Marshal(&authInfo{AuthID: args[0], AuthKey: args[1]})
	if err != nil {
		fmt.Println(err)
		return subcommands.ExitFailure
	}
	if err := ioutil.WriteFile(b2File, data, 0600); err != nil {
		fmt.Println(err)
		return subcommands.ExitFailure
	}
	return subcommands.ExitSuccess
}

// TODO: move this into a common lib
type authInfo struct {
	AuthID  string
	AuthKey string
}
