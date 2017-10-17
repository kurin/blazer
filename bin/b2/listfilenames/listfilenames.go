package listfilenames

// The tool I'm copying has a lot of 1-1 correspondence with the API.

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/google/subcommands"
	"github.com/kurin/blazer/b2"
)

func init() {
	subcommands.Register(command{}, "file")
}

type command struct{}

func (command) Name() string           { return "list-file-names" }
func (command) Synopsis() string       { return "List file names." }
func (command) Usage() string          { return "list-file-names <bucketName> [<startFileName>] [<maxToShow>]\n" }
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

	args := f.Args()
	if len(args) < 1 {
		f.Usage()
		return subcommands.ExitUsageError
	}

	client, err := b2.NewClient(ctx, ai.AuthID, ai.AuthKey)
	if err != nil {
		fmt.Println(err)
		return subcommands.ExitFailure
	}
	bucket, err := client.Bucket(ctx, args[0])
	if err != nil {
		fmt.Println(err)
		return subcommands.ExitFailure
	}

	cur := &b2.Cursor{}
	for {
		objs, nc, err := bucket.ListCurrentObjects(ctx, 1000, cur)
		if err != io.EOF && err != nil {
			fmt.Println(err)
			return subcommands.ExitFailure
		}
		for _, obj := range objs {
			fmt.Println(obj.Name())
		}
		if err == io.EOF {
			return subcommands.ExitSuccess
		}
		cur = nc
	}
}

type authInfo struct {
	AuthID  string
	AuthKey string
}
