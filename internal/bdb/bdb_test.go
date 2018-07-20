// Copyright 2018, Google
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bdb_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	bolt "github.com/coreos/bbolt"

	"github.com/kurin/blazer/internal/bdb"
)

type setter struct {
	spec bdb.Spec
	args []fmt.Stringer
	v    string
}

type getter struct {
	spec bdb.Spec
	w    string
}

func TestReadWrite(t *testing.T) {
	table := []struct {
		sets []setter
		gets []getter
	}{
		{
			sets: []setter{
				{
					spec: "/path/to/a/thing",
					v:    "value",
				},
			},
			gets: []getter{
				{
					spec: "/path/to/a/thing",
					w:    "value",
				},
			},
		},
	}

	td, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(td)
	db, err := bolt.Open(filepath.Join(td, "bolt"), 0644, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, e := range table {
		tx := bdb.New(db)
		for _, set := range e.sets {
			path := set.spec.Bind(set.args...)
			tx.Put(path, []byte(set.v))
		}
		if err := tx.Run(); err != nil {
			t.Error(err)
			continue
		}
		tx = bdb.New(db)
		var gots []*bdb.Value
		for _, get := range e.gets {
			path := get.spec.Bind()
			gots = append(gots, tx.Read(path))
		}
		if err := tx.Run(); err != nil {
			t.Error(err)
		}
		for i := range gots {
			want := e.gets[i].w
			got := gots[i].String()
			if got != want {
				t.Errorf("%v: bad values: got %q, want %q", e.gets[i].spec, got, want)
			}
		}
	}
}

func TestFuturePath(t *testing.T) {
	td, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(td)
	db, err := bolt.Open(filepath.Join(td, "bolt"), 0644, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tx := bdb.New(db)
	tx.Put(bdb.Spec("/path/to/the/thing").Bind(), []byte("value"))
	tx.Put(bdb.Spec("/a/value/b/c").Bind(), []byte("other value"))
	value := tx.Read(bdb.Spec("/path/to/the/thing").Bind())
	oval := tx.Read(bdb.Spec("/a/%%value/b/c").Bind(value))
	if err := tx.Run(); err != nil {
		t.Fatal(err)
	}
	if oval.String() != "other value" {
		t.Errorf("bad read: got %q, got %q", oval.String(), "other value")
	}
}
