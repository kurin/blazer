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
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	bolt "github.com/coreos/bbolt"

	"github.com/kurin/blazer/internal/bdb"
)

type kv struct {
	key []interface{}
	val []byte
}

func TestReadWrite(t *testing.T) {
	table := []struct {
		kvs  []kv
		want []kv
	}{
		{
			kvs: []kv{
				{
					key: []interface{}{"a", "b"},
					val: []byte("qwer"),
				},
				{
					key: []interface{}{"path", "to", "the", "thing"},
					val: []byte("lerp"),
				},
			},
			want: []kv{
				{
					key: []interface{}{"a", "b"},
					val: []byte("qwer"),
				},
				{
					key: []interface{}{"path", "to", "the", "thing"},
					val: []byte("lerp"),
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
		for _, kvp := range e.kvs {
			tx.Put(kvp.val, kvp.key...)
		}
		if err := tx.Run(); err != nil {
			t.Error(err)
			continue
		}
		tx = bdb.New(db)
		var gots []*bdb.Value
		for _, kvp := range e.want {
			gots = append(gots, tx.Read(kvp.key...))
		}
		if err := tx.Run(); err != nil {
			t.Error(err)
		}
		for i := range gots {
			want := e.want[i].val
			got := gots[i].Bytes()
			if !bytes.Equal(got, want) {
				t.Errorf("%v: bad values: got %q, want %q", e.want[i].key, string(got), string(want))
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
	tx.Put([]byte("value"), "path", "to", "the", "thing")
	tx.Put([]byte("other value"), "a", "value", "b", "c")
	value := tx.Read("path", "to", "the", "thing")
	oval := tx.Read("a", value, "b", "c")
	if err := tx.Run(); err != nil {
		t.Fatal(err)
	}
	if oval.String() != "other value" {
		t.Errorf("bad read: got %q, got %q", oval.String(), "other value")
	}
}
