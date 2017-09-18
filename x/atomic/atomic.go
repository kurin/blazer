// Copyright 2016, Google
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

// Package atomic implements an experimental interface for using B2 as a
// coordination primitive.
package atomic

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"

	"github.com/kurin/blazer/b2"
)

const metaKey = "blazer-meta-key-no-touchie"

var errUpdateConflict = errors.New("update conflict")

// Group represents a collection of B2 objects that can be modified atomically.
type Group struct {
	name string
	b    *b2.Bucket
	ba   *b2.BucketAttrs
}

func (g *Group) info(ctx context.Context) (atomicInfo, error) {
	var ai atomicInfo
	attrs, err := g.b.Attrs(ctx)
	if err != nil {
		return ai, err
	}
	g.ba = attrs
	imap := attrs.Info
	if imap == nil {
		return ai, nil
	}
	enc, ok := imap[metaKey]
	if !ok {
		return ai, nil
	}
	b, err := base64.StdEncoding.DecodeString(enc)
	if err != nil {
		return ai, err
	}
	if err := json.Unmarshal(b, &ai); err != nil {
		return ai, err
	}
	return ai, nil
}

func (g *Group) save(ctx context.Context, ai atomicInfo) error {
	ai.Serial++

	b, err := json.Marshal(ai)
	if err != nil {
		return err
	}
	s := base64.StdEncoding.EncodeToString(b)

	for {
		oldAI, err := g.info(ctx)
		if err != nil {
			return err
		}
		if oldAI.Serial != ai.Serial-1 {
			return errUpdateConflict
		}
		if g.ba.Info == nil {
			g.ba.Info = make(map[string]string)
		}
		g.ba.Info[metaKey] = s
		err = g.b.Update(ctx, g.ba)
		if err == nil {
			return nil
		}
		if !b2.IsUpdateConflict(err) {
			return err
		}
		// Bucket update conflict; try again.
	}
}

func (g *Group) List(ctx context.Context) ([]string, error) {
	ai, err := g.info(ctx)
	if err != nil {
		return nil, err
	}
	var l []string
	for name := range ai.Locations {
		l = append(l, name)
	}
	return l, nil
}

func (g *Group) NewWriter(ctx context.Context, name string) (io.WriteCloser, error) {
	return nil, nil
}

func (g *Group) NewReader(ctx context.Context, name string) (io.ReadCloser, error) {
	return nil, nil
}

type atomicInfo struct {
	Version   int
	Serial    int
	Locations map[string]string
}
