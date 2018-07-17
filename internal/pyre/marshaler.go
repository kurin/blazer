package pyre

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"

	"github.com/golang/protobuf/proto"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/kurin/blazer/internal/b2json"
)

// A lot of this is copied from runtime.
type marshaler struct{ b2json.Marshaler }

func (m marshaler) ContentType() string { return "application/json" }

func (m marshaler) Marshal(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.marshalTo(&buf, v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (m marshaler) marshalTo(w io.Writer, v interface{}) error {
	if p, ok := v.(proto.Message); ok {
		return m.Marshaler.Marshal(w, p)
	}
	buf, err := m.marshalNonProtoField(v)
	if err != nil {
		return err
	}
	_, err = w.Write(buf)
	return err
}

func (m marshaler) marshalNonProtoField(v interface{}) ([]byte, error) {
	if v == nil {
		return []byte("null"), nil
	}
	rv := reflect.ValueOf(v)
	for rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return []byte("null"), nil
		}
		rv = rv.Elem()
	}

	if rv.Kind() == reflect.Map {
		msg := make(map[string]*json.RawMessage)
		for _, k := range rv.MapKeys() {
			buf, err := m.Marshal(rv.MapIndex(k).Interface())
			if err != nil {
				return nil, err
			}
			msg[fmt.Sprintf("%v", k.Interface())] = (*json.RawMessage)(&buf)
		}
		if m.Marshaler.Indent != "" {
			return json.MarshalIndent(msg, "", m.Marshaler.Indent)
		}
		return json.Marshal(msg)
	}
	if enum, ok := rv.Interface().(protoEnum); ok && !m.Marshaler.EnumsAsInts {
		return json.Marshal(enum.String())
	}
	return json.Marshal(rv.Interface())
}

func (m marshaler) NewDecoder(r io.Reader) runtime.Decoder {
	d := json.NewDecoder(r)
	return runtime.DecoderWrapper{Decoder: d}
}

func (m marshaler) NewEncoder(w io.Writer) runtime.Encoder {
	return runtime.EncoderFunc(func(v interface{}) error { return m.marshalTo(w, v) })
}

func (m marshaler) Unmarshal(data []byte, v interface{}) error {
	u := &runtime.JSONPb{}
	return u.Unmarshal(data, v)
}

type protoEnum interface {
	fmt.Stringer
	EnumDescriptor() ([]byte, []int)
}
