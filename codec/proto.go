package codec

import (
	"errors"

	"google.golang.org/protobuf/proto"
)

// ProtoCodec implements Protocol Buffers encoding/decoding
type ProtoCodec struct{}

// NewProtoCodec creates a new protobuf codec
func NewProtoCodec() Codec {
	return &ProtoCodec{}
}

func (c *ProtoCodec) Name() string {
	return "proto"
}

func (c *ProtoCodec) Marshal(v any) ([]byte, error) {
	msg, ok := v.(proto.Message)
	if !ok {
		return nil, errors.New("proto codec: value must implement proto.Message")
	}
	return proto.Marshal(msg)
}

func (c *ProtoCodec) Unmarshal(data []byte, v any) error {
	msg, ok := v.(proto.Message)
	if !ok {
		return errors.New("proto codec: target must implement proto.Message")
	}
	return proto.Unmarshal(data, msg)
}

func (c *ProtoCodec) ContentType() string {
	return "application/x-protobuf"
}
