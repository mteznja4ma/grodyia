package codec

import (
	"encoding/json"
)

// JSONCodec implements JSON encoding/decoding
type JSONCodec struct{}

// NewJSONCodec creates a new JSON codec
func NewJSONCodec() Codec {
	return &JSONCodec{}
}

func (c *JSONCodec) Name() string {
	return "json"
}

func (c *JSONCodec) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (c *JSONCodec) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

func (c *JSONCodec) ContentType() string {
	return "application/json"
}
