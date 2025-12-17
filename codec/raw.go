package codec

import (
	"errors"
)

// RawCodec is a pass-through codec for raw bytes
// Perfect for custom protocols and maximum flexibility
type RawCodec struct{}

// NewRawCodec creates a new raw codec
func NewRawCodec() Codec {
	return &RawCodec{}
}

func (c *RawCodec) Name() string {
	return "raw"
}

func (c *RawCodec) Marshal(v interface{}) ([]byte, error) {
	switch val := v.(type) {
	case []byte:
		return val, nil
	case string:
		return []byte(val), nil
	case *[]byte:
		if val != nil {
			return *val, nil
		}
		return nil, nil
	default:
		return nil, errors.New("raw codec: value must be []byte or string")
	}
}

func (c *RawCodec) Unmarshal(data []byte, v interface{}) error {
	switch val := v.(type) {
	case *[]byte:
		*val = data
		return nil
	case *string:
		*val = string(data)
		return nil
	default:
		return errors.New("raw codec: target must be *[]byte or *string")
	}
}

func (c *RawCodec) ContentType() string {
	return "application/octet-stream"
}

