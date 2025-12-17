package codec

// Codec defines the interface for encoding/decoding
// This allows protocol-agnostic serialization
type Codec interface {
	// Name returns the codec name
	Name() string
	// Marshal encodes a value to bytes
	Marshal(v any) ([]byte, error)
	// Unmarshal decodes bytes to a value
	Unmarshal(data []byte, v any) error
	// ContentType returns the MIME type
	ContentType() string
}

// Reader is a codec that can read from a stream
type Reader interface {
	Read(any) error
}

// Writer is a codec that can write to a stream
type Writer interface {
	Write(any) error
}

// ReadWriteCloser combines Reader, Writer and io.Closer
type ReadWriteCloser interface {
	Reader
	Writer
	Close() error
}
