package config

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/fsnotify/fsnotify"
)

// fileSource is a file configuration source
type fileSource struct {
	path string
}

// NewFileSource creates a new file source
func NewFileSource(path string) Source {
	return &fileSource{path: path}
}

// Load loads configuration from the file
func (f *fileSource) Load() ([]*KeyValue, error) {
	data, err := os.ReadFile(f.path)
	if err != nil {
		return nil, err
	}

	// Determine format from extension
	format := strings.TrimPrefix(filepath.Ext(f.path), ".")
	if format == "yml" {
		format = "yaml"
	}

	return []*KeyValue{
		{
			Key:    f.path,
			Value:  data,
			Format: format,
		},
	}, nil
}

// Watch returns a watcher for file changes
func (f *fileSource) Watch() (Watcher, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	if err := watcher.Add(f.path); err != nil {
		watcher.Close()
		return nil, err
	}

	return &fileWatcher{
		f:       f,
		watcher: watcher,
	}, nil
}

// fileWatcher watches file changes
type fileWatcher struct {
	f       *fileSource
	watcher *fsnotify.Watcher
}

// Next returns the next file change
func (w *fileWatcher) Next() ([]*KeyValue, error) {
	for {
		select {
		case event, ok := <-w.watcher.Events:
			if !ok {
				return nil, nil
			}
			if event.Op&fsnotify.Write == fsnotify.Write {
				return w.f.Load()
			}
		case err, ok := <-w.watcher.Errors:
			if !ok {
				return nil, nil
			}
			return nil, err
		}
	}
}

// Stop stops the watcher
func (w *fileWatcher) Stop() error {
	return w.watcher.Close()
}

// envSource is an environment variable configuration source
type envSource struct {
	prefix string
}

// NewEnvSource creates a new environment variable source
func NewEnvSource(prefix string) Source {
	return &envSource{prefix: prefix}
}

// Load loads configuration from environment variables
func (e *envSource) Load() ([]*KeyValue, error) {
	var data []byte
	prefix := strings.ToUpper(e.prefix)
	if prefix != "" && !strings.HasSuffix(prefix, "_") {
		prefix += "_"
	}

	// Build JSON from environment variables
	kvs := make(map[string]string)
	for _, env := range os.Environ() {
		parts := strings.SplitN(env, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := parts[0]
		value := parts[1]

		if prefix != "" {
			if !strings.HasPrefix(key, prefix) {
				continue
			}
			key = strings.TrimPrefix(key, prefix)
		}

		// Convert KEY_NAME to key.name
		key = strings.ToLower(strings.ReplaceAll(key, "_", "."))
		kvs[key] = value
	}

	// Simple JSON building
	data = []byte("{")
	first := true
	for k, v := range kvs {
		if !first {
			data = append(data, ',')
		}
		first = false
		data = append(data, '"')
		data = append(data, k...)
		data = append(data, '"', ':', '"')
		data = append(data, v...)
		data = append(data, '"')
	}
	data = append(data, '}')

	return []*KeyValue{
		{
			Key:    "env",
			Value:  data,
			Format: "json",
		},
	}, nil
}

// Watch returns nil for env source (no watching)
func (e *envSource) Watch() (Watcher, error) {
	return nil, nil
}
