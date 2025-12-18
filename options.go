package grodyia

import (
	"github.com/google/uuid"

	"github.com/mteznja4ma/grodyia/codec"
	"github.com/mteznja4ma/grodyia/registry"
)

// Options 应用配置
type Options struct {
	// 服务基本信息
	Name    string
	ID      string
	Version string

	// 元数据
	Metadata map[string]string

	// 注册中心
	Registry registry.Registry

	// 编解码器
	Codec codec.Codec
}

// Option 配置函数
type Option func(*Options)

// DefaultOptions 默认配置
func DefaultOptions() Options {
	id := uuid.New().String()[:8]
	return Options{
		Name:     "grodyia-app",
		ID:       id,
		Version:  "1.0.0",
		Metadata: make(map[string]string),
	}
}

// WithName 设置应用名称
func WithName(name string) Option {
	return func(o *Options) {
		o.Name = name
	}
}

// WithID 设置应用ID
func WithID(id string) Option {
	return func(o *Options) {
		o.ID = id
	}
}

// WithVersion 设置应用版本
func WithVersion(version string) Option {
	return func(o *Options) {
		o.Version = version
	}
}

// WithMetadata 设置元数据
func WithMetadata(md map[string]string) Option {
	return func(o *Options) {
		for k, v := range md {
			o.Metadata[k] = v
		}
	}
}

// WithRegistry 设置注册中心
func WithRegistry(r registry.Registry) Option {
	return func(o *Options) {
		o.Registry = r
	}
}

// WithCodec 设置编解码器
func WithCodec(c codec.Codec) Option {
	return func(o *Options) {
		o.Codec = c
	}
}
