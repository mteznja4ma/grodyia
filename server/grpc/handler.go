package grpc

import (
	"reflect"
	"sync"
)

var once sync.Once

type Handler struct {
	name    string
	handler interface{}
}

func NewGPRCHandler(handler interface{}, opts ...Option) *Handler {
	options := Options{}

	//typ := reflect.TypeOf(handler)
	h := reflect.ValueOf(handler)
	name := reflect.Indirect(h).Type().Name()

	for _, o := range opts {
		o(&options)
	}

	// for m := 0; m < typ.NumMethod(); m++ {
	// 	if e := extractEndpoint(typ.Method(m)); e != nil {
	// 		e.Name = name + "." + e.Name

	// 		for k, v := range options.Metadata[e.Name] {
	// 			e.Metadata[k] = v
	// 		}

	// 		endpoints = append(endpoints, e)
	// 	}
	// }

	return &Handler{
		name:    name,
		handler: handler,
	}
}
