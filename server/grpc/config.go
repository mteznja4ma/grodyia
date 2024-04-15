package grpc

type (
	ServiceMiddleware struct {
		Trace      bool `json:"trace,default=true"`
		Recover    bool `json:"recover,default=true"`
		Prometheus bool `json:"prometheus,default=true"`
		Breaker    bool `json:"breaker,default=true"`
	}
)
