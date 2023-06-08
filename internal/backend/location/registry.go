package location

import (
	"context"
	"net/http"

	"github.com/restic/restic/internal/backend/limiter"
	"github.com/restic/restic/internal/restic"
)

type Registry struct {
	factories map[string]Factory
}

func NewRegistry() *Registry {
	return &Registry{
		factories: make(map[string]Factory),
	}
}

func (r *Registry) Register(scheme string, factory Factory) {
	if r.factories[scheme] != nil {
		panic("duplicate backend")
	}
	r.factories[scheme] = factory
}

func (r *Registry) Lookup(scheme string) Factory {
	return r.factories[scheme]
}

type Factory interface {
	ParseConfig(s string) (interface{}, error)
	StripPassword(s string) string
	Create(ctx context.Context, cfg interface{}, rt http.RoundTripper, lim limiter.Limiter) (restic.Backend, error)
	Open(ctx context.Context, cfg interface{}, rt http.RoundTripper, lim limiter.Limiter) (restic.Backend, error)
}

type GenericBackendFactory[C any, T restic.Backend] struct {
	parseConfigFn   func(s string) (*C, error)
	stripPasswordFn func(s string) string
	createFn        func(ctx context.Context, cfg C, rt http.RoundTripper, lim limiter.Limiter) (T, error)
	openFn          func(ctx context.Context, cfg C, rt http.RoundTripper, lim limiter.Limiter) (T, error)
}

func (f *GenericBackendFactory[C, T]) ParseConfig(s string) (interface{}, error) {
	return f.parseConfigFn(s)
}
func (f *GenericBackendFactory[C, T]) StripPassword(s string) string {
	if f.stripPasswordFn != nil {
		return f.stripPasswordFn(s)
	}
	return s
}
func (f *GenericBackendFactory[C, T]) Create(ctx context.Context, cfg interface{}, rt http.RoundTripper, lim limiter.Limiter) (restic.Backend, error) {
	return f.createFn(ctx, *cfg.(*C), rt, lim)
}
func (f *GenericBackendFactory[C, T]) Open(ctx context.Context, cfg interface{}, rt http.RoundTripper, lim limiter.Limiter) (restic.Backend, error) {
	return f.openFn(ctx, *cfg.(*C), rt, lim)
}

func NewHTTPBackendFactory[C any, T restic.Backend](parseConfigFn func(s string) (*C, error),
	stripPasswordFn func(s string) string,
	createFn func(ctx context.Context, cfg C, rt http.RoundTripper) (T, error),
	openFn func(ctx context.Context, cfg C, rt http.RoundTripper) (T, error)) *GenericBackendFactory[C, T] {

	return &GenericBackendFactory[C, T]{
		parseConfigFn:   parseConfigFn,
		stripPasswordFn: stripPasswordFn,
		createFn: func(ctx context.Context, cfg C, rt http.RoundTripper, _ limiter.Limiter) (T, error) {
			return createFn(ctx, cfg, rt)
		},
		openFn: func(ctx context.Context, cfg C, rt http.RoundTripper, _ limiter.Limiter) (T, error) {
			return openFn(ctx, cfg, rt)
		},
	}
}

func NewLimitedBackendFactory[C any, T restic.Backend](parseConfigFn func(s string) (*C, error),
	stripPasswordFn func(s string) string,
	createFn func(ctx context.Context, cfg C, lim limiter.Limiter) (T, error),
	openFn func(ctx context.Context, cfg C, lim limiter.Limiter) (T, error)) *GenericBackendFactory[C, T] {

	return &GenericBackendFactory[C, T]{
		parseConfigFn:   parseConfigFn,
		stripPasswordFn: stripPasswordFn,
		createFn: func(ctx context.Context, cfg C, _ http.RoundTripper, lim limiter.Limiter) (T, error) {
			return createFn(ctx, cfg, lim)
		},
		openFn: func(ctx context.Context, cfg C, _ http.RoundTripper, lim limiter.Limiter) (T, error) {
			return openFn(ctx, cfg, lim)
		},
	}
}
