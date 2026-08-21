// Package search supplies bounded, cancellable parallel opportunity work.
package search

import (
	"context"
	"sync"
)

type Job[T any] struct{ Value T }
type Result[T any] struct {
	Value T
	Err   error
}
type Evaluate[T any] func(context.Context, T) error
type MapResult[T, R any] struct {
	Value  T
	Result R
	Err    error
}

// Run is deliberately bounded by workers and closes cleanly on cancellation.
func Run[T any](ctx context.Context, values []T, workers int, evaluate Evaluate[T]) []Result[T] {
	if workers < 1 {
		workers = 1
	}
	jobs := make(chan T)
	results := make(chan Result[T], len(values))
	var wg sync.WaitGroup
	for n := 0; n < workers; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for value := range jobs {
				results <- Result[T]{Value: value, Err: evaluate(ctx, value)}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for _, value := range values {
			select {
			case jobs <- value:
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() { wg.Wait(); close(results) }()
	output := make([]Result[T], 0, len(values))
	for result := range results {
		output = append(output, result)
	}
	return output
}

// Map is the bounded worker-pool variant for operations that return a value,
// such as one executable quote chain per independent route.
func Map[T, R any](ctx context.Context, values []T, workers int, evaluate func(context.Context, T) (R, error)) []MapResult[T, R] {
	if workers < 1 {
		workers = 1
	}
	type item struct {
		value  T
		result R
		err    error
	}
	jobs := make(chan T)
	results := make(chan item, len(values))
	var wg sync.WaitGroup
	for n := 0; n < workers; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for value := range jobs {
				result, err := evaluate(ctx, value)
				results <- item{value, result, err}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for _, value := range values {
			select {
			case jobs <- value:
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() { wg.Wait(); close(results) }()
	output := make([]MapResult[T, R], 0, len(values))
	for item := range results {
		output = append(output, MapResult[T, R]{Value: item.value, Result: item.result, Err: item.err})
	}
	return output
}
