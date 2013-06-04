package pool

import (
	"sync"
	"testing"
	"time"
)

func TestAcquire(t *testing.T) {

	var (
		resources []*test_resource
		wg        sync.WaitGroup
		sum       int
	)

	pool := Pool{
		Max: 40,
		Open: func() Resource {
			res := &test_resource{}
			resources = append(resources, res)
			return res
		},
	}

	pool.Start()

	for i := 0; i < 4000; i++ {
		wg.Add(1)
		go func(i int, wg *sync.WaitGroup) {
			defer wg.Done()

			r := pool.Acquire().(*test_resource)
			defer pool.Release(r)

			r.Inc()
		}(i, &wg)
	}

	wg.Wait()

	pool.Stop()
	pool.Wait()

	if len(resources) != 40 {
		t.Fatalf("expected only 40 resources to be opened. instead %d were opened", len(resources))
	}

	for _, res := range resources {
		sum += res.Count()
		if !res.IsClosed() {
			t.Fatalf("failed to close a resource")
		}
	}

	if sum != 4000 {
		t.Fatalf("expected sum to be 400 but it was %d", sum)
	}
}

func TestWithMaxIdle(t *testing.T) {

	var (
		resources []*test_resource
		wg        sync.WaitGroup
		sum       int
	)

	pool := Pool{
		Min:     20,
		Max:     40,
		MaxIdle: 1 * time.Second,
		Open: func() Resource {
			res := &test_resource{}
			resources = append(resources, res)
			return res
		},
	}

	pool.Start()

	for i := 0; i < 4000; i++ {
		wg.Add(1)
		go func(i int, wg *sync.WaitGroup) {
			defer wg.Done()

			r := pool.Acquire().(*test_resource)
			defer pool.Release(r)

			r.Inc()
		}(i, &wg)
	}
	wg.Wait()

	time.Sleep(1100 * time.Millisecond)

	for i := 0; i < 4000; i++ {
		wg.Add(1)
		go func(i int, wg *sync.WaitGroup) {
			defer wg.Done()

			r := pool.Acquire().(*test_resource)
			defer pool.Release(r)

			r.Inc()
		}(i, &wg)
	}
	wg.Wait()

	pool.Stop()
	pool.Wait()

	if len(resources) != 60 {
		t.Fatalf("expected only 60 resources to be opened. instead %d were opened", len(resources))
	}

	for _, res := range resources {
		sum += res.Count()
		if !res.IsClosed() {
			t.Fatalf("failed to close a resource")
		}
	}

	if sum != 8000 {
		t.Fatalf("expected sum to be 8000 but it was %d", sum)
	}
}

func TestAcquireConc(t *testing.T) {

	var (
		resources []*test_resource
		wg        sync.WaitGroup
		sum       int
	)

	pool := Pool{
		Max:         40,
		Concurrency: 5,
		Open: func() Resource {
			res := &test_resource{}
			resources = append(resources, res)
			return res
		},
	}

	pool.Start()

	for i := 0; i < 4000; i++ {
		wg.Add(1)
		go func(i int, wg *sync.WaitGroup) {
			defer wg.Done()

			r := pool.Acquire().(*test_resource)
			defer pool.Release(r)

			r.Inc()
		}(i, &wg)
	}

	wg.Wait()

	pool.Stop()
	pool.Wait()

	if len(resources) != 40 {
		t.Fatalf("expected only 40 resources to be opened. instead %d were opened", len(resources))
	}

	for _, res := range resources {
		sum += res.Count()
		if !res.IsClosed() {
			t.Fatalf("failed to close a resource")
		}
	}

	if sum != 4000 {
		t.Fatalf("expected sum to be 400 but it was %d", sum)
	}
}

func TestWithMaxIdleConc(t *testing.T) {

	var (
		resources []*test_resource
		wg        sync.WaitGroup
		sum       int
	)

	pool := Pool{
		Min:         20,
		Max:         40,
		Concurrency: 5,
		MaxIdle:     1 * time.Second,
		Open: func() Resource {
			res := &test_resource{}
			resources = append(resources, res)
			return res
		},
	}

	pool.Start()

	for i := 0; i < 4000; i++ {
		wg.Add(1)
		go func(i int, wg *sync.WaitGroup) {
			defer wg.Done()

			r := pool.Acquire().(*test_resource)
			defer pool.Release(r)

			r.Inc()
		}(i, &wg)
	}
	wg.Wait()

	time.Sleep(1100 * time.Millisecond)

	for i := 0; i < 4000; i++ {
		wg.Add(1)
		go func(i int, wg *sync.WaitGroup) {
			defer wg.Done()

			r := pool.Acquire().(*test_resource)
			defer pool.Release(r)

			r.Inc()
		}(i, &wg)
	}
	wg.Wait()

	pool.Stop()
	pool.Wait()

	if len(resources) != 60 {
		t.Fatalf("expected only 60 resources to be opened. instead %d were opened", len(resources))
	}

	for _, res := range resources {
		sum += res.Count()
		if !res.IsClosed() {
			t.Fatalf("failed to close a resource")
		}
	}

	if sum != 8000 {
		t.Fatalf("expected sum to be 8000 but it was %d", sum)
	}
}

type test_resource struct {
	counter int
	closed  bool
	mtx     sync.RWMutex
}

func (t *test_resource) Inc() {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	time.Sleep(5 * time.Millisecond)
	t.counter++
}

func (t *test_resource) Count() int {
	t.mtx.RLock()
	defer t.mtx.RUnlock()

	return t.counter
}

func (t *test_resource) Close() {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	t.closed = true
}

func (t *test_resource) IsClosed() bool {
	t.mtx.RLock()
	defer t.mtx.RUnlock()

	return t.closed
}
