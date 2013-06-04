package pool

import (
	"container/list"
	"time"
)

type Pool struct {
	Min         int           // minimum resources to keep
	Max         int           // maximum resources to open
	Concurrency int           // number of types a resource can be acquired (concurrently)
	MaxIdle     time.Duration // Time in idle state (no acquire resources) before closing excess resources (keeps at most Min resources)
	Open        OpenFunc      // Function for openeing a resource

	acquire  chan chan Resource
	release  chan Resource
	stop     chan bool
	done     chan bool
	stopping bool

	queue     *list.List
	available *list.List
	resources map[Resource]bool
}

// Must return a Resource of nil. When nil is returned the pool remains unchanged.
type OpenFunc func() Resource

type Resource interface {
	// Close doesn't return an error as the resources is expected to handle (and log) all errors
	Close()
	IsClosed() bool
}

// Acquire a resource. This function blocks until a resource is available.
// When Acquire() is called after the pool is stopped it will return nil.
// Acquired resources MUST ALWAYS be released with Release().
func (p *Pool) Acquire() Resource {
	reply := make(chan Resource, 1)
	p.acquire <- reply
	return <-reply
}

// Release a resource back to the pool.
func (p *Pool) Release(resource Resource) {
	if resource == nil {
		return
	}
	p.release <- resource
}

// Start the pool.
func (p *Pool) Start() {
	p.stop = make(chan bool, 1)
	p.done = make(chan bool, 1)
	p.acquire = make(chan chan Resource)
	p.release = make(chan Resource, 5)

	if p.Concurrency == 0 {
		p.Concurrency = 1
	}

	go p.manage()
}

// Stop the pool. This functions does't block (Use Wait() to wait for the pool to finish).
func (p *Pool) Stop() {
	p.stop <- true
}

// Wait for the pool to finish
func (p *Pool) Wait() {
	<-p.done
	p.done <- true
}

// Stop the pool. This function blocks until all resources have been returned.
func (p *Pool) StopAndWait() {
	p.Stop()
	p.Wait()
}

func (p *Pool) manage() {
	p.queue = list.New()
	p.available = list.New()
	p.resources = make(map[Resource]bool, p.Max)

	var (
		timeout   *time.Timer
		timeout_c <-chan time.Time
	)

	defer func() { p.done <- true }()

	for {
		select {

		case <-p.stop:
			p.stopping = true
			for {
				res := p.pop_available()
				p.close_resource(res)
				if len(p.resources) == 0 {
					return
				}
				if res == nil {
					break
				}
			}

		case reply := <-p.acquire:
			if timeout != nil {
				timeout.Stop()
				timeout = nil
				timeout_c = nil
			}

			if p.stopping {
				reply <- nil

			} else if resource := p.pop_available(); resource != nil {
				reply <- resource

			} else if len(p.resources) < p.Max {
				resource := p.Open()
				if resource != nil {
					p.resources[resource] = true
					reply <- resource
					for i, l := 1, p.Concurrency; i < l; i++ {
						p.available.PushBack(resource)
					}
				} else {
					p.queue.PushBack(reply)
				}

			} else {
				p.queue.PushBack(reply)

			}

		case resource := <-p.release:
			if len(p.resources) > p.Max {
				p.close_resource(resource)

			} else if front := p.queue.Front(); front != nil {
				p.queue.Remove(front)
				reply := front.Value.(chan Resource)
				reply <- resource

			} else if !p.stopping {
				p.available.PushBack(resource)

			} else {
				// Close the resource, we're stopping
				p.close_resource(resource)
				if len(p.resources) == 0 {
					return
				}

			}

			if len(p.resources) == (p.available.Len()/p.Concurrency) && p.MaxIdle > 0 {
				timeout = time.NewTimer(p.MaxIdle)
				timeout_c = timeout.C
			}

		case <-timeout_c:
			timeout_c = nil
			p.close_extra_resources()

		}
	}
}

func (p *Pool) close_extra_resources() {
	for resource := range p.resources {
		if resource.IsClosed() {
			delete(p.resources, resource)
		}
	}

	for len(p.resources) > p.Min {
		p.close_resource(p.pop_available())
	}
}

func (p *Pool) close_resource(resource Resource) {
	if resource == nil {
		return
	}

	if !resource.IsClosed() {
		resource.Close()
	}

	delete(p.resources, resource)
}

func (p *Pool) pop_available() Resource {
	for {
		front := p.available.Front()
		if front == nil {
			break
		}

		p.available.Remove(front)
		resource := front.Value.(Resource)

		if !resource.IsClosed() {
			return resource
		}
	}

	return nil
}
