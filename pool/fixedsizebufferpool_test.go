package pool_test

import (
	"sync"
	"testing"

	"github.com/netflix/rend/pool"
)

const (
	poolBufSize  = 24
	poolBufScale = 10
	numWorkers   = 10
	numOps       = 100000
)

func TestFixedSizeBufferPool(t *testing.T) {
	p := pool.NewFixedSizeBufferPool(poolBufSize, poolBufScale)

	start := make(chan struct{})

	end := &sync.WaitGroup{}
	end.Add(numWorkers)

	for i := 0; i < numWorkers; i++ {
		go worker(t, i, p, start, end)
	}

	close(start)
	end.Wait()

	t.Log("Test done")
}

func worker(t *testing.T, id int, p *pool.FixedSizeBufferPool, start chan struct{}, end *sync.WaitGroup) {
	<-start

	t.Logf("Worker %d started", id)

	for i := 0; i < numOps; i++ {
		buf, bid := p.Get()

		for j := 1; j <= poolBufSize; j++ {
			buf[j-1] = byte(id) + byte(j)
		}

		p.Put(bid)
	}

	t.Logf("Worker %d done", id)
	end.Done()
}
