package pool_test

import (
	"runtime"
	"sync"
	"testing"

	"github.com/netflix/rend/pool"
)

const (
	poolBufSize  = 24
	poolBufScale = 4
	numWorkers   = 40
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

	//t.Log("Test done")
}

func worker(t *testing.T, id int, p *pool.FixedSizeBufferPool, start chan struct{}, end *sync.WaitGroup) {
	<-start

	//t.Logf("Worker %d started", id)

	for i := 0; i < numOps; i++ {
		buf, bid := p.Get()

		for j := 1; j <= poolBufSize; j++ {
			buf[j-1] = byte(id) + byte(j)
		}

		runtime.Gosched()

		for j := 1; j <= poolBufSize; j++ {
			if buf[j-1] != (byte(id) + byte(j)) {
				t.Fatalf("Caught inconsistency in data: expected %d, got %d", byte(id)+byte(j), buf[j-1])
			}
		}

		p.Put(bid)
	}

	//t.Logf("Worker %d done", id)
	end.Done()
}
