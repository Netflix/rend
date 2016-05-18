# Rend: Memcached-Compatible Server and Proxy

[![Dev chat at https://gitter.im/Netflix/rend](https://badges.gitter.im/Netflix/rend.svg)](https://gitter.im/Netflix/rend?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge) [![GoDoc](https://godoc.org/github.com/netflix/rend?status.svg)](https://godoc.org/github.com/netflix/rend)

Rend is a proxy whose primary use case is to sit on the same server as both a [memcached](https://github.com/memcached/memcached) process and an SSD-backed L2 cache, such as [RocksDB](https://github.com/facebook/rocksdb). It is written in [Go](https://github.com/golang/go) and is under active development at Netflix. Some more points about Rend:

 * Designed to handle tens of thousands of concurrent connections
 * Speaks a subset of the memcached text and binary protocols
 * Uses binary protocol locally to efficiently communicate with memcached
 * Comes with a load testing package
 * Modular design to allow different backends to be plugged in (see: [rend-lmdb](https://github.com/netflix/rend-lmdb))

Rend is currently in production at Netflix and serving live member traffic.

## Motivation

Caching is used several ways at Netflix. Some people use it as a true working set cache, while others use it as the only storage mechanism for their service. Others use it as a session cache. This means that some services can continue as usual with some data loss, while others will permanently lose data and start to serve fallbacks. Rend is built to complement [EVCache](https://github.com/Netflix/EVCache), which is the main caching solution in use at Netflix.

The genesis of Rend starts with memcached memory management. Internally, memcached keeps a set of slabs for different size data. Slabs are logical groupings of pages, which are a fixed size set on startup. Pages map to physical memory and are split based on the slab's data size. In versions 1.4.24 and prior, pages were permanently allocated to a particular slab and never released even if empty. As well, if there were many holes in the data in RAM, there was no compaction and therefore memory could get very fragmented over time.

The second half of the story is within Netflix. Every night, a big batch process computes recommendations for each of our members in multiple steps. Each of these steps loads their output into EVCache. An underlying data source changed one day in such a way that caused the output of one batch compute process to change drastically in size. When the data set was being written to the cache, it was different enough in size to land in a different memcached slab. The cache was sized to hold one copy of the data, not two, so when the new data was written, the memory filled completely. Once full, memcached started evicting large portions of newly-computed data while holding on to mostly empty memory in a different slab.

So what was the solution? Take the incoming data and split it into fixed-size chunks prior to inserting into memcached. This bypassed the complication of the slab allocator. If everything is the same size, there will never be holes that are out of reach for new data. This hardened us against future data changes, which are inevitable. Rend (which means "to tear apart") is the server-side solution to this problem, which also enables much more intelligence on the server.

## Components

Rend is a server and a set of libraries that can be used to compose a memcached-compatible server of your own. It consists of packages for protocol parsing, a server loop, request orchestration (for L1 / L2 logic), and a set of handlers that actually communicate with the backing storage. It also includes a metrics library that is unintrusive and very fast. The memproxy.go file acts as the main function for Rend as a server and showcases the usage of all of the available components.

![Rend internals](rend_internals.png "Rend internals")

## Setup and Prerequisites

### Dependencies

To just get started, everything needed is in this repository. The Basic Server section shows how to stand up a simple server

In order to use the proxy in L1-only mode, it is required to have a memcached-compatible server running on the local machine. For our production deployment, this is Memcached itself. The recommended version is the latest version, currently 1.4.25. This version has the full set of features used by the proxy as well as a bunch of performance and stability improvements. The version that ships with Mac OS X does not work (it is very old). You can see installation instructions for Memcached at https://memcached.org.

To run the project in L1/L2 mode, it is required to run a Rend-based server, as the logic within Rend uses a Memcached protocol extension (the gete command) to retrieve the TTL from the L2. There's plans to make this optional, but it is not yet.

As well, to build Rend, a working Go distribution is required. The latest Go version (1.6.2) is used for development. Thanks to the Go 1.0 [compatibility promise](https://golang.org/doc/go1compat), it should be able to be compiled and run on earlier versions, though we do not use or test versions earlier than 1.6.2. The garbage collection improvements in 1.5 and 1.6 help latency numbers, which is why we don't test on older versions.

### Get the Source Code

    git clone https://github.com/Netflix/rend.git
    cd rend

### Build and Run

For distribution:

    go build memproxy.go
    ./memproxy

or for development:

    go run memproxy.go

## Basic Server

## Using the default Rend server (memproxy.go)

```
go get github.com/netflix/rend
go build github.com/netflix/rend
./rend --l1-inmem
```

And in another console window (> means typed input):

```
$ nc localhost 11211
> get foo
END
> set foo 0 0 6
> foobar
STORED
> get foo
VALUE foo 0 6
foobar
END
> touch foo 2
TOUCHED
> get foo
VALUE foo 0 6
foobar
END
> get foo
END
> quit
Bye
```

### Using Rend as Libraries

To get a working debug server using the Rend libraries, it takes 21 lines of code, including imports and whitespace:

    package main

    import (
        "github.com/netflix/rend/handlers"
        "github.com/netflix/rend/handlers/inmem"
        "github.com/netflix/rend/orcas"
        "github.com/netflix/rend/server"
    )

    func main() {
        server.ListenAndServe(
            server.ListenArgs{
                Type: server.ListenTCP,
                Port: 11211,
            },
            server.Default,
            orcas.L1Only,
            inmem.New,
            handlers.NilHandler(""),
        )
    }

## Testing

### blast<i></i>.go

The blast script sends random requests of all types to the target, including:
* `set`
* `add`
* `replace`
* `get`
* batch `get`
* `touch`
* `get-and-touch`
* `delete`

Use the binary memcached protocol with 10 worker goroutines (i.e. 10 connections) to send 1,000,000 requests with a key length of 5.

    go run blast.go --binary -n 1000000 -p 11211 -w 10 -kl 5

### setget<i></i>.go

Run sets followed by gets, with verification of contents.

    go run setget.go --binary -n 100000 -p 11211 -w 10

### sizes<i></i>.go

Runs sets of a steadily increasing size to catch errors with specific size data.

    go run sizes.go --binary -p 11211

