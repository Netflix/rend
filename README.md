**Note: This is an early stage project under active development. It is not yet in widespread use in Netflix. It is going to change quite a bit from the current state before it is ready. Consider it pre-alpha.**

# Rend - memcached proxy

Rend is a proxy that is designed to sit on the same server as both a [memcached](https://github.com/memcached/memcached) process and an SSD-backed L2 cache, such as [RocksDB](https://github.com/facebook/rocksdb). It is written in [Go](https://github.com/golang/go) and is under active development at Netflix. Some more points about Rend:

 * Designed to handle tens of thousands of concurrent connections
 * Speaks a subset of the memcached text and binary protocols
 * Uses binary protocol locally to efficiently communicate with memcached
 * Comes with a load testing package

Rend is still under active development and is in the testing phases internally. It still needs work to fully productionize. It is being developed in public as a part of Netflix's philosophy toward open sourcing non-differentiating potions of our infrastructure.

## Motivation

Caching is used several ways at Netflix. Some people use it as a true working set cache, while others use it as the only storage mechanism for their service. Others use it as a session cache. This means that some services can continue as usual with some data loss, while others will permanently lose data and start to serve fallbacks. The caching project that Rend is built to complement is [EVCache](https://github.com/Netflix/EVCache).

The genesis of rend starts with memcached memory management. Internally, memcached keeps a set of slabs for different size data. Slabs are logical groupings of pages, which are a fixed size set on startup. Pages map to real physical memory and are split based on the slab's data size. In versions 1.4.24 and prior, pages were permanently allocated to a particular slab and never released even if empty. As well, if there were many holes in the data in RAM, there was no compaction and therefore memory could get very fragmented over time.

The second half of the story is within Netflix. There was a set of data being written daily to a cache en masse during nightly computation. An underlying data source changed in such a way that caused the output of one such process to change drastically in size from one day to the next. When the data set was being written to the cache, it was different enough in size to land in a different memcached slab. The cache was sized to hold one copy of the data, not two, so when the new data was written the memory filled completely. Once full, memcached was evicting large portions of newly-computed data while holding on to mostly empty memory in a different slab.

So what was the solution? Take the incoming data and split it into fixed-size chunks prior to inserting into memcached to bypass the complication of the slab allocator. If everything is the same size, there will never be holes which are out of reach for new data. This hardened us against future data changes, which are inevitable. Rend (which means "to tear apart") is the server-side solution to this problem, which also enables much more intelligence on the server at the cost of increased complexity.

## Setup and Prerequisites

### Dependencies

 * `memcached ^1.4.24`
 * `go ^1.5.1`

In order to use the proxy, it is required to have a memcached running on the local machine. The recommended version is the latest version, currently 1.4.25. This version has the full set of features used by the proxy as well as a bunch of performance and stability improvements. It is the same one we use at Netflix for production use cases. The version that ships with Mac OSX does not work. You can see installation instructions for memcached.

As well, to build Rend a working Go distribution is required. The latest Go version (1.5.1) is used for development. Thanks to the Go 1.0 [compatibility promise](https://golang.org/doc/go1compat), it should be able to be compiled and run on earlier versions, though we do not use or test versions earlier than 1.5.1. The garbage collection improvements in 1.5 help latency numbers, which is why we don't test on older versions.

### Get the source code

    git clone https://github.com/Netflix/rend.git
    cd rend

### Build and Run

For distribution:

    go build memproxy.go
    ./memproxy

or for development:

    go run memproxy.go

## Testing

### blast<i></i>.go

send random `set`, `get`, `touch` and `delete` commands. Examples:

Use the binary memcached protocol with 10 worker goroutines (i.e. 10 connections) to send 100,000 requests with a key length of 5.

    go run blast.go --binary -w 10 -n 100000 -kl 5

### setget<i></i>.go

Run sets followed by gets, with verification of contents.

    go run setget.go (needs opts)

### sizes<i></i>.go

Runs sets of a steadily increasing size to catch errors with specific size data.

    go run sizes.go (needs opts)

<br>
<br>
<br>

#### Readme TODO:

 * links to godoc
 * limitations
 * any known problems
