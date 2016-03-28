package server

import (
	"io"

	"github.com/netflix/rend/common"
	"github.com/netflix/rend/metrics"
	"github.com/netflix/rend/orcas"
)

type ServerConst func(conns []io.Closer, rp common.RequestParser, o orcas.Orca) Server

type Server interface {
	Loop()
}

type ListenType int

const (
	ListenTCP ListenType = iota
	ListenUnix
)

type ListenArgs struct {
	// The type of the connection. "tcp" or "unix" only.
	Type ListenType
	// TCP port to listen on, if applicable
	Port int
	// Unix domain socket path to listen on, if applicable
	Path string
}

var (
	MetricConnectionsEstablishedExt = metrics.AddCounter("conn_established_ext")
	MetricConnectionsEstablishedL1  = metrics.AddCounter("conn_established_l1")
	MetricConnectionsEstablishedL2  = metrics.AddCounter("conn_established_l2")
	MetricCmdTotal                  = metrics.AddCounter("cmd_total")
	MetricErrAppError               = metrics.AddCounter("err_app_err")
	MetricErrUnrecoverable          = metrics.AddCounter("err_unrecoverable")

	HistSet     = metrics.AddHistogram("set")
	HistAdd     = metrics.AddHistogram("add")
	HistReplace = metrics.AddHistogram("replace")
	HistDelete  = metrics.AddHistogram("delete")
	HistTouch   = metrics.AddHistogram("touch")
	HistGet     = metrics.AddHistogram("get")
	HistGat     = metrics.AddHistogram("gat")

	// TODO: inconsistency metrics for when L1 is not a subset of L2
)
