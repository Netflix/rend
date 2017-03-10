// Copyright 2015 Netflix, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"io"

	"github.com/netflix/rend/metrics"
	"github.com/netflix/rend/orcas"
	"github.com/netflix/rend/protocol"
)

type ServerConst func(conns []io.Closer, rp protocol.RequestParser, o orcas.Orca) Server

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
	MetricConnectionsEstablishedExt = metrics.AddCounter("conn_established_ext", nil)
	MetricConnectionsEstablishedL1  = metrics.AddCounter("conn_established_l1", nil)
	MetricConnectionsEstablishedL2  = metrics.AddCounter("conn_established_l2", nil)
	MetricCmdTotal                  = metrics.AddCounter("cmd_total", nil)
	MetricErrAppError               = metrics.AddCounter("err_app_err", nil)
	MetricErrUnrecoverable          = metrics.AddCounter("err_unrecoverable", nil)

	MetricCmdGet     = metrics.AddCounter("cmd_get", nil)
	MetricCmdGetE    = metrics.AddCounter("cmd_gete", nil)
	MetricCmdSet     = metrics.AddCounter("cmd_set", nil)
	MetricCmdAdd     = metrics.AddCounter("cmd_add", nil)
	MetricCmdReplace = metrics.AddCounter("cmd_replace", nil)
	MetricCmdAppend  = metrics.AddCounter("cmd_append", nil)
	MetricCmdPrepend = metrics.AddCounter("cmd_prepend", nil)
	MetricCmdDelete  = metrics.AddCounter("cmd_delete", nil)
	MetricCmdTouch   = metrics.AddCounter("cmd_touch", nil)
	MetricCmdGat     = metrics.AddCounter("cmd_gat", nil)
	MetricCmdUnknown = metrics.AddCounter("cmd_unknown", nil)
	MetricCmdNoop    = metrics.AddCounter("cmd_noop", nil)
	MetricCmdQuit    = metrics.AddCounter("cmd_quit", nil)
	MetricCmdVersion = metrics.AddCounter("cmd_version", nil)

	HistSet     = metrics.AddHistogram("set", false, nil)
	HistAdd     = metrics.AddHistogram("add", false, nil)
	HistReplace = metrics.AddHistogram("replace", false, nil)
	HistAppend  = metrics.AddHistogram("append", false, nil)
	HistPrepend = metrics.AddHistogram("prepend", false, nil)
	HistDelete  = metrics.AddHistogram("delete", false, nil)
	HistTouch   = metrics.AddHistogram("touch", false, nil)
	HistGet     = metrics.AddHistogram("get", false, nil)  // not sampled until configurable
	HistGetE    = metrics.AddHistogram("gete", false, nil) // not sampled until configurable
	HistGat     = metrics.AddHistogram("gat", false, nil)  // not sampled until configurable

	// TODO: inconsistency metrics for when L1 is not a subset of L2
)
