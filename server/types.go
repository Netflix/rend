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

	MetricCmdGet     = metrics.AddCounter("cmd_get")
	MetricCmdGetE    = metrics.AddCounter("cmd_gete")
	MetricCmdSet     = metrics.AddCounter("cmd_set")
	MetricCmdAdd     = metrics.AddCounter("cmd_add")
	MetricCmdReplace = metrics.AddCounter("cmd_replace")
	MetricCmdAppend  = metrics.AddCounter("cmd_append")
	MetricCmdPrepend = metrics.AddCounter("cmd_prepend")
	MetricCmdDelete  = metrics.AddCounter("cmd_delete")
	MetricCmdTouch   = metrics.AddCounter("cmd_touch")
	MetricCmdGat     = metrics.AddCounter("cmd_gat")
	MetricCmdUnknown = metrics.AddCounter("cmd_unknown")
	MetricCmdNoop    = metrics.AddCounter("cmd_noop")
	MetricCmdQuit    = metrics.AddCounter("cmd_quit")
	MetricCmdVersion = metrics.AddCounter("cmd_version")

	HistSet     = metrics.AddHistogram("set", false)
	HistAdd     = metrics.AddHistogram("add", false)
	HistReplace = metrics.AddHistogram("replace", false)
	HistAppend  = metrics.AddHistogram("append", false)
	HistPrepend = metrics.AddHistogram("prepend", false)
	HistDelete  = metrics.AddHistogram("delete", false)
	HistTouch   = metrics.AddHistogram("touch", false)
	HistGet     = metrics.AddHistogram("get", false) // not sampled until configurable
	HistGat     = metrics.AddHistogram("gat", false) // not sampled until configurable

	// TODO: inconsistency metrics for when L1 is not a subset of L2
)
