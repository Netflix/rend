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

package orcas

import (
	"github.com/netflix/rend/common"
	"github.com/netflix/rend/handlers"
	"github.com/netflix/rend/metrics"
	"github.com/netflix/rend/protocol"
)

type OrcaConst func(l1, l2 handlers.Handler, res protocol.Responder) Orca

type Orca interface {
	Set(req common.SetRequest) error
	Add(req common.SetRequest) error
	Replace(req common.SetRequest) error
	Append(req common.SetRequest) error
	Prepend(req common.SetRequest) error
	Delete(req common.DeleteRequest) error
	Touch(req common.TouchRequest) error
	Get(req common.GetRequest) error
	GetE(req common.GetRequest) error
	Gat(req common.GATRequest) error
	Noop(req common.NoopRequest) error
	Quit(req common.QuitRequest) error
	Version(req common.VersionRequest) error
	Unknown(req common.Request) error
	Error(req common.Request, reqType common.RequestType, err error)
}

var (
	MetricCmdGetL1       = metrics.AddCounter("cmd_get_l1", nil)
	MetricCmdGetL2       = metrics.AddCounter("cmd_get_l2", nil)
	MetricCmdGetHits     = metrics.AddCounter("cmd_get_hits", nil)
	MetricCmdGetHitsL1   = metrics.AddCounter("cmd_get_hits_l1", nil)
	MetricCmdGetHitsL2   = metrics.AddCounter("cmd_get_hits_l2", nil)
	MetricCmdGetMisses   = metrics.AddCounter("cmd_get_misses", nil)
	MetricCmdGetMissesL1 = metrics.AddCounter("cmd_get_misses_l1", nil)
	MetricCmdGetMissesL2 = metrics.AddCounter("cmd_get_misses_l2", nil)
	MetricCmdGetErrors   = metrics.AddCounter("cmd_get_errors", nil)
	MetricCmdGetErrorsL1 = metrics.AddCounter("cmd_get_errors_l1", nil)
	MetricCmdGetErrorsL2 = metrics.AddCounter("cmd_get_errors_l2", nil)
	MetricCmdGetKeys     = metrics.AddCounter("cmd_get_keys", nil)
	MetricCmdGetKeysL1   = metrics.AddCounter("cmd_get_keys_l1", nil)
	MetricCmdGetKeysL2   = metrics.AddCounter("cmd_get_keys_l2", nil)

	MetricCmdGetSetL1       = metrics.AddCounter("cmd_get_set_l1", nil)
	MetricCmdGetSetErrorsL1 = metrics.AddCounter("cmd_get_set_errors_l1", nil)
	MetricCmdGetSetSucessL1 = metrics.AddCounter("cmd_get_set_success_l1", nil)

	MetricCmdGetSetErrorL1DeleteL1       = metrics.AddCounter("cmd_get_set_l1_error_delete_l1", nil)
	MetricCmdGetSetErrorL1DeleteHitsL1   = metrics.AddCounter("cmd_get_set_l1_error_delete_hits_l1", nil)
	MetricCmdGetSetErrorL1DeleteMissesL1 = metrics.AddCounter("cmd_get_set_l1_error_delete_misses_l1", nil)
	MetricCmdGetSetErrorL1DeleteErrorsL1 = metrics.AddCounter("cmd_get_set_l1_error_delete_errors_l1", nil)

	MetricCmdGetEL1       = metrics.AddCounter("cmd_gete_l1", nil)
	MetricCmdGetEL2       = metrics.AddCounter("cmd_gete_l2", nil)
	MetricCmdGetEHits     = metrics.AddCounter("cmd_gete_hits", nil)
	MetricCmdGetEHitsL1   = metrics.AddCounter("cmd_gete_hits_l1", nil)
	MetricCmdGetEHitsL2   = metrics.AddCounter("cmd_gete_hits_l2", nil)
	MetricCmdGetEMisses   = metrics.AddCounter("cmd_gete_misses", nil)
	MetricCmdGetEMissesL1 = metrics.AddCounter("cmd_gete_misses_l1", nil)
	MetricCmdGetEMissesL2 = metrics.AddCounter("cmd_gete_misses_l2", nil)
	MetricCmdGetEErrors   = metrics.AddCounter("cmd_gete_errors", nil)
	MetricCmdGetEErrorsL1 = metrics.AddCounter("cmd_gete_errors_l1", nil)
	MetricCmdGetEErrorsL2 = metrics.AddCounter("cmd_gete_errors_l2", nil)
	MetricCmdGetEKeys     = metrics.AddCounter("cmd_gete_keys", nil)
	MetricCmdGetEKeysL1   = metrics.AddCounter("cmd_gete_keys_l1", nil)
	MetricCmdGetEKeysL2   = metrics.AddCounter("cmd_gete_keys_l2", nil)

	MetricCmdSetL1        = metrics.AddCounter("cmd_set_l1", nil)
	MetricCmdSetL2        = metrics.AddCounter("cmd_set_l2", nil)
	MetricCmdSetSuccess   = metrics.AddCounter("cmd_set_success", nil)
	MetricCmdSetSuccessL1 = metrics.AddCounter("cmd_set_success_l1", nil)
	MetricCmdSetSuccessL2 = metrics.AddCounter("cmd_set_success_l2", nil)
	MetricCmdSetErrors    = metrics.AddCounter("cmd_set_errors", nil)
	MetricCmdSetErrorsL1  = metrics.AddCounter("cmd_set_errors_l1", nil)
	MetricCmdSetErrorsL2  = metrics.AddCounter("cmd_set_errors_l2", nil)

	// L1L2 delete after failed L1 set metrics
	MetricCmdSetL1ErrorDeleteL1       = metrics.AddCounter("cmd_set_l1_error_delete_l1", nil)
	MetricCmdSetL1ErrorDeleteHitsL1   = metrics.AddCounter("cmd_set_l1_error_delete_hits_l1", nil)
	MetricCmdSetL1ErrorDeleteMissesL1 = metrics.AddCounter("cmd_set_l1_error_delete_misses_l1", nil)
	MetricCmdSetL1ErrorDeleteErrorsL1 = metrics.AddCounter("cmd_set_l1_error_delete_errors_l1", nil)

	// Batch L1L2 set metrics
	MetricCmdSetReplaceL1          = metrics.AddCounter("cmd_set_replace_l1", nil)
	MetricCmdSetReplaceNotStoredL1 = metrics.AddCounter("cmd_set_replace_not_stored_l1", nil)
	MetricCmdSetReplaceErrorsL1    = metrics.AddCounter("cmd_set_replace_errors_l1", nil)
	MetricCmdSetReplaceStoredL1    = metrics.AddCounter("cmd_set_replace_stored_l1", nil)

	// Batch L1L2 delete after failed set metrics
	MetricsCmdSetReplaceL1ErrorDeleteL1       = metrics.AddCounter("cmd_set_replace_l1_error_delete_l1", nil)
	MetricsCmdSetReplaceL1ErrorDeleteHitsL1   = metrics.AddCounter("cmd_set_replace_l1_error_delete_hits_l1", nil)
	MetricsCmdSetReplaceL1ErrorDeleteMissesL1 = metrics.AddCounter("cmd_set_replace_l1_error_delete_misses_l1", nil)
	MetricsCmdSetReplaceL1ErrorDeleteErrorsL1 = metrics.AddCounter("cmd_set_replace_l1_error_delete_errors_l1", nil)

	MetricCmdAddL1          = metrics.AddCounter("cmd_add_l1", nil)
	MetricCmdAddL2          = metrics.AddCounter("cmd_add_l2", nil)
	MetricCmdAddStored      = metrics.AddCounter("cmd_add_stored", nil)
	MetricCmdAddStoredL1    = metrics.AddCounter("cmd_add_stored_l1", nil)
	MetricCmdAddStoredL2    = metrics.AddCounter("cmd_add_stored_l2", nil)
	MetricCmdAddNotStored   = metrics.AddCounter("cmd_add_not_stored", nil)
	MetricCmdAddNotStoredL1 = metrics.AddCounter("cmd_add_not_stored_l1", nil)
	MetricCmdAddNotStoredL2 = metrics.AddCounter("cmd_add_not_stored_l2", nil)
	MetricCmdAddErrors      = metrics.AddCounter("cmd_add_errors", nil)
	MetricCmdAddErrorsL1    = metrics.AddCounter("cmd_add_errors_l1", nil)
	MetricCmdAddErrorsL2    = metrics.AddCounter("cmd_add_errors_l2", nil)

	// Batch L1L2 add metrics
	MetricCmdAddReplaceL1          = metrics.AddCounter("cmd_add_replace_l1", nil)
	MetricCmdAddReplaceNotStoredL1 = metrics.AddCounter("cmd_add_replace_not_stored_l1", nil)
	MetricCmdAddReplaceErrorsL1    = metrics.AddCounter("cmd_add_replace_errors_l1", nil)
	MetricCmdAddReplaceStoredL1    = metrics.AddCounter("cmd_add_replace_stored_l1", nil)

	MetricCmdReplaceL1          = metrics.AddCounter("cmd_replace_l1", nil)
	MetricCmdReplaceL2          = metrics.AddCounter("cmd_replace_l2", nil)
	MetricCmdReplaceStored      = metrics.AddCounter("cmd_replace_stored", nil)
	MetricCmdReplaceStoredL1    = metrics.AddCounter("cmd_replace_stored_l1", nil)
	MetricCmdReplaceStoredL2    = metrics.AddCounter("cmd_replace_stored_l2", nil)
	MetricCmdReplaceNotStored   = metrics.AddCounter("cmd_replace_not_stored", nil)
	MetricCmdReplaceNotStoredL1 = metrics.AddCounter("cmd_replace_not_stored_l1", nil)
	MetricCmdReplaceNotStoredL2 = metrics.AddCounter("cmd_replace_not_stored_l2", nil)
	MetricCmdReplaceErrors      = metrics.AddCounter("cmd_replace_errors", nil)
	MetricCmdReplaceErrorsL1    = metrics.AddCounter("cmd_replace_errors_l1", nil)
	MetricCmdReplaceErrorsL2    = metrics.AddCounter("cmd_replace_errors_l2", nil)

	// Batch L1L2 replace metrics
	MetricCmdReplaceReplaceL1          = metrics.AddCounter("cmd_replace_replace_l1", nil)
	MetricCmdReplaceReplaceNotStoredL1 = metrics.AddCounter("cmd_replace_replace_not_stored_l1", nil)
	MetricCmdReplaceReplaceErrorsL1    = metrics.AddCounter("cmd_replace_replace_errors_l1", nil)
	MetricCmdReplaceReplaceStoredL1    = metrics.AddCounter("cmd_replace_replace_stored_l1", nil)

	MetricCmdAppendL1          = metrics.AddCounter("cmd_append_l1", nil)
	MetricCmdAppendL2          = metrics.AddCounter("cmd_append_l2", nil)
	MetricCmdAppendStored      = metrics.AddCounter("cmd_append_stored", nil)
	MetricCmdAppendStoredL1    = metrics.AddCounter("cmd_append_stored_l1", nil)
	MetricCmdAppendStoredL2    = metrics.AddCounter("cmd_append_stored_l2", nil)
	MetricCmdAppendNotStored   = metrics.AddCounter("cmd_append_not_stored", nil)
	MetricCmdAppendNotStoredL1 = metrics.AddCounter("cmd_append_not_stored_l1", nil)
	MetricCmdAppendNotStoredL2 = metrics.AddCounter("cmd_append_not_stored_l2", nil)
	MetricCmdAppendErrors      = metrics.AddCounter("cmd_append_errors", nil)
	MetricCmdAppendErrorsL1    = metrics.AddCounter("cmd_append_errors_l1", nil)
	MetricCmdAppendErrorsL2    = metrics.AddCounter("cmd_append_errors_l2", nil)

	MetricCmdPrependL1          = metrics.AddCounter("cmd_prepend_l1", nil)
	MetricCmdPrependL2          = metrics.AddCounter("cmd_prepend_l2", nil)
	MetricCmdPrependStored      = metrics.AddCounter("cmd_prepend_stored", nil)
	MetricCmdPrependStoredL1    = metrics.AddCounter("cmd_prepend_stored_l1", nil)
	MetricCmdPrependStoredL2    = metrics.AddCounter("cmd_prepend_stored_l2", nil)
	MetricCmdPrependNotStored   = metrics.AddCounter("cmd_prepend_not_stored", nil)
	MetricCmdPrependNotStoredL1 = metrics.AddCounter("cmd_prepend_not_stored_l1", nil)
	MetricCmdPrependNotStoredL2 = metrics.AddCounter("cmd_prepend_not_stored_l2", nil)
	MetricCmdPrependErrors      = metrics.AddCounter("cmd_prepend_errors", nil)
	MetricCmdPrependErrorsL1    = metrics.AddCounter("cmd_prepend_errors_l1", nil)
	MetricCmdPrependErrorsL2    = metrics.AddCounter("cmd_prepend_errors_l2", nil)

	MetricCmdDeleteL1       = metrics.AddCounter("cmd_delete_l1", nil)
	MetricCmdDeleteL2       = metrics.AddCounter("cmd_delete_l2", nil)
	MetricCmdDeleteHits     = metrics.AddCounter("cmd_delete_hits", nil)
	MetricCmdDeleteHitsL1   = metrics.AddCounter("cmd_delete_hits_l1", nil)
	MetricCmdDeleteHitsL2   = metrics.AddCounter("cmd_delete_hits_l2", nil)
	MetricCmdDeleteMisses   = metrics.AddCounter("cmd_delete_misses", nil)
	MetricCmdDeleteMissesL1 = metrics.AddCounter("cmd_delete_misses_l1", nil)
	MetricCmdDeleteMissesL2 = metrics.AddCounter("cmd_delete_misses_l2", nil)
	MetricCmdDeleteErrors   = metrics.AddCounter("cmd_delete_errors", nil)
	MetricCmdDeleteErrorsL1 = metrics.AddCounter("cmd_delete_errors_l1", nil)
	MetricCmdDeleteErrorsL2 = metrics.AddCounter("cmd_delete_errors_l2", nil)

	MetricCmdTouchL1       = metrics.AddCounter("cmd_touch_l1", nil)
	MetricCmdTouchL2       = metrics.AddCounter("cmd_touch_l2", nil)
	MetricCmdTouchHits     = metrics.AddCounter("cmd_touch_hits", nil)
	MetricCmdTouchHitsL1   = metrics.AddCounter("cmd_touch_hits_l1", nil)
	MetricCmdTouchHitsL2   = metrics.AddCounter("cmd_touch_hits_l2", nil)
	MetricCmdTouchMisses   = metrics.AddCounter("cmd_touch_misses", nil)
	MetricCmdTouchMissesL1 = metrics.AddCounter("cmd_touch_misses_l1", nil)
	MetricCmdTouchMissesL2 = metrics.AddCounter("cmd_touch_misses_l2", nil)
	MetricCmdTouchErrors   = metrics.AddCounter("cmd_touch_errors", nil)
	MetricCmdTouchErrorsL1 = metrics.AddCounter("cmd_touch_errors_l1", nil)
	MetricCmdTouchErrorsL2 = metrics.AddCounter("cmd_touch_errors_l2", nil)

	// Batch L1L2 touch metrics
	MetricCmdTouchTouchL1       = metrics.AddCounter("cmd_touch_touch_l1", nil)
	MetricCmdTouchTouchMissesL1 = metrics.AddCounter("cmd_touch_touch_misses_l1", nil)
	MetricCmdTouchTouchErrorsL1 = metrics.AddCounter("cmd_touch_touch_errors_l1", nil)
	MetricCmdTouchTouchHitsL1   = metrics.AddCounter("cmd_touch_touch_hits_l1", nil)

	MetricCmdGatL1       = metrics.AddCounter("cmd_gat_l1", nil)
	MetricCmdGatL2       = metrics.AddCounter("cmd_gat_l2", nil)
	MetricCmdGatHits     = metrics.AddCounter("cmd_gat_hits", nil)
	MetricCmdGatHitsL1   = metrics.AddCounter("cmd_gat_hits_l1", nil)
	MetricCmdGatHitsL2   = metrics.AddCounter("cmd_gat_hits_l2", nil)
	MetricCmdGatMisses   = metrics.AddCounter("cmd_gat_misses", nil)
	MetricCmdGatMissesL1 = metrics.AddCounter("cmd_gat_misses_l1", nil)
	MetricCmdGatMissesL2 = metrics.AddCounter("cmd_gat_misses_l2", nil)
	MetricCmdGatErrors   = metrics.AddCounter("cmd_gat_errors", nil)
	MetricCmdGatErrorsL1 = metrics.AddCounter("cmd_gat_errors_l1", nil)
	MetricCmdGatErrorsL2 = metrics.AddCounter("cmd_gat_errors_l2", nil)

	// Secondary metrics under GAT that refer to other kinds of operations to
	// backing datastores as a part of the overall request
	MetricCmdGatAddL1          = metrics.AddCounter("cmd_gat_add_l1", nil)
	MetricCmdGatAddErrorsL1    = metrics.AddCounter("cmd_gat_add_errors_l1", nil)
	MetricCmdGatAddStoredL1    = metrics.AddCounter("cmd_gat_add_stored_l1", nil)
	MetricCmdGatAddNotStoredL1 = metrics.AddCounter("cmd_gat_add_not_stored_l1", nil)
	MetricCmdGatTouchL2        = metrics.AddCounter("cmd_gat_touch_l2", nil)
	MetricCmdGatTouchHitsL2    = metrics.AddCounter("cmd_gat_touch_hits_l2", nil)
	MetricCmdGatTouchMissesL2  = metrics.AddCounter("cmd_gat_touch_misses_l2", nil)
	MetricCmdGatTouchErrorsL2  = metrics.AddCounter("cmd_gat_touch_errors_l2", nil)

	// Batch L1L2 gat metrics
	MetricCmdGatTouchL1       = metrics.AddCounter("cmd_gat_touch_l1", nil)
	MetricCmdGatTouchMissesL1 = metrics.AddCounter("cmd_gat_touch_misses_l1", nil)
	MetricCmdGatTouchErrorsL1 = metrics.AddCounter("cmd_gat_touch_errors_l1", nil)
	MetricCmdGatTouchHitsL1   = metrics.AddCounter("cmd_gat_touch_hits_l1", nil)

	// Special metrics
	MetricInconsistencyDetected = metrics.AddCounter("inconsistency_detected", nil)

	// Histograms for sub-operations
	HistSetL1     = metrics.AddHistogram("set_l1", false, nil)
	HistSetL2     = metrics.AddHistogram("set_l2", false, nil)
	HistAddL1     = metrics.AddHistogram("add_l1", false, nil)
	HistAddL2     = metrics.AddHistogram("add_l2", false, nil)
	HistReplaceL1 = metrics.AddHistogram("replace_l1", false, nil)
	HistReplaceL2 = metrics.AddHistogram("replace_l2", false, nil)
	HistAppendL1  = metrics.AddHistogram("append_l1", false, nil)
	HistAppendL2  = metrics.AddHistogram("append_l2", false, nil)
	HistPrependL1 = metrics.AddHistogram("prepend_l1", false, nil)
	HistPrependL2 = metrics.AddHistogram("prepend_l2", false, nil)
	HistDeleteL1  = metrics.AddHistogram("delete_l1", false, nil)
	HistDeleteL2  = metrics.AddHistogram("delete_l2", false, nil)
	HistTouchL1   = metrics.AddHistogram("touch_l1", false, nil)
	HistTouchL2   = metrics.AddHistogram("touch_l2", false, nil)

	HistGetL1 = metrics.AddHistogram("get_l1", false, nil) // not sampled until configurable
	HistGetL2 = metrics.AddHistogram("get_l2", false, nil) // not sampled until configurable
	//HistGetSingleL1 = metrics.AddHistogram("get_single_l1", false, nil) // not sampled until configurable
	//HistGetSingleL2 = metrics.AddHistogram("get_single_l2", false, nil) // not sampled until configurable

	HistGetEL1 = metrics.AddHistogram("gete_l1", false, nil) // not sampled until configurable
	HistGetEL2 = metrics.AddHistogram("gete_l2", false, nil) // not sampled until configurable
	//HistGetESingleL1 = metrics.AddHistogram("gete_single_l1", false, nil) // not sampled until configurable
	//HistGetESingleL2 = metrics.AddHistogram("gete_single_l2", false, nil) // not sampled until configurable

	HistGatL1 = metrics.AddHistogram("gat_l1", false, nil) // not sampled until configurable
	HistGatL2 = metrics.AddHistogram("gat_l2", false, nil) // not sampled until configurable
	//HistGatSingleL1 = metrics.AddHistogram("gat_single_l1", false, nil) // not sampled until configurable
	//HistGatSingleL2 = metrics.AddHistogram("gat_single_l2", false, nil) // not sampled until configurable
)
