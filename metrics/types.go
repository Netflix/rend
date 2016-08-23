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

package metrics

const (
	TagMetricType = "type"

	MetricTypeCounter = "counter"
	MetricTypeGauge   = "gauge"

	TagDataType = "dataType"

	DataTypeUint64  = "uint64"
	DataTypeFloat64 = "float64"

	TagStatistic = "statistic"
)

type IntMetric struct {
	Name string
	Val  uint64
	Tgs  Tags
}

type FloatMetric struct {
	Name string
	Val  float64
	Tgs  Tags
}

type Tags map[string]string
