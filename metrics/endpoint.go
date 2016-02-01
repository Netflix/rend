package metrics

import (
	"fmt"
	"net/http"
)

const metricPrefix = "rend_"

func init() {
	http.Handle("/metrics/counters", http.HandlerFunc(printCounters))
}

func printCounters(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	ctrs := GetAllCounters()
	for name, val := range ctrs {
		fmt.Fprintf(w, "%s%s %d\n", metricPrefix, name, val)
	}
}
