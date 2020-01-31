package tracing

import (
	"regexp"
	"strings"

	"go.opencensus.io/trace"
	"go.opencensus.io/trace/tracestate"
)

// CODE BASED ON:
// https://github.com/census-instrumentation/opencensus-go/blob/ \
// master/plugin/ochttp/propagation/tracecontext/propagation.go

const (
	trimOWSRegexFmt  = `^[\x09\x20]*(.*[^\x20\x09])[\x09\x20]*$`
	maxTracestateLen = 512
)

var trimOWSRegExp = regexp.MustCompile(trimOWSRegexFmt) // nolint

func tracestateToString(sc trace.SpanContext) string {
	var pairs = make([]string, 0, len(sc.Tracestate.Entries()))
	if sc.Tracestate != nil {
		for _, entry := range sc.Tracestate.Entries() {
			pairs = append(pairs, strings.Join([]string{entry.Key, entry.Value}, "="))
		}
		return strings.Join(pairs, ",")
	}
	return ""
}

func tracestateFromString(tracestateString string) *tracestate.Tracestate {
	var entries []tracestate.Entry // nolint
	pairs := strings.Split(tracestateString, ",")
	hdrLenWithoutOWS := len(pairs) - 1 // Number of commas
	for _, pair := range pairs {
		matches := trimOWSRegExp.FindStringSubmatch(pair)
		if matches == nil {
			return nil
		}
		pair = matches[1]
		hdrLenWithoutOWS += len(pair)
		if hdrLenWithoutOWS > maxTracestateLen {
			return nil
		}
		kv := strings.Split(pair, "=")
		if len(kv) != 2 {
			return nil
		}
		entries = append(entries, tracestate.Entry{Key: kv[0], Value: kv[1]})
	}
	ts, err := tracestate.New(nil, entries...)
	if err != nil {
		return nil
	}

	return ts
}
