package storegatewaypb

import (
	"strings"

	"github.com/cortexproject/cortex/pkg/cortexpb"
)

func NewQueryResponse(series *cortexpb.TimeSeries) *QueryResponse {
	return &QueryResponse{
		Result: &QueryResponse_Timeseries{
			Timeseries: series,
		},
	}
}

func NewQueryWarningsResponse(errs ...error) *QueryResponse {
	warnings := make([]string, 0, len(errs))
	for _, err := range errs {
		warnings = append(warnings, err.Error())
	}
	return &QueryResponse{
		Result: &QueryResponse_Warnings{
			Warnings: strings.Join(warnings, ", "),
		},
	}
}

func NewQueryRangeResponse(series *cortexpb.TimeSeries) *QueryRangeResponse {
	return &QueryRangeResponse{
		Result: &QueryRangeResponse_Timeseries{
			Timeseries: series,
		},
	}
}

func NewQueryRangeWarningsResponse(errs ...error) *QueryRangeResponse {
	warnings := make([]string, 0, len(errs))
	for _, err := range errs {
		warnings = append(warnings, err.Error())
	}
	return &QueryRangeResponse{
		Result: &QueryRangeResponse_Warnings{
			Warnings: strings.Join(warnings, ", "),
		},
	}
}
