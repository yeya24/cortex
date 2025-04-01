package api

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/prometheus/prometheus/promql/parser"
	"io"
	"net/http"
	"os"
	"testing"

	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/server"
)

const (
	acceptEncodingHeader = "Accept-Encoding"
	gzipEncoding         = "gzip"
)

type FakeLogger struct{}

func (fl *FakeLogger) Log(keyvals ...interface{}) error {
	return nil
}

func TestNewApiWithoutSourceIPExtractor(t *testing.T) {
	cfg := Config{}
	serverCfg := server.Config{
		HTTPListenNetwork: server.DefaultNetwork,
		MetricsNamespace:  "without_source_ip_extractor",
	}
	server, err := server.New(serverCfg)
	require.NoError(t, err)

	api, err := New(cfg, serverCfg, server, &FakeLogger{})
	require.NoError(t, err)
	require.Nil(t, api.sourceIPs)
}

func TestNewApiWithSourceIPExtractor(t *testing.T) {
	cfg := Config{}
	serverCfg := server.Config{
		HTTPListenNetwork: server.DefaultNetwork,
		LogSourceIPs:      true,
		MetricsNamespace:  "with_source_ip_extractor",
	}
	server, err := server.New(serverCfg)
	require.NoError(t, err)

	api, err := New(cfg, serverCfg, server, &FakeLogger{})
	require.NoError(t, err)
	require.NotNil(t, api.sourceIPs)
}

func TestNewApiWithInvalidSourceIPExtractor(t *testing.T) {
	cfg := Config{}
	s := server.Server{
		HTTP: &mux.Router{},
	}
	serverCfg := server.Config{
		HTTPListenNetwork:  server.DefaultNetwork,
		LogSourceIPs:       true,
		LogSourceIPsHeader: "SomeHeader",
		LogSourceIPsRegex:  "[*",
		MetricsNamespace:   "with_invalid_source_ip_extractor",
	}

	api, err := New(cfg, serverCfg, &s, &FakeLogger{})
	require.Error(t, err)
	require.Nil(t, api)
}

func TestNewApiWithHeaderLogging(t *testing.T) {
	cfg := Config{
		HTTPRequestHeadersToLog: []string{"ForTesting"},
	}
	serverCfg := server.Config{
		HTTPListenNetwork: server.DefaultNetwork,
		MetricsNamespace:  "with_header_logging",
	}
	server, err := server.New(serverCfg)
	require.NoError(t, err)

	api, err := New(cfg, serverCfg, server, &FakeLogger{})
	require.NoError(t, err)
	require.NotNil(t, api.HTTPHeaderMiddleware)

}

func TestNewApiWithoutHeaderLogging(t *testing.T) {
	cfg := Config{
		HTTPRequestHeadersToLog: []string{},
	}
	serverCfg := server.Config{
		HTTPListenNetwork: server.DefaultNetwork,
		MetricsNamespace:  "without_header_logging",
	}
	server, err := server.New(serverCfg)
	require.NoError(t, err)

	api, err := New(cfg, serverCfg, server, &FakeLogger{})
	require.NoError(t, err)
	require.Nil(t, api.HTTPHeaderMiddleware)

}

func Benchmark_Compression(b *testing.B) {
	client := &http.Client{
		Transport: &http.Transport{
			DisableCompression: true,
		},
	}

	cfg := Config{
		ResponseCompression: true,
	}

	cases := map[string]struct {
		enc            string
		numberOfLabels int
	}{
		"gzip-10-labels": {
			enc:            gzipEncoding,
			numberOfLabels: 10,
		},
		"gzip-100-labels": {
			enc:            gzipEncoding,
			numberOfLabels: 100,
		},
		"gzip-1K-labels": {
			enc:            gzipEncoding,
			numberOfLabels: 1000,
		},
		"gzip-10K-labels": {
			enc:            gzipEncoding,
			numberOfLabels: 10000,
		},
		"gzip-100K-labels": {
			enc:            gzipEncoding,
			numberOfLabels: 100000,
		},
		"gzip-1M-labels": {
			enc:            gzipEncoding,
			numberOfLabels: 1000000,
		},
	}

	for name, tc := range cases {
		b.Run(name, func(b *testing.B) {
			serverCfg := server.Config{
				HTTPListenNetwork: server.DefaultNetwork,
				HTTPListenPort:    8080,
				Registerer:        prometheus.NewRegistry(),
			}

			server, err := server.New(serverCfg)
			require.NoError(b, err)
			api, err := New(cfg, serverCfg, server, &FakeLogger{})
			require.NoError(b, err)

			labels := labels.ScratchBuilder{}

			for i := 0; i < tc.numberOfLabels; i++ {
				labels.Add(fmt.Sprintf("Name%v", i), fmt.Sprintf("Value%v", i))
			}

			respBody, err := json.Marshal(labels.Labels())
			require.NoError(b, err)

			api.RegisterRoute("/foo_endpoint", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				_, err := w.Write(respBody)
				require.NoError(b, err)
			}), false, "GET")

			go func() {
				err := server.Run()
				require.NoError(b, err)
			}()

			defer server.Shutdown()
			req, _ := http.NewRequest("GET", "http://"+server.HTTPListenAddr().String()+"/foo_endpoint", nil)
			req.Header.Set(acceptEncodingHeader, "gzip")

			b.ReportAllocs()
			b.ResetTimer()

			// Reusing the array to read the body and avoid allocation on the test
			encRespBody := make([]byte, len(respBody))

			for i := 0; i < b.N; i++ {
				resp, err := client.Do(req)

				require.NoError(b, err)

				require.NoError(b, err, "client get failed with unexpected error")

				responseBodySize := 0
				for {
					n, err := resp.Body.Read(encRespBody)
					responseBodySize += n
					if err == io.EOF {
						break
					}
				}

				b.ReportMetric(float64(responseBodySize), "ContentLength")
			}
		})
	}
}

func TestCCC(t *testing.T) {
	m := labels.MustNewMatcher(labels.MatchRegexp, "name", "10\\.1\\.202\\.201")
	fmt.Println(m.IsRegexOptimized())
}

func TestAAAC(t *testing.T) {
	m := labels.MustNewMatcher(labels.MatchRegexp, "name", "sts..*.amazonaws.com")
	fmt.Println(m.IsRegexOptimized())
}

func TestAAE(t *testing.T) {
	m := labels.MustNewMatcher(labels.MatchRegexp, "name", "/augury\\.AmbientHealth/.*|/augury\\.Health/.*")
	fmt.Println(m.IsRegexOptimized())
}

func TestAAD(t *testing.T) {
	m := labels.MustNewMatcher(labels.MatchRegexp, "name", "/stripe_internal.henson.Henson/.*")
	fmt.Println(m.IsRegexOptimized())
}

func TestAAA(t *testing.T) {
	m := labels.MustNewMatcher(labels.MatchRegexp, "name", "sts\\..*\\.amazonaws\\.com")
	fmt.Println(m.IsRegexOptimized())
}

func TestBBB(t *testing.T) {
	// Open the CSV file
	file, err := os.Open("/Users/benye/Downloads/queries-top100.csv")
	if err != nil {
		fmt.Printf("Error opening file: %v\n", err)
		return
	}
	defer file.Close()

	// Create CSV reader
	reader := csv.NewReader(file)
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true

	// Skip header row
	_, err = reader.Read()
	if err != nil {
		fmt.Printf("Error reading header: %v\n", err)
		return
	}

	// Read and process each row
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Printf("Error reading row: %v\n", err)
			continue
		}

		query := row[1]

		// Parse the PromQL query
		expr, err := parser.ParseExpr(query)
		if err != nil {
			fmt.Printf("Error parsing query: %v\n", err)
			continue
		}

		// Function to check for regex matchers recursively
		var matched bool
		parser.Inspect(expr, func(node parser.Node, nodes []parser.Node) error {
			switch n := node.(type) {
			case *parser.VectorSelector:
				for _, m := range n.LabelMatchers {
					if m.Type == labels.MatchRegexp || m.Type == labels.MatchNotRegexp {
						if len(m.SetMatches()) > 0 {
							continue
						}
						if m.StringMatcher() != nil {
							continue
						}
						//if m.Value == "/com.stripe.gocode.instancelifecycle.fleet.FleetRepository/GetChecks|/com.stripe.gocode.instancelifecycle.fleet.FleetRepository/GetHostMetadata|/com.stripe.gocode.instancelifecycle.InstanceLifecycle/GetInstanceLifecycleStatus|/com.stripe.gocode.instancelifecycle.fleet.FleetRepository/ReportCheckEvaluationBatch|/com.stripe.gocode.instancelifecycle.fleet.FleetRepository/ReportCurrentCheckConfigs" {
						//	fmt.Printf("aaa\n")
						//}
						if m.Value == `sts\\..*.amazonaws.com` {
							fmt.Printf("aaa\n")
						}

						mightMatch := len(m.SetMatches()) == 0 && m.Prefix() == "" && m.Suffix() == "" && len(m.Contains()) == 0 && m.StringMatcher() != nil
						if mightMatch || len(m.Prefix()) > 0 || len(m.Suffix()) > 0 || len(m.Contains()) > 0 {
							matched = true
							fmt.Println(m.String())
						}
					}
				}
			}
			return nil
		})

		if matched {
			fmt.Printf("Found potential regex heavy query: %s\n", query)
		}
	}
}
