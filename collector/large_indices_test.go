// Copyright The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package collector

import (
    "net/http"
    "net/http/httptest"
    "net/url"
    "strings"
    "testing"
    "time"

    "github.com/prometheus/client_golang/prometheus/testutil"
    "github.com/prometheus/common/promslog"
)

func TestLargeIndices(t *testing.T) {
    oldPattern := *largeIndicesPattern
    oldThreshold := *largeIndicesThresholdBytes
    oldMaxIndices := *largeIndicesMaxIndices
    oldCacheTTL := *largeIndicesCacheTTL
    t.Cleanup(func() {
        *largeIndicesPattern = oldPattern
        *largeIndicesThresholdBytes = oldThreshold
        *largeIndicesMaxIndices = oldMaxIndices
        *largeIndicesCacheTTL = oldCacheTTL
    })

    *largeIndicesPattern = ".ds-*"
    *largeIndicesThresholdBytes = 1000
    *largeIndicesMaxIndices = 2
    *largeIndicesCacheTTL = time.Minute

    response := `[
      {"index":".ds-foo-2026.03.18-000001","pri.store.size":"1500","store.size":"3000"},
      {"index":".ds-bar-000001","pri.store.size":"1200","store.size":"2500"},
      {"index":".ds-baz-000001","pri.store.size":"1100","store.size":"2000"},
      {"index":".ds-small-000001","pri.store.size":"100","store.size":"500"}
    ]`

    ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        if !strings.HasPrefix(r.URL.Path, "/_cat/indices/") {
            http.Error(w, "Not Found", http.StatusNotFound)
            return
        }
        _, _ = w.Write([]byte(response))
    }))
    defer ts.Close()

    u, err := url.Parse(ts.URL)
    if err != nil {
        t.Fatal(err)
    }

    c, err := NewLargeIndices(promslog.NewNopLogger(), u, http.DefaultClient)
    if err != nil {
        t.Fatal(err)
    }

    want := `# HELP elasticsearch_large_index_dropped_total Number of large indices filtered out because the collector max-indices limit was reached.
# TYPE elasticsearch_large_index_dropped_total gauge
elasticsearch_large_index_dropped_total 1
# HELP elasticsearch_large_index_monitored_total Number of large indices exported after filtering and limiting.
# TYPE elasticsearch_large_index_monitored_total gauge
elasticsearch_large_index_monitored_total 2
# HELP elasticsearch_large_index_primary_store_size_bytes Current primary store size in bytes for large indices.
# TYPE elasticsearch_large_index_primary_store_size_bytes gauge
elasticsearch_large_index_primary_store_size_bytes{data_stream="foo",index=".ds-foo-2026.03.18-000001"} 1500
elasticsearch_large_index_primary_store_size_bytes{data_stream="bar",index=".ds-bar-000001"} 1200
# HELP elasticsearch_large_index_store_size_bytes Current total store size in bytes for large indices.
# TYPE elasticsearch_large_index_store_size_bytes gauge
elasticsearch_large_index_store_size_bytes{data_stream="foo",index=".ds-foo-2026.03.18-000001"} 3000
elasticsearch_large_index_store_size_bytes{data_stream="bar",index=".ds-bar-000001"} 2500
`

    if err := testutil.CollectAndCompare(wrapCollector{c}, strings.NewReader(want)); err != nil {
        t.Fatal(err)
    }
}
