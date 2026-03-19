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
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/alecthomas/kingpin/v2"
	regexp "github.com/grafana/regexp"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	largeIndicesPattern = kingpin.Flag(
		"collector.large-indices.pattern",
		"Index pattern queried by the large indices collector.",
	).Default(".ds-*").String()
	largeIndicesThresholdBytes = kingpin.Flag(
		"collector.large-indices.threshold-bytes",
		"Minimum total store size in bytes for an index to be exposed by the large indices collector.",
	).Default("107374182400").Int64() // 100 GiB
	largeIndicesMaxIndices = kingpin.Flag(
		"collector.large-indices.max-indices",
		"Maximum number of indices exported by the large indices collector after filtering and sorting by size.",
	).Default("200").Int()
	largeIndicesCacheTTL = kingpin.Flag(
		"collector.large-indices.cache-ttl",
		"Cache TTL for the large indices collector.",
	).Default("5m").Duration()
)

var (
	largeIndexStoreSizeBytes = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "large_index", "store_size_bytes"),
		"Current total store size in bytes for large indices.",
		[]string{"index", "data_stream"},
		nil,
	)
	largeIndexPrimaryStoreSizeBytes = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "large_index", "primary_store_size_bytes"),
		"Current primary store size in bytes for large indices.",
		[]string{"index", "data_stream"},
		nil,
	)
	largeIndexMonitoredTotal = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "large_index", "monitored_total"),
		"Number of large indices exported after filtering and limiting.",
		nil,
		nil,
	)
	largeIndexDroppedTotal = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "large_index", "dropped_total"),
		"Number of large indices filtered out because the collector max-indices limit was reached.",
		nil,
		nil,
	)
)

var dataStreamIndexPattern = regexp.MustCompile(`^\.ds-(.+?)(?:-\d{4}\.\d{2}\.\d{2})?-\d+$`)

func init() {
	registerCollector("large-indices", defaultDisabled, NewLargeIndices)
}

type largeIndicesCatResponse struct {
	Index            string `json:"index"`
	PrimaryStoreSize string `json:"pri.store.size"`
	StoreSize        string `json:"store.size"`
}

type largeIndexMetric struct {
	Index                 string
	DataStream            string
	PrimaryStoreSizeBytes float64
	StoreSizeBytes        float64
}

type largeIndicesCacheSnapshot struct {
	Metrics   []largeIndexMetric
	Dropped   int
	ExpiresAt time.Time
}

// LargeIndices exports metrics only for indices over a configured threshold.
type LargeIndices struct {
	logger *slog.Logger
	hc     *http.Client
	u      *url.URL

	mu    sync.Mutex
	cache largeIndicesCacheSnapshot
}

func NewLargeIndices(logger *slog.Logger, u *url.URL, hc *http.Client) (Collector, error) {
	logger.Info(
		"large indices collector created",
		"pattern", *largeIndicesPattern,
		"threshold_bytes", *largeIndicesThresholdBytes,
		"max_indices", *largeIndicesMaxIndices,
		"cache_ttl", (*largeIndicesCacheTTL).String(),
	)

	return &LargeIndices{
		logger: logger,
		hc:     hc,
		u:      u,
	}, nil
}

func (li *LargeIndices) Update(ctx context.Context, ch chan<- prometheus.Metric) error {
	snapshot, err := li.getSnapshot(ctx)
	if err != nil {
		return err
	}

	for _, metric := range snapshot.Metrics {
		ch <- prometheus.MustNewConstMetric(
			largeIndexStoreSizeBytes,
			prometheus.GaugeValue,
			metric.StoreSizeBytes,
			metric.Index,
			metric.DataStream,
		)
		ch <- prometheus.MustNewConstMetric(
			largeIndexPrimaryStoreSizeBytes,
			prometheus.GaugeValue,
			metric.PrimaryStoreSizeBytes,
			metric.Index,
			metric.DataStream,
		)
	}

	ch <- prometheus.MustNewConstMetric(
		largeIndexMonitoredTotal,
		prometheus.GaugeValue,
		float64(len(snapshot.Metrics)),
	)
	ch <- prometheus.MustNewConstMetric(
		largeIndexDroppedTotal,
		prometheus.GaugeValue,
		float64(snapshot.Dropped),
	)

	return nil
}

func (li *LargeIndices) getSnapshot(ctx context.Context) (largeIndicesCacheSnapshot, error) {
	li.mu.Lock()
	defer li.mu.Unlock()

	now := time.Now()
	if !li.cache.ExpiresAt.IsZero() && now.Before(li.cache.ExpiresAt) {
		return cloneLargeIndicesSnapshot(li.cache), nil
	}

	snapshot, err := li.fetchSnapshot(ctx)
	if err != nil {
		return largeIndicesCacheSnapshot{}, err
	}

	snapshot.ExpiresAt = now.Add(*largeIndicesCacheTTL)
	li.cache = cloneLargeIndicesSnapshot(snapshot)

	return cloneLargeIndicesSnapshot(snapshot), nil
}

func (li *LargeIndices) fetchSnapshot(ctx context.Context) (largeIndicesCacheSnapshot, error) {
	catURL := li.u.ResolveReference(&url.URL{Path: "/_cat/indices/" + *largeIndicesPattern})
	query := catURL.Query()
	query.Set("format", "json")
	query.Set("bytes", "b")
	query.Set("h", "index,pri.store.size,store.size")
	query.Set("expand_wildcards", "open")
	catURL.RawQuery = query.Encode()

	resp, err := getURL(ctx, li.hc, li.logger, catURL.String())
	if err != nil {
		return largeIndicesCacheSnapshot{}, fmt.Errorf("failed to fetch cat indices response: %w", err)
	}

	var rows []largeIndicesCatResponse
	if err := json.Unmarshal(resp, &rows); err != nil {
		return largeIndicesCacheSnapshot{}, fmt.Errorf("failed to decode cat indices response: %w", err)
	}

	filtered := make([]largeIndexMetric, 0, len(rows))
	for _, row := range rows {
		storeSizeBytes, err := parseCatBytes(row.StoreSize)
		if err != nil {
			li.logger.Debug("skipping large index metric due to invalid store.size", "index", row.Index, "value", row.StoreSize, "err", err)
			continue
		}
		if storeSizeBytes < *largeIndicesThresholdBytes {
			continue
		}

		primaryStoreSizeBytes, err := parseCatBytes(row.PrimaryStoreSize)
		if err != nil {
			li.logger.Debug("skipping large index metric due to invalid pri.store.size", "index", row.Index, "value", row.PrimaryStoreSize, "err", err)
			continue
		}

		filtered = append(filtered, largeIndexMetric{
			Index:                 row.Index,
			DataStream:            extractDataStreamName(row.Index),
			PrimaryStoreSizeBytes: float64(primaryStoreSizeBytes),
			StoreSizeBytes:        float64(storeSizeBytes),
		})
	}

	slices.SortFunc(filtered, func(a, b largeIndexMetric) int {
		if a.StoreSizeBytes == b.StoreSizeBytes {
			return compareStrings(a.Index, b.Index)
		}
		if a.StoreSizeBytes > b.StoreSizeBytes {
			return -1
		}
		return 1
	})

	dropped := 0
	if *largeIndicesMaxIndices > 0 && len(filtered) > *largeIndicesMaxIndices {
		dropped = len(filtered) - *largeIndicesMaxIndices
		filtered = filtered[:*largeIndicesMaxIndices]
	}

	return largeIndicesCacheSnapshot{
		Metrics: filtered,
		Dropped: dropped,
	}, nil
}

func parseCatBytes(value string) (int64, error) {
	if value == "" || value == "-" {
		return 0, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, err
	}
	return parsed, nil
}

func extractDataStreamName(indexName string) string {
	matches := dataStreamIndexPattern.FindStringSubmatch(indexName)
	if len(matches) != 2 {
		return ""
	}
	return matches[1]
}

func cloneLargeIndicesSnapshot(snapshot largeIndicesCacheSnapshot) largeIndicesCacheSnapshot {
	cloned := snapshot
	cloned.Metrics = slices.Clone(snapshot.Metrics)
	return cloned
}

func compareStrings(a string, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}
