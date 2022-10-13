// Copyright © 2022 Weald Technology Trading.
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

package standard

import (
	"context"

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/wealdtech/comptrollerd/services/metrics"
)

var metricsNamespace = "comptrollerd"

var latestTime prometheus.Gauge
var poorBids prometheus.Gauge
var inaccurateBids prometheus.Gauge

func registerMetrics(ctx context.Context, monitor metrics.Service) error {
	if latestTime != nil {
		// Already registered.
		return nil
	}
	if monitor == nil {
		// No monitor.
		return nil
	}
	if monitor.Presenter() == "prometheus" {
		return registerPrometheusMetrics(ctx)
	}
	return nil
}

func registerPrometheusMetrics(ctx context.Context) error {
	latestTime = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: "blockpayments",
		Name:      "latest_ts",
		Help:      "Timestamp of last successful update",
	})
	if err := prometheus.Register(latestTime); err != nil {
		return errors.Wrap(err, "failed to register latest timestamp")
	}
	poorBids = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: "blockpayments",
		Name:      "poor_bids_total",
		Help:      "Number of poor bids seen",
	})
	if err := prometheus.Register(poorBids); err != nil {
		return errors.Wrap(err, "failed to register poor bids")
	}
	inaccurateBids = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: "blockpayments",
		Name:      "inaccurate_bids_total",
		Help:      "Number of inaccurate bids seen",
	})
	if err := prometheus.Register(inaccurateBids); err != nil {
		return errors.Wrap(err, "failed to register inaccurate bids")
	}

	return nil
}

func monitorPaymentsUpdated(_ phase0.Slot) {
	if latestTime != nil {
		latestTime.SetToCurrentTime()
	}
}

func monitorPoorBid(_ phase0.Slot) {
	if poorBids != nil {
		poorBids.Inc()
	}
}

func monitorInaccurateBid(_ phase0.Slot) {
	if inaccurateBids != nil {
		inaccurateBids.Inc()
	}
}
