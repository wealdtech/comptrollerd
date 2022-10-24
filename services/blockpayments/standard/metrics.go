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
var betterBids *prometheus.GaugeVec
var inaccurateBids *prometheus.GaugeVec

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
	betterBids = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: "blockpayments",
		Name:      "better_bids_total",
		Help:      "Number of better bids seen",
	}, []string{"relay"})
	if err := prometheus.Register(betterBids); err != nil {
		return errors.Wrap(err, "failed to register better bids")
	}
	inaccurateBids = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: "blockpayments",
		Name:      "inaccurate_bids_total",
		Help:      "Number of inaccurate bids seen",
	}, []string{"relay"})
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

func monitorBetterBid(relay string, _ phase0.Slot) {
	if betterBids != nil {
		betterBids.WithLabelValues(relay).Inc()
	}
}

func monitorInaccurateBid(relay string, _ phase0.Slot) {
	if inaccurateBids != nil {
		inaccurateBids.WithLabelValues(relay).Inc()
	}
}
