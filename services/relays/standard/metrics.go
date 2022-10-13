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

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/wealdtech/comptrollerd/services/metrics"
)

var metricsNamespace = "comptrollerd"

var latestTime *prometheus.GaugeVec
var relayActive *prometheus.GaugeVec

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
	latestTime = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: "relays",
		Name:      "latest_ts",
		Help:      "Timestamp of last update",
	}, []string{"relay"})
	if err := prometheus.Register(latestTime); err != nil {
		return errors.Wrap(err, "failed to register latest timestamp")
	}

	relayActive = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: "relays",
		Name:      "active",
		Help:      "1 if active, 0 otherwise",
	}, []string{"relay"})
	if err := prometheus.Register(relayActive); err != nil {
		return errors.Wrap(err, "failed to register relay active")
	}

	return nil
}

func monitorRegistrationsProcessed(relay string) {
	if latestTime != nil {
		latestTime.WithLabelValues(relay).SetToCurrentTime()
	}
}

func monitorRelayActive(relay string, active bool) {
	if relayActive != nil {
		if active {
			relayActive.WithLabelValues(relay).Set(1)
		} else {
			relayActive.WithLabelValues(relay).Set(0)
		}
	}
}
