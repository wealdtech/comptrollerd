// Copyright © 2022, 2024 Weald Technology Trading.
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
	"errors"
	"time"

	relayclient "github.com/attestantio/go-relay-client"
	"github.com/rs/zerolog"
	"github.com/wealdtech/comptrollerd/services/bids"
	"github.com/wealdtech/comptrollerd/services/chaintime"
	"github.com/wealdtech/comptrollerd/services/comptrollerdb"
	"github.com/wealdtech/comptrollerd/services/metrics"
	"github.com/wealdtech/comptrollerd/services/scheduler"
)

type parameters struct {
	logLevel                   zerolog.Level
	monitor                    metrics.Service
	scheduler                  scheduler.Service
	chainTime                  chaintime.Service
	receivedBidTracesProviders []relayclient.ReceivedBidTracesProvider
	receivedBidsSetter         comptrollerdb.ReceivedBidsSetter
	deliveredBidTraceProviders []relayclient.DeliveredBidTraceProvider
	deliveredBidsSetter        comptrollerdb.DeliveredBidsSetter
	bidsReceivedHandlers       []bids.ReceivedHandler
	interval                   time.Duration
	startSlot                  int64
}

// Parameter is the interface for service parameters.
type Parameter interface {
	apply(p *parameters)
}

type parameterFunc func(*parameters)

func (f parameterFunc) apply(p *parameters) {
	f(p)
}

// WithLogLevel sets the log level for the module.
func WithLogLevel(logLevel zerolog.Level) Parameter {
	return parameterFunc(func(p *parameters) {
		p.logLevel = logLevel
	})
}

// WithMonitor sets the monitor for the module.
func WithMonitor(monitor metrics.Service) Parameter {
	return parameterFunc(func(p *parameters) {
		p.monitor = monitor
	})
}

// WithScheduler sets the scheduler for the module.
func WithScheduler(schedulerSvc scheduler.Service) Parameter {
	return parameterFunc(func(p *parameters) {
		p.scheduler = schedulerSvc
	})
}

// WithChainTime sets the chain time service for the module.
func WithChainTime(service chaintime.Service) Parameter {
	return parameterFunc(func(p *parameters) {
		p.chainTime = service
	})
}

// WithReceivedBidTracesProviders sets the providers for traces of bids received by relays.
func WithReceivedBidTracesProviders(providers []relayclient.ReceivedBidTracesProvider) Parameter {
	return parameterFunc(func(p *parameters) {
		p.receivedBidTracesProviders = providers
	})
}

// WithReceivedBidsSetter sets the setter for received bids.
func WithReceivedBidsSetter(setter comptrollerdb.ReceivedBidsSetter) Parameter {
	return parameterFunc(func(p *parameters) {
		p.receivedBidsSetter = setter
	})
}

// WithDeliveredBidTraceProviders sets the providers for traces of bids delivered to providers.
func WithDeliveredBidTraceProviders(providers []relayclient.DeliveredBidTraceProvider) Parameter {
	return parameterFunc(func(p *parameters) {
		p.deliveredBidTraceProviders = providers
	})
}

// WithDeliveredBidsSetter sets the setter for delivered bids.
func WithDeliveredBidsSetter(setter comptrollerdb.DeliveredBidsSetter) Parameter {
	return parameterFunc(func(p *parameters) {
		p.deliveredBidsSetter = setter
	})
}

// WithBidsReceivedHandlers sets the handlers for received bids.
func WithBidsReceivedHandlers(handlers []bids.ReceivedHandler) Parameter {
	return parameterFunc(func(p *parameters) {
		p.bidsReceivedHandlers = handlers
	})
}

// WithInterval sets the interval between updates.
func WithInterval(interval time.Duration) Parameter {
	return parameterFunc(func(p *parameters) {
		p.interval = interval
	})
}

// WithStartSlot sets the start slot to obtain delivered bid traces.
func WithStartSlot(slot int64) Parameter {
	return parameterFunc(func(p *parameters) {
		p.startSlot = slot
	})
}

// parseAndCheckParameters parses and checks parameters to ensure that mandatory parameters are present and correct.
func parseAndCheckParameters(params ...Parameter) (*parameters, error) {
	parameters := parameters{
		logLevel:  zerolog.GlobalLevel(),
		startSlot: -1,
	}
	for _, p := range params {
		if params != nil {
			p.apply(&parameters)
		}
	}

	if parameters.scheduler == nil {
		return nil, errors.New("no scheduler specified")
	}
	if parameters.chainTime == nil {
		return nil, errors.New("no chain time specified")
	}
	if len(parameters.receivedBidTracesProviders) == 0 {
		return nil, errors.New("no received bid traces providers specified")
	}
	if parameters.receivedBidsSetter == nil {
		return nil, errors.New("no received bids setter specified")
	}
	if len(parameters.deliveredBidTraceProviders) == 0 {
		return nil, errors.New("no delivered bid trace providers specified")
	}
	if parameters.deliveredBidsSetter == nil {
		return nil, errors.New("no delivered bids setter specified")
	}
	if parameters.interval == 0 {
		return nil, errors.New("no interval specified")
	}

	return &parameters, nil
}
