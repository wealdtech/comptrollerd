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
	"github.com/wealdtech/comptrollerd/services/comptrollerdb"
	"github.com/wealdtech/comptrollerd/services/metrics"
	"github.com/wealdtech/comptrollerd/services/scheduler"
)

type parameters struct {
	logLevel                       zerolog.Level
	monitor                        metrics.Service
	scheduler                      scheduler.Service
	queuedProposersProviders       []relayclient.QueuedProposersProvider
	validatorRegistrationsProvider comptrollerdb.ValidatorRegistrationsProvider
	validatorRegistrationsSetter   comptrollerdb.ValidatorRegistrationsSetter
	interval                       time.Duration
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

// WithQueuedProposersProviders sets the providers for queued providers.
func WithQueuedProposersProviders(providers []relayclient.QueuedProposersProvider) Parameter {
	return parameterFunc(func(p *parameters) {
		p.queuedProposersProviders = providers
	})
}

// WithValidatorRegistrationsProvider sets the provider for valdiator registrations.
func WithValidatorRegistrationsProvider(provider comptrollerdb.ValidatorRegistrationsProvider) Parameter {
	return parameterFunc(func(p *parameters) {
		p.validatorRegistrationsProvider = provider
	})
}

// WithValidatorRegistrationsSetter sets the setter for valdiator registrations.
func WithValidatorRegistrationsSetter(setter comptrollerdb.ValidatorRegistrationsSetter) Parameter {
	return parameterFunc(func(p *parameters) {
		p.validatorRegistrationsSetter = setter
	})
}

// WithInterval sets the interval between updates.
func WithInterval(interval time.Duration) Parameter {
	return parameterFunc(func(p *parameters) {
		p.interval = interval
	})
}

// parseAndCheckParameters parses and checks parameters to ensure that mandatory parameters are present and correct.
func parseAndCheckParameters(params ...Parameter) (*parameters, error) {
	parameters := parameters{
		logLevel: zerolog.GlobalLevel(),
	}
	for _, p := range params {
		if params != nil {
			p.apply(&parameters)
		}
	}

	if parameters.scheduler == nil {
		return nil, errors.New("no scheduler specified")
	}
	if len(parameters.queuedProposersProviders) == 0 {
		return nil, errors.New("no queued proposers providers specified")
	}
	if parameters.validatorRegistrationsProvider == nil {
		return nil, errors.New("no validator registrations provider specified")
	}
	if parameters.validatorRegistrationsSetter == nil {
		return nil, errors.New("no validator registrations setter specified")
	}
	if parameters.interval == 0 {
		return nil, errors.New("no interval specified")
	}

	return &parameters, nil
}
