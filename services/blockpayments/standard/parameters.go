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
	"errors"

	"github.com/rs/zerolog"
	"github.com/wealdtech/comptrollerd/services/chaintime"
	"github.com/wealdtech/comptrollerd/services/comptrollerdb"
	"github.com/wealdtech/comptrollerd/services/metrics"
	"github.com/wealdtech/execd/services/execdb"
)

type parameters struct {
	logLevel                          zerolog.Level
	monitor                           metrics.Service
	chainTime                         chaintime.Service
	receivedBidsProvider              comptrollerdb.ReceivedBidsProvider
	deliveredBidsProvider             comptrollerdb.DeliveredBidsProvider
	blocksProvider                    execdb.BlocksProvider
	balancesProvider                  execdb.BalancesProvider
	transactionsProvider              execdb.TransactionsProvider
	transactionBalanceChangesProvider execdb.TransactionBalanceChangesProvider
	blockPaymentsSetter               comptrollerdb.BlockPaymentsSetter
	alternateBidsSetter               comptrollerdb.AlternateBidsSetter
	trackDistance                     uint32
	startSlot                         int64
	replaySlot                        int64
}

// Parameter is the interface for service parameters.
type Parameter interface {
	apply(*parameters)
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

// WithChainTime sets the chain time service for the module.
func WithChainTime(service chaintime.Service) Parameter {
	return parameterFunc(func(p *parameters) {
		p.chainTime = service
	})
}

// WithReceivedBidsProvider sets the providers for bids received by relays.
func WithReceivedBidsProvider(provider comptrollerdb.ReceivedBidsProvider) Parameter {
	return parameterFunc(func(p *parameters) {
		p.receivedBidsProvider = provider
	})
}

// WithDeliveredBidsProvider sets the providers for bids delivered by relays.
func WithDeliveredBidsProvider(provider comptrollerdb.DeliveredBidsProvider) Parameter {
	return parameterFunc(func(p *parameters) {
		p.deliveredBidsProvider = provider
	})
}

// WithBlockPaymentsSetter sets the setter for block payments calculated by this module.
func WithBlockPaymentsSetter(setter comptrollerdb.BlockPaymentsSetter) Parameter {
	return parameterFunc(func(p *parameters) {
		p.blockPaymentsSetter = setter
	})
}

// WithAlternateBidsSetter sets the setter for alternate bids obtained by this module.
func WithAlternateBidsSetter(setter comptrollerdb.AlternateBidsSetter) Parameter {
	return parameterFunc(func(p *parameters) {
		p.alternateBidsSetter = setter
	})
}

// WithTrackDistance sets the track distance for this module.
func WithTrackDistance(trackDistance uint32) Parameter {
	return parameterFunc(func(p *parameters) {
		p.trackDistance = trackDistance
	})
}

// WithStartSlot sets the start slot to calculate block payments.
func WithStartSlot(slot int64) Parameter {
	return parameterFunc(func(p *parameters) {
		p.startSlot = slot
	})
}

// WithReplaySlot sets a single slot to replay block payments.
func WithReplaySlot(slot int64) Parameter {
	return parameterFunc(func(p *parameters) {
		p.replaySlot = slot
	})
}

// WithBlocksProvider sets the blocks provider for this module.
func WithBlocksProvider(provider execdb.BlocksProvider) Parameter {
	return parameterFunc(func(p *parameters) {
		p.blocksProvider = provider
	})
}

// WithBalancesProvider sets the balances provider for this module.
func WithBalancesProvider(provider execdb.BalancesProvider) Parameter {
	return parameterFunc(func(p *parameters) {
		p.balancesProvider = provider
	})
}

// WithTransactionsProvider sets the transactions provider for this module.
func WithTransactionsProvider(provider execdb.TransactionsProvider) Parameter {
	return parameterFunc(func(p *parameters) {
		p.transactionsProvider = provider
	})
}

// WithTransactionBalanceChangesProvider sets the transaction balance change provider for this module.
func WithTransactionBalanceChangesProvider(provider execdb.TransactionBalanceChangesProvider) Parameter {
	return parameterFunc(func(p *parameters) {
		p.transactionBalanceChangesProvider = provider
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

	if parameters.chainTime == nil {
		return nil, errors.New("no chain time specified")
	}
	if parameters.receivedBidsProvider == nil {
		return nil, errors.New("no received bid provider specified")
	}
	if parameters.deliveredBidsProvider == nil {
		return nil, errors.New("no delivered bid provider specified")
	}
	if parameters.blocksProvider == nil {
		return nil, errors.New("no blocks provider specified")
	}
	if parameters.balancesProvider == nil {
		return nil, errors.New("no balances provider specified")
	}
	if parameters.transactionsProvider == nil {
		return nil, errors.New("no transactions provider specified")
	}
	if parameters.transactionBalanceChangesProvider == nil {
		return nil, errors.New("no transaction balance changes provider specified")
	}
	if parameters.blockPaymentsSetter == nil {
		return nil, errors.New("no block payments setter specified")
	}
	if parameters.alternateBidsSetter == nil {
		return nil, errors.New("no alternate bids setter specified")
	}

	return &parameters, nil
}
