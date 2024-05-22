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
	"os"

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	zerologger "github.com/rs/zerolog/log"
	"github.com/wealdtech/comptrollerd/services/chaintime"
	"github.com/wealdtech/comptrollerd/services/comptrollerdb"
	"github.com/wealdtech/execd/services/execdb"
	"golang.org/x/sync/semaphore"
)

// Service is a chain database service.
type Service struct {
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
	activitySem                       *semaphore.Weighted
}

// module-wide log.
var log zerolog.Logger

// New creates a new service.
func New(ctx context.Context, params ...Parameter) (*Service, error) {
	parameters, err := parseAndCheckParameters(params...)
	if err != nil {
		return nil, errors.Wrap(err, "problem with parameters")
	}

	// Set logging.
	log = zerologger.With().Str("service", "blockpayments").Str("impl", "standard").Logger().Level(parameters.logLevel)

	if err := registerMetrics(ctx, parameters.monitor); err != nil {
		return nil, errors.New("failed to register metrics")
	}

	s := &Service{
		chainTime:                         parameters.chainTime,
		receivedBidsProvider:              parameters.receivedBidsProvider,
		deliveredBidsProvider:             parameters.deliveredBidsProvider,
		blocksProvider:                    parameters.blocksProvider,
		balancesProvider:                  parameters.balancesProvider,
		transactionsProvider:              parameters.transactionsProvider,
		transactionBalanceChangesProvider: parameters.transactionBalanceChangesProvider,
		blockPaymentsSetter:               parameters.blockPaymentsSetter,
		alternateBidsSetter:               parameters.alternateBidsSetter,
		trackDistance:                     parameters.trackDistance,
		activitySem:                       semaphore.NewWeighted(1),
	}

	if parameters.replaySlot >= 0 {
		if err := s.onReplaySlot(ctx, phase0.Slot(parameters.replaySlot)); err != nil {
			return nil, err
		}
		//nolint:revive
		os.Exit(0)
	}

	if err := s.onStart(ctx, parameters.startSlot); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Service) onReplaySlot(ctx context.Context, slot phase0.Slot) error {
	ctx, cancel, err := s.blockPaymentsSetter.BeginTx(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to begin transaction")
	}

	if err := s.handleSlot(ctx, slot); err != nil {
		cancel()
		return errors.Wrap(err, "Failed to replay slot")
	}

	if err := s.blockPaymentsSetter.CommitTx(ctx); err != nil {
		cancel()
		return errors.Wrap(err, "failed to commit transaction")
	}

	return nil
}

func (s *Service) onStart(ctx context.Context,
	startSlot int64,
) error {
	if startSlot >= 0 {
		md, err := s.getMetadata(ctx)
		if err != nil {
			return errors.Wrap(err, "failed to obtain metadata")
		}
		md.LatestSlot = startSlot - 1

		ctx, cancel, err := s.blockPaymentsSetter.BeginTx(ctx)
		if err != nil {
			return errors.Wrap(err, "failed to begin transaction")
		}

		if err := s.setMetadata(ctx, md); err != nil {
			cancel()
			return errors.Wrap(err, "failed to set metadata")
		}

		if err := s.blockPaymentsSetter.CommitTx(ctx); err != nil {
			cancel()
			return errors.Wrap(err, "failed to commit transaction")
		}
	}

	return nil
}

// BidsReceived is called whenever the bids for a given slot have been obtained.
func (s *Service) BidsReceived(ctx context.Context, _ phase0.Slot) {
	// Only allow 1 handler to be active.
	acquired := s.activitySem.TryAcquire(1)
	if !acquired {
		log.Debug().Msg("Another handler running")
		return
	}
	defer s.activitySem.Release(1)

	md, err := s.getMetadata(ctx)
	if err != nil {
		log.Error().Err(err).Msg("Failed to obtain metadata")
		return
	}

	s.catchup(ctx, md)
}
