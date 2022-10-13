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
	"bytes"
	"context"
	"fmt"
	"time"

	relayclient "github.com/attestantio/go-relay-client"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	zerologger "github.com/rs/zerolog/log"
	"github.com/wealdtech/comptrollerd/services/comptrollerdb"
	"github.com/wealdtech/comptrollerd/services/scheduler"
	"golang.org/x/sync/semaphore"
)

// Service is a chain database service.
type Service struct {
	scheduler                      scheduler.Service
	queuedProposersProviders       []relayclient.QueuedProposersProvider
	validatorRegistrationsProvider comptrollerdb.ValidatorRegistrationsProvider
	validatorRegistrationsSetter   comptrollerdb.ValidatorRegistrationsSetter
	interval                       time.Duration
	activitySem                    *semaphore.Weighted
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
	log = zerologger.With().Str("service", "relays").Str("impl", "standard").Logger().Level(parameters.logLevel)

	if err := registerMetrics(ctx, parameters.monitor); err != nil {
		return nil, errors.New("failed to register metrics")
	}

	s := &Service{
		scheduler:                      parameters.scheduler,
		queuedProposersProviders:       parameters.queuedProposersProviders,
		validatorRegistrationsProvider: parameters.validatorRegistrationsProvider,
		validatorRegistrationsSetter:   parameters.validatorRegistrationsSetter,
		interval:                       parameters.interval,
		activitySem:                    semaphore.NewWeighted(1),
	}

	runtimeFunc := func(ctx context.Context, data interface{}) (time.Time, error) {
		return time.Now().Add(s.interval), nil
	}

	if err := s.scheduler.SchedulePeriodicJob(ctx,
		"Queued proposers",
		"Fetch queued proposers",
		runtimeFunc,
		nil,
		s.updateOnScheduleTick,
		nil,
	); err != nil {
		log.Fatal().Err(err).Msg("Failed to schedule queued proposer updates.")
	}
	return s, nil
}

func (s *Service) updateOnScheduleTick(ctx context.Context, data interface{}) {
	// Only allow 1 handler to be active.
	acquired := s.activitySem.TryAcquire(1)
	if !acquired {
		log.Debug().Msg("Another handler running")
		return
	}
	defer s.activitySem.Release(1)

	s.updateQueuedProposersProviders(ctx)
}

func (s *Service) updateQueuedProposersProviders(ctx context.Context) {
	for _, provider := range s.queuedProposersProviders {
		log := log.With().Str("relay", provider.Address()).Logger()
		proposers, err := provider.QueuedProposers(ctx)
		if err != nil {
			log.Warn().Err(err).Msg("Failed to obtain queued proposers for update")
			monitorRelayActive(provider.Address(), false)
			continue
		}
		if len(proposers) == 0 {
			log.Trace().Msg("No proposers")
			// We consider a provider with no proposers to be inactive.
			monitorRelayActive(provider.Address(), false)
			continue
		}
		monitorRelayActive(provider.Address(), true)

		for _, proposer := range proposers {
			slot := uint32(proposer.Slot)
			// See if we already have this in the database.
			filter := &comptrollerdb.ValidatorRegistrationFilter{
				Relays:   []string{provider.Address()},
				FromSlot: &slot,
				ToSlot:   &slot,
			}
			registrations, err := s.validatorRegistrationsProvider.ValidatorRegistrations(ctx, filter)
			if err != nil {
				log.Warn().Err(err).Msg("Failed to obtain validator registration")
				// Continue as we may fix the error with an update.
			}
			if len(registrations) == 1 &&
				bytes.Equal(registrations[0].FeeRecipient, proposer.Entry.Message.FeeRecipient[:]) &&
				registrations[0].GasLimit == proposer.Entry.Message.GasLimit {
				log.Trace().Uint32("slot", slot).Str("fee_recipient", fmt.Sprintf("%#x", registrations[0].FeeRecipient)).Uint64("gas_limit", registrations[0].GasLimit).Msg("Duplicate; ignoring")
				monitorRegistrationsProcessed(provider.Address())
				continue
			}

			ctx, cancel, err := s.validatorRegistrationsSetter.BeginTx(ctx)
			if err != nil {
				log.Error().Err(err).Msg("Failed to begin transaction")
				continue
			}

			if err := s.validatorRegistrationsSetter.SetValidatorRegistration(ctx, &comptrollerdb.ValidatorRegistration{
				Slot:            slot,
				Relay:           provider.Address(),
				ValidatorPubkey: proposer.Entry.Message.Pubkey[:],
				FeeRecipient:    proposer.Entry.Message.FeeRecipient[:],
				GasLimit:        proposer.Entry.Message.GasLimit,
				Timestamp:       proposer.Entry.Message.Timestamp,
				Signature:       proposer.Entry.Signature[:],
			}); err != nil {
				cancel()
				log.Error().Err(err).Msg("Failed to set validator registration")
				continue
			}
			if err := s.validatorRegistrationsSetter.CommitTx(ctx); err != nil {
				cancel()
				log.Error().Err(err).Msg("Failed to commit transaction")
				continue
			}
			log.Trace().Str("relay", provider.Address()).Uint64("slot", uint64(proposer.Slot)).Str("pubkey", fmt.Sprintf("%#x", proposer.Entry.Message.Pubkey)).Str("fee_recipient", fmt.Sprintf("%#x", proposer.Entry.Message.FeeRecipient)).Msg("Stored validator registration")
			monitorRegistrationsProcessed(provider.Address())
		}
	}
}
