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

	for _, queuedProposersProvider := range s.queuedProposersProviders {
		log := log.With().Str("relay", queuedProposersProvider.Address()).Logger()
		queuedProposers, err := queuedProposersProvider.QueuedProposers(ctx)
		if err != nil {
			log.Warn().Err(err).Msg("Failed to obtain queued proposers for update")
			continue
		}

		for _, queuedProposer := range queuedProposers {
			slot := uint32(queuedProposer.Slot)
			// See if we already have this in the database.
			filter := &comptrollerdb.ValidatorRegistrationFilter{
				Relays:   []string{queuedProposersProvider.Address()},
				FromSlot: &slot,
				ToSlot:   &slot,
			}
			registrations, err := s.validatorRegistrationsProvider.ValidatorRegistrations(ctx, filter)
			if err != nil {
				log.Warn().Err(err).Msg("Failed to obtain validator registration")
				// Continue as we may fix the error with an update.
			}
			if len(registrations) == 1 &&
				bytes.Equal(registrations[0].FeeRecipient, queuedProposer.Entry.Message.FeeRecipient[:]) &&
				registrations[0].GasLimit == queuedProposer.Entry.Message.GasLimit {
				log.Trace().Uint32("slot", slot).Str("fee_recipient", fmt.Sprintf("%#x", registrations[0].FeeRecipient)).Uint64("gas_limit", registrations[0].GasLimit).Msg("Duplicate; ignoring")
				continue
			} else {
				ctx, cancel, err := s.validatorRegistrationsSetter.BeginTx(ctx)
				if err != nil {
					log.Error().Err(err).Msg("Failed to begin transaction")
					continue
				}

				if err := s.validatorRegistrationsSetter.SetValidatorRegistration(ctx, &comptrollerdb.ValidatorRegistration{
					Slot:            slot,
					Relay:           queuedProposersProvider.Address(),
					ValidatorPubkey: queuedProposer.Entry.Message.Pubkey[:],
					FeeRecipient:    queuedProposer.Entry.Message.FeeRecipient[:],
					GasLimit:        queuedProposer.Entry.Message.GasLimit,
					Timestamp:       queuedProposer.Entry.Message.Timestamp,
					Signature:       queuedProposer.Entry.Signature[:],
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
				log.Trace().Str("relay", queuedProposersProvider.Address()).Uint64("slot", uint64(queuedProposer.Slot)).Str("pubkey", fmt.Sprintf("%#x", queuedProposer.Entry.Message.Pubkey)).Str("fee_recipient", fmt.Sprintf("%#x", queuedProposer.Entry.Message.FeeRecipient)).Msg("Stored validator registration")
			}
			monitorRegistrationsProcessed(queuedProposersProvider.Address())
		}
	}
}
