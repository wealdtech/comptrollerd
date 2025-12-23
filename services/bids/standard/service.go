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
	"time"

	relayclient "github.com/attestantio/go-relay-client"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	zerologger "github.com/rs/zerolog/log"
	"github.com/wealdtech/comptrollerd/services/bids"
	"github.com/wealdtech/comptrollerd/services/chaintime"
	"github.com/wealdtech/comptrollerd/services/comptrollerdb"
	"github.com/wealdtech/comptrollerd/services/scheduler"
	"golang.org/x/sync/semaphore"
)

// Service is a chain database service.
type Service struct {
	scheduler                  scheduler.Service
	chainTime                  chaintime.Service
	receivedBidTracesProviders []relayclient.ReceivedBidTracesProvider
	receivedBidsSetter         comptrollerdb.ReceivedBidsSetter
	deliveredBidTraceProviders []relayclient.DeliveredBidTraceProvider
	deliveredBidsSetter        comptrollerdb.DeliveredBidsSetter
	bidsReceivedHandlers       []bids.ReceivedHandler
	interval                   time.Duration
	activitySem                *semaphore.Weighted
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
	log = zerologger.With().Str("service", "bids").Str("impl", "standard").Logger().Level(parameters.logLevel)

	if err := registerMetrics(ctx, parameters.monitor); err != nil {
		return nil, errors.New("failed to register metrics")
	}

	s := &Service{
		scheduler:                  parameters.scheduler,
		chainTime:                  parameters.chainTime,
		receivedBidTracesProviders: parameters.receivedBidTracesProviders,
		receivedBidsSetter:         parameters.receivedBidsSetter,
		deliveredBidTraceProviders: parameters.deliveredBidTraceProviders,
		deliveredBidsSetter:        parameters.deliveredBidsSetter,
		bidsReceivedHandlers:       parameters.bidsReceivedHandlers,
		interval:                   parameters.interval,
		activitySem:                semaphore.NewWeighted(1),
	}

	if err := s.onStart(ctx, parameters.startSlot); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Service) onStart(ctx context.Context,
	startSlot int64,
) error {
	md, err := s.getMetadata(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to obtain metadata")
	}

	if startSlot >= 0 {
		err := updateStartSlot(ctx, s, md, startSlot)
		if err != nil {
			return err
		}
	}

	log.Trace().Msg("Running initial catchup")
	s.catchup(ctx, md)
	log.Trace().Msg("Finished initial catchup")

	runtimeFunc := func(_ context.Context, _ any) (time.Time, error) {
		return time.Now().Add(s.interval), nil
	}

	if err := s.scheduler.SchedulePeriodicJob(ctx,
		"bids",
		"Obtain new bid information",
		runtimeFunc,
		nil,
		s.onTick,
		nil,
	); err != nil {
		return errors.Wrap(err, "failed to schedule bid updates")
	}

	return nil
}

func updateStartSlot(ctx context.Context, s *Service, md *metadata, startSlot int64) error {
	md.mu.Lock()
	defer md.mu.Unlock()

	opCtx, cancel, err := s.receivedBidsSetter.BeginTx(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to begin transaction for metadata update")
	}

	for _, prov := range s.receivedBidTracesProviders {
		name := prov.Name()
		if cur, ok := md.LatestSlots[name]; !ok || cur < startSlot-1 {
			md.LatestSlots[name] = startSlot - 1
		}
	}

	if err := s.setMetadata(ctx, md); err != nil {
		return errors.Wrap(err, "failed to set metadata")
	}

	if err := s.receivedBidsSetter.CommitTx(opCtx); err != nil {
		cancel()
		return errors.Wrap(err, "failed to commit metadata transaction")
	}

	return nil
}

func (s *Service) onTick(ctx context.Context, _ any) {
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
