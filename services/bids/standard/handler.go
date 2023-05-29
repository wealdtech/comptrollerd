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
	"encoding/json"
	"sync"
	"sync/atomic"

	"github.com/attestantio/go-eth2-client/spec/phase0"
	relayclient "github.com/attestantio/go-relay-client"
	"github.com/pkg/errors"
	"github.com/wealdtech/comptrollerd/services/comptrollerdb"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func (s *Service) catchup(ctx context.Context, md *metadata) {
	// We fetch up to, but not including, the current slot.
	currentSlot := s.chainTime.CurrentSlot()
	for slot := phase0.Slot(md.LatestSlot + 1); slot < currentSlot; slot++ {
		if err := s.catchupSlot(ctx, md, slot); err != nil {
			log.Error().Uint64("slot", uint64(slot)).Err(err).Msg("Failed to catchup for slot")
			return
		}
	}
}

func (s *Service) catchupSlot(ctx context.Context, md *metadata, slot phase0.Slot) error {
	ctx, span := otel.Tracer("wealdtech.comptrollerd.services.bids.standard").Start(ctx, "catchupSlot", trace.WithAttributes(
		attribute.Int64("slot", int64(slot)),
	))
	defer span.End()

	log := log.With().Uint64("slot", uint64(slot)).Logger()
	log.Trace().Msg("Handling slot")

	// We need to keep a version of the context unencumbered by a transaction for handlers,
	// so scope the piece where the transaction lives.
	{
		ctx, cancel, err := s.receivedBidsSetter.BeginTx(ctx)
		if err != nil {
			return errors.Wrap(err, "failed to begin transaction")
		}

		if err := s.handleSlot(ctx, slot); err != nil {
			cancel()
			return errors.Wrap(err, "failed to handle slot")
		}

		md.LatestSlot = int64(slot)
		if err := s.setMetadata(ctx, md); err != nil {
			cancel()
			return errors.Wrap(err, "failed to set metadata")
		}

		if err := s.receivedBidsSetter.CommitTx(ctx); err != nil {
			cancel()
			return errors.Wrap(err, "failed to commit transaction")
		}
	}

	for _, relay := range s.receivedBidTracesProviders {
		monitorRelayUpdated(relay.Address())
	}
	for _, relay := range s.deliveredBidTraceProviders {
		monitorRelayUpdated(relay.Address())
	}

	for _, handler := range s.bidsReceivedHandlers {
		log.Trace().Msg("Notifying handler")
		go handler.BidsReceived(ctx, slot)
	}

	return nil
}

func (s *Service) handleSlot(ctx context.Context, slot phase0.Slot) error {
	ctx, span := otel.Tracer("wealdtech.comptrollerd.services.bids.standard").Start(ctx, "handleSlot", trace.WithAttributes(
		attribute.Int64("slot", int64(slot)),
	))
	defer span.End()

	if err := s.handleReceivedBids(ctx, slot); err != nil {
		return errors.Wrap(err, "failed to handle received bids")
	}
	if err := s.handleDeliveredBids(ctx, slot); err != nil {
		return errors.Wrap(err, "failed to handle delivered bids")
	}

	return nil
}

func (s *Service) handleReceivedBids(ctx context.Context, slot phase0.Slot) error {
	var wg sync.WaitGroup
	errs := int32(0)
	bids := make([]*comptrollerdb.ReceivedBid, 0)
	bidsMu := sync.Mutex{}
	for _, provider := range s.receivedBidTracesProviders {
		wg.Add(1)
		go func(ctx context.Context,
			wg *sync.WaitGroup,
			provider relayclient.ReceivedBidTracesProvider,
			bidsMu *sync.Mutex,
		) {
			defer wg.Done()
			traces, err := provider.ReceivedBidTraces(ctx, slot)
			if err != nil {
				log.Error().Str("provider", provider.Address()).Uint64("slot", uint64(slot)).Err(err).Msg("Failed to obtain received bid traces")
				atomic.AddInt32(&errs, 1)
				return
			}
			data, err := json.Marshal(traces)
			if err != nil {
				log.Error().Str("provider", provider.Address()).Uint64("slot", uint64(slot)).Err(err).Msg("Failed to unmarshal received bid traces")
				atomic.AddInt32(&errs, 1)
				return
			}
			log.Trace().Str("provider", provider.Address()).Uint64("slot", uint64(slot)).RawJSON("bid_traces", data).Msg("Obtained received bid traces")

			bidsMu.Lock()
			for _, trace := range traces {
				bids = append(bids, &comptrollerdb.ReceivedBid{
					Slot:                 uint32(trace.Slot),
					Relay:                provider.Address(),
					ParentHash:           trace.ParentHash[:],
					BlockHash:            trace.BlockHash[:],
					BuilderPubkey:        trace.BuilderPubkey[:],
					Timestamp:            trace.Timestamp,
					ProposerPubkey:       trace.ProposerPubkey[:],
					ProposerFeeRecipient: trace.ProposerFeeRecipient[:],
					GasLimit:             trace.GasLimit,
					GasUsed:              trace.GasUsed,
					Value:                trace.Value,
				})
			}
			bidsMu.Unlock()
		}(ctx, &wg, provider, &bidsMu)
	}
	wg.Wait()
	if errs > 0 {
		return errors.New("failed to handle slot")
	}

	if err := s.receivedBidsSetter.SetReceivedBids(ctx, bids); err != nil {
		return errors.Wrap(err, "failed to set received bids")
	}

	return nil
}

func (s *Service) handleDeliveredBids(ctx context.Context, slot phase0.Slot) error {
	var wg sync.WaitGroup
	errs := int32(0)
	bids := make([]*comptrollerdb.DeliveredBid, 0)
	bidsMu := sync.Mutex{}
	for _, provider := range s.deliveredBidTraceProviders {
		wg.Add(1)
		go func(ctx context.Context,
			wg *sync.WaitGroup,
			provider relayclient.DeliveredBidTraceProvider,
			bidsMu *sync.Mutex,
		) {
			defer wg.Done()
			trace, err := provider.DeliveredBidTrace(ctx, slot)
			if err != nil {
				log.Error().Str("provider", provider.Address()).Uint64("slot", uint64(slot)).Err(err).Msg("Failed to obtain delivered bid trace")
				atomic.AddInt32(&errs, 1)
				return
			}
			if trace == nil {
				log.Trace().Str("provider", provider.Address()).Uint64("slot", uint64(slot)).Msg("No delivered bid trace")
				return
			}
			data, err := json.Marshal(trace)
			if err != nil {
				log.Error().Str("provider", provider.Address()).Uint64("slot", uint64(slot)).Err(err).Msg("Failed to unmarshal delivered bid trace")
				atomic.AddInt32(&errs, 1)
				return
			}
			log.Trace().Str("provider", provider.Address()).Uint64("slot", uint64(slot)).RawJSON("bid_trace", data).Msg("Obtained delivered bid trace")

			bidsMu.Lock()
			bids = append(bids, &comptrollerdb.DeliveredBid{
				Slot:                 uint32(trace.Slot),
				Relay:                provider.Address(),
				ParentHash:           trace.ParentHash[:],
				BlockHash:            trace.BlockHash[:],
				BuilderPubkey:        trace.BuilderPubkey[:],
				ProposerPubkey:       trace.ProposerPubkey[:],
				ProposerFeeRecipient: trace.ProposerFeeRecipient[:],
				GasLimit:             trace.GasLimit,
				GasUsed:              trace.GasUsed,
				Value:                trace.Value,
			})
			bidsMu.Unlock()
		}(ctx, &wg, provider, &bidsMu)
	}
	wg.Wait()
	if errs > 0 {
		return errors.New("failed to handle slot")
	}

	for _, bid := range bids {
		if err := s.deliveredBidsSetter.SetDeliveredBid(ctx, bid); err != nil {
			log.Error().Str("provider", bid.Relay).Uint64("slot", uint64(slot)).Err(err).Msg("Failed to set delivered bid")
			return errors.Wrap(err, "failed to set delivered bid")
		}
	}

	return nil
}
