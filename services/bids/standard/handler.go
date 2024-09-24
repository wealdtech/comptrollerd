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
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/pkg/errors"
	"github.com/wealdtech/comptrollerd/services/comptrollerdb"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func (s *Service) catchup(ctx context.Context, md *metadata) {
	log.Trace().Msg("Catching up")
	// We fetch up to, but not including, the current slot.
	var wg sync.WaitGroup
	for i, provider := range s.receivedBidTracesProviders {
		wg.Add(1)
		go s.catchupProvider(ctx, &wg, md, provider.Name(), i)
	}
	wg.Wait()
	log.Trace().Msg("Caught up")
}

func (s *Service) catchupProvider(ctx context.Context,
	wg *sync.WaitGroup,
	md *metadata,
	provider string,
	providerIndex int,
) {
	defer wg.Done()

	ctx, span := otel.Tracer("wealdtech.comptrollerd.services.bids.standard").Start(ctx, "catchupProvider", trace.WithAttributes(
		attribute.String("provider", provider),
	))
	defer span.End()

	log.Trace().Str("provider", provider).Int("provider_index", providerIndex).Msg("Catching up for provider")
	md.mu.RLock()
	firstSlot := phase0.Slot(md.LatestSlots[provider] + 1)
	md.mu.RUnlock()
	lastSlot := s.chainTime.CurrentSlot()
	for slot := firstSlot; slot < lastSlot; slot++ {
		if err := s.catchupProviderSlot(ctx, md, provider, providerIndex, slot); err != nil {
			log.Error().
				Uint64("slot", uint64(slot)).
				Str("provider", provider).
				Err(err).
				Msg("Failed to catchup for provider slot")

			return
		}
	}
	log.Trace().Str("provider", provider).Int("provider_index", providerIndex).Msg("Caught up for provider")
}

func (s *Service) catchupProviderSlot(ctx context.Context,
	md *metadata,
	provider string,
	providerIndex int,
	slot phase0.Slot,
) error {
	ctx, span := otel.Tracer("wealdtech.comptrollerd.services.bids.standard").
		Start(ctx, "catchupProviderSlot", trace.WithAttributes(
			attribute.Int64("slot", int64(slot)),
		))
	defer span.End()

	log := log.With().Uint64("slot", uint64(slot)).Logger()
	log.Trace().Msg("Handling slot")

	opCtx, cancel, err := s.receivedBidsSetter.BeginTx(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to begin transaction")
	}

	if err := s.handleSlot(opCtx, provider, providerIndex, slot); err != nil {
		cancel()
		return errors.Wrap(err, "failed to handle slot")
	}

	md.mu.Lock()
	md.LatestSlots[provider] = int64(slot)
	md.mu.Unlock()

	md.mu.RLock()
	if err := s.setMetadata(opCtx, md); err != nil {
		md.mu.RUnlock()
		cancel()
		return errors.Wrap(err, "failed to set metadata")
	}
	md.mu.RUnlock()

	if err := s.receivedBidsSetter.CommitTx(opCtx); err != nil {
		cancel()
		return errors.Wrap(err, "failed to commit transaction")
	}

	monitorRelayUpdated(provider)

	if len(s.bidsReceivedHandlers) > 0 {
		// We need to pass the lowest slot of all of the providers, to ensure that we dont miss data.
		lowestSlot := slot
		md.mu.RLock()
		for _, latestSlot := range md.LatestSlots {
			if phase0.Slot(latestSlot) < lowestSlot {
				lowestSlot = phase0.Slot(latestSlot)
			}
		}
		md.mu.RUnlock()

		log.Trace().
			Uint64("lowest_completed_slot", uint64(lowestSlot)).
			Int("handlers", len(s.bidsReceivedHandlers)).
			Msg("Notifying handlers")
		for _, handler := range s.bidsReceivedHandlers {
			go handler.BidsReceived(ctx, lowestSlot)
		}
	}

	return nil
}

func (s *Service) handleSlot(ctx context.Context,
	provider string,
	providerIndex int,
	slot phase0.Slot,
) error {
	ctx, span := otel.Tracer("wealdtech.comptrollerd.services.bids.standard").Start(ctx, "handleSlot")
	defer span.End()

	if err := s.handleReceivedBids(ctx, provider, providerIndex, slot); err != nil {
		return errors.Wrap(err, "failed to handle received bids")
	}
	if err := s.handleDeliveredBids(ctx, provider, providerIndex, slot); err != nil {
		return errors.Wrap(err, "failed to handle delivered bid")
	}

	return nil
}

func (s *Service) handleReceivedBids(ctx context.Context,
	provider string,
	providerIndex int,
	slot phase0.Slot,
) error {
	bids := make([]*comptrollerdb.ReceivedBid, 0)

	bidTraces, err := s.receivedBidTracesProviders[providerIndex].ReceivedBidTraces(ctx, slot)
	if err != nil {
		return errors.Wrap(err, "failed to obtain received bid traces")
	}
	data, err := json.Marshal(bidTraces)
	if err != nil {
		return errors.Wrap(err, "failed to unmarshal received bid traces")
	}
	log.Trace().
		Str("provider", provider).
		Uint64("slot", uint64(slot)).
		RawJSON("bid_traces", data).
		Msg("Obtained received bid traces")

	for _, bidTrace := range bidTraces {
		bids = append(bids, &comptrollerdb.ReceivedBid{
			Slot:                 uint32(bidTrace.Slot),
			Relay:                provider,
			ParentHash:           bidTrace.ParentHash[:],
			BlockHash:            bidTrace.BlockHash[:],
			BuilderPubkey:        bidTrace.BuilderPubkey[:],
			Timestamp:            bidTrace.Timestamp,
			ProposerPubkey:       bidTrace.ProposerPubkey[:],
			ProposerFeeRecipient: bidTrace.ProposerFeeRecipient[:],
			GasLimit:             bidTrace.GasLimit,
			GasUsed:              bidTrace.GasUsed,
			Value:                bidTrace.Value,
		})
	}

	// Some relays return multiple copies of the same bid, so filter them out.
	filteredBidsMap := make(map[string]struct{})
	filteredBids := make([]*comptrollerdb.ReceivedBid, 0, len(bids))
	for _, bid := range bids {
		key := fmt.Sprintf("%d:%s:%#x:%#x:%#x:%d",
			bid.Slot,
			bid.Relay,
			bid.ParentHash,
			bid.BlockHash,
			bid.BuilderPubkey,
			bid.Timestamp.UnixNano(),
		)
		if _, exists := filteredBidsMap[key]; !exists {
			filteredBidsMap[key] = struct{}{}
			filteredBids = append(filteredBids, bid)
		}
	}

	if err := s.receivedBidsSetter.SetReceivedBids(ctx, filteredBids); err != nil {
		return errors.Wrap(err, "failed to set received bids")
	}

	return nil
}

func (s *Service) handleDeliveredBids(ctx context.Context,
	provider string,
	providerIndex int,
	slot phase0.Slot,
) error {
	bids := make([]*comptrollerdb.DeliveredBid, 0)
	bidTrace, err := s.deliveredBidTraceProviders[providerIndex].DeliveredBidTrace(ctx, slot)
	if err != nil {
		return errors.Wrap(err, "failed to obtain delivered bid trace")
	}
	if bidTrace == nil {
		log.Trace().
			Str("provider", provider).
			Uint64("slot", uint64(slot)).
			Msg("No delivered bid trace")

		return nil
	}
	data, err := json.Marshal(bidTrace)
	if err != nil {
		return errors.Wrap(err, "failed to unmarshal delivered bid trace")
	}
	log.Trace().
		Str("provider", provider).
		Uint64("slot", uint64(slot)).
		RawJSON("bid_trace", data).
		Msg("Obtained delivered bid trace")

	bids = append(bids, &comptrollerdb.DeliveredBid{
		Slot:                 uint32(bidTrace.Slot),
		Relay:                provider,
		ParentHash:           bidTrace.ParentHash[:],
		BlockHash:            bidTrace.BlockHash[:],
		BuilderPubkey:        bidTrace.BuilderPubkey[:],
		ProposerPubkey:       bidTrace.ProposerPubkey[:],
		ProposerFeeRecipient: bidTrace.ProposerFeeRecipient[:],
		GasLimit:             bidTrace.GasLimit,
		GasUsed:              bidTrace.GasUsed,
		Value:                bidTrace.Value,
	})

	for _, bid := range bids {
		if err := s.deliveredBidsSetter.SetDeliveredBid(ctx, bid); err != nil {
			log.Error().
				Str("provider", bid.Relay).
				Uint64("slot", uint64(slot)).
				Err(err).
				Msg("Failed to set delivered bid")

			return errors.Wrap(err, "failed to set delivered bid")
		}
	}

	return nil
}
