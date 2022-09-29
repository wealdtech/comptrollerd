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

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/pkg/errors"
	"github.com/wealdtech/comptrollerd/services/comptrollerdb"
)

func (s *Service) catchup(ctx context.Context,
	md *metadata) {

	// We fetch up to, but not including, the current slot.
	currentSlot := s.chainTime.CurrentSlot()
	for slot := phase0.Slot(md.LatestSlot + 1); slot < currentSlot; slot++ {
		log := log.With().Uint64("slot", uint64(slot)).Logger()
		log.Trace().Msg("Handling slot")

		ctx, cancel, err := s.receivedBidsSetter.BeginTx(ctx)
		if err != nil {
			log.Error().Err(err).Msg("Failed to begin transaction")
			return
		}

		if err := s.handleSlot(ctx, slot); err != nil {
			cancel()
			log.Error().Err(err).Msg("Failed to handle slot")
			return
		}

		md.LatestSlot = int64(slot)
		if err := s.setMetadata(ctx, md); err != nil {
			cancel()
			log.Error().Err(err).Msg("Failed to set metadata")
			return
		}

		if err := s.receivedBidsSetter.CommitTx(ctx); err != nil {
			cancel()
			log.Error().Err(err).Msg("Failed to commit transaction")
			return
		}

		for _, relay := range s.receivedBidTracesProviders {
			monitorRelayUpdated(relay.Address())
		}
		for _, relay := range s.deliveredBidTraceProviders {
			monitorRelayUpdated(relay.Address())
		}

		log.Trace().Msg("Handled slot")
	}
}

func (s *Service) handleSlot(ctx context.Context, slot phase0.Slot) error {
	if err := s.handleReceivedBids(ctx, slot); err != nil {
		return errors.Wrap(err, "failed to handle received bids")
	}
	if err := s.handleDeliveredBids(ctx, slot); err != nil {
		return errors.Wrap(err, "failed to handle delivered bids")
	}

	return nil
}

func (s *Service) handleReceivedBids(ctx context.Context, slot phase0.Slot) error {
	for _, provider := range s.receivedBidTracesProviders {
		traces, err := provider.ReceivedBidTraces(ctx, slot)
		if err != nil {
			log.Debug().Str("provider", provider.Address()).Uint64("slot", uint64(slot)).Err(err).Msg("Failed to obtain received bid traces")
			return errors.Wrap(err, "failed to obtain received bid traces")
		}
		data, err := json.Marshal(traces)
		if err != nil {
			return errors.Wrap(err, "failed to marshal received bid traces")
		}
		log.Trace().Str("provider", provider.Address()).Uint64("slot", uint64(slot)).RawJSON("bid_traces", data).Msg("Obtained received bid traces")

		for _, trace := range traces {
			if err := s.receivedBidsSetter.SetReceivedBid(ctx, &comptrollerdb.ReceivedBid{
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
			}); err != nil {
				return errors.Wrap(err, "failed to set received bid")
			}
		}
	}
	return nil
}

func (s *Service) handleDeliveredBids(ctx context.Context, slot phase0.Slot) error {
	for _, provider := range s.deliveredBidTraceProviders {
		trace, err := provider.DeliveredBidTrace(ctx, slot)
		if err != nil {
			log.Debug().Str("provider", provider.Address()).Uint64("slot", uint64(slot)).Err(err).Msg("Failed to obtain delivered bid trace")
			return errors.Wrap(err, "failed to obtain delivered bid trace")
		}
		if trace == nil {
			log.Trace().Str("provider", provider.Address()).Uint64("slot", uint64(slot)).Msg("No delivered bid trace")
			continue
		}
		log.Trace().Str("provider", provider.Address()).Uint64("slot", uint64(slot)).Stringer("bid_trace", trace).Msg("Obtained delivered bid trace")

		if err := s.deliveredBidsSetter.SetDeliveredBid(ctx, &comptrollerdb.DeliveredBid{
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
		}); err != nil {
			return errors.Wrap(err, "failed to set delivered bid")
		}

		// For now just log these.
		slot := uint32(trace.Slot)
		bids, err := s.deliveredBidsSetter.(comptrollerdb.ReceivedBidsProvider).ReceivedBids(ctx, &comptrollerdb.ReceivedBidFilter{
			FromSlot: &slot,
			ToSlot:   &slot,
		})
		if err != nil {
			return errors.Wrap(err, "failed to obtain received bids")
		}
		if len(bids) == 0 {
			log.Info().Uint64("slot", uint64(slot)).Msg("No received bids!")
		}
		slotStart := s.chainTime.StartOfSlot(trace.Slot)
		for _, bid := range bids {
			if bid.Value.Cmp(trace.Value) > 0 {
				slotStartDelta := bid.Timestamp.Unix() - slotStart.Unix()
				// This is a higher bid.
				log.Info().
					Uint64("slot", uint64(slot)).
					Stringer("value", trace.Value).
					Str("relay", provider.Address()).
					Int64("slot_start_delta", slotStartDelta).
					Stringer("bid_value", bid.Value).
					Str("bid_relay", bid.Relay).
					Msg("Found a better bid")
			}
		}
	}
	return nil
}
