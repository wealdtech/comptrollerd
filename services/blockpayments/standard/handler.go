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
	"math/big"

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/pkg/errors"
	"github.com/wealdtech/comptrollerd/services/comptrollerdb"
	"github.com/wealdtech/execd/services/execdb"
)

func (s *Service) catchup(ctx context.Context, md *metadata) {
	// We find the latest slot for which we have a delivered bid.
	bids, err := s.deliveredBidsProvider.DeliveredBids(ctx, &comptrollerdb.DeliveredBidFilter{
		Order: comptrollerdb.OrderLatest,
		Limit: 1,
	})
	if err != nil {
		log.Error().Err(err).Msg("Failed to obtain latest delivered bid")
	}
	if len(bids) == 0 {
		log.Debug().Msg("No delivered bids")
	}
	latestSlot := phase0.Slot(bids[0].Slot) - phase0.Slot(s.trackDistance)

	if md.LatestSlot >= int64(latestSlot) {
		log.Trace().Msg("Up-to-date")
		return
	}

	if md.LatestSlot == -1 {
		// We find the earliest slot for which we have a delivered bid to avoid unnecessary work.
		bids, err := s.deliveredBidsProvider.DeliveredBids(ctx, &comptrollerdb.DeliveredBidFilter{
			Order: comptrollerdb.OrderEarliest,
			Limit: 1,
		})
		if err != nil {
			log.Error().Err(err).Msg("Failed to obtain earliest delivered bid")
		}
		md.LatestSlot = int64(bids[0].Slot) - 1
	}

	log.Trace().Int64("start_slot", md.LatestSlot+1).Uint64("end_slot", uint64(latestSlot)).Msg("Working through slot range")
	for slot := phase0.Slot(md.LatestSlot + 1); slot <= latestSlot; slot++ {
		log := log.With().Uint64("slot", uint64(slot)).Logger()
		log.Trace().Msg("Handling slot")

		ctx, cancel, err := s.blockPaymentsSetter.BeginTx(ctx)
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

		if err := s.blockPaymentsSetter.CommitTx(ctx); err != nil {
			cancel()
			log.Error().Err(err).Msg("Failed to commit transaction")
			return
		}

		monitorPaymentsUpdated(slot)
	}
}

func (s *Service) handleSlot(ctx context.Context, slot phase0.Slot) error {
	if err := s.checkInaccurateBid(ctx, slot); err != nil {
		return err
	}
	log.Trace().Msg("Checked inaccurate bid")

	if err := s.checkPoorBidSelection(ctx, slot); err != nil {
		return err
	}
	log.Trace().Msg("Checked poor bid selection")

	return nil
}

func (s *Service) checkInaccurateBid(ctx context.Context, slot phase0.Slot) error {
	dbSlot := uint32(slot)
	deliveredBids, err := s.deliveredBidsProvider.DeliveredBids(ctx, &comptrollerdb.DeliveredBidFilter{
		FromSlot: &dbSlot,
		ToSlot:   &dbSlot,
	})
	if err != nil {
		return errors.Wrap(err, "failed to obtain delivered bids")
	}
	if len(deliveredBids) == 0 {
		// No delivered bids means this wasn't an MEV block (or one that we don't know about).
		return nil
	}
	deliveredBid := deliveredBids[0]

	// Obtain the execution block.
	slotStart := s.chainTime.StartOfSlot(slot)
	blocks, err := s.blocksProvider.Blocks(ctx, &execdb.BlockFilter{
		TimestampFrom: &slotStart,
		TimestampTo:   &slotStart,
	})
	if err != nil {
		return errors.Wrap(err, "failed to obtain execution block")
	}
	if len(blocks) == 0 {
		// We do not know about this block.  It is possible that it was not incorporated in to the chain,
		// or that there was a relay failure.  Either way, there isn't anything more we can do here.
		log.Debug().Uint64("slot", uint64(slot)).Msg("No block obtained for slot")
		return nil
	}
	block := blocks[0]

	// Fetch the transactions for the block.
	txs, err := s.transactionsProvider.Transactions(ctx, &execdb.TransactionFilter{
		From: &block.Height,
		To:   &block.Height,
	})
	if err != nil {
		return errors.Wrap(err, "failed to obtain execution block transactions")
	}

	// We always calculate the fee recipient rewards.
	costs, gasFees, feeRecipientPayments, err := s.calcFeeRecipientRewards(ctx, block, txs)
	if err != nil {
		return errors.Wrap(err, "failed to obtain calculate fee recipient rewards")
	}
	log.Trace().Uint32("block", block.Height).Str("fee_recipient", fmt.Sprintf("%#x", block.FeeRecipient)).Stringer("costs", costs).Stringer("gas_fees", gasFees).Stringer("payments", feeRecipientPayments).Msg("Calculated fee recipient rewards")

	blockPayment := &comptrollerdb.BlockPayment{
		Height:                  block.Height,
		Hash:                    block.Hash,
		Slot:                    uint32(slot),
		ProposerExpectedPayment: deliveredBid.Value,
	}

	if bytes.Equal(block.FeeRecipient, deliveredBid.ProposerFeeRecipient) {
		// The block fee recipient is that provided by the proposer.
		blockPayment.ProposerFeeRecipient = block.FeeRecipient
		blockPayment.ProposerExpectedPayment = deliveredBid.Value
		blockPayment.ProposerPayment = new(big.Int).Add(gasFees, feeRecipientPayments)
	} else {
		// The block fee recipient is not that provided by the proposer.
		blockPayment.ProposerFeeRecipient = deliveredBid.ProposerFeeRecipient
		blockPayment.ProposerExpectedPayment = deliveredBid.Value
		blockPayment.BuilderFeeRecipient = block.FeeRecipient

		// We also want to calculate the payment to the proposer.
		blockPayment.ProposerPayment, err = s.calcProposerPayments(ctx, deliveredBid.ProposerFeeRecipient, block, txs)
		if err != nil {
			return errors.Wrap(err, "failed to obtain calculate proposer payments")
		}

		// The builder payment is net of the proposer payment.
		blockPayment.BuilderPayment = new(big.Int).Sub(new(big.Int).Sub(new(big.Int).Add(gasFees, feeRecipientPayments), blockPayment.ProposerPayment), costs)

		log.Trace().Uint32("block", block.Height).
			Str("proposer_fee_recipient", fmt.Sprintf("%#x", deliveredBid.ProposerFeeRecipient)).
			Stringer("proposer_expected_payment", blockPayment.ProposerExpectedPayment).
			Stringer("proposer_payment", blockPayment.ProposerPayment).
			Str("builder_fee_recipient", fmt.Sprintf("%#x", block.FeeRecipient)).
			Stringer("builder_payment", blockPayment.BuilderPayment).
			Msg("End results")
	}
	if err := s.blockPaymentsSetter.SetBlockPayment(ctx, blockPayment); err != nil {
		return errors.Wrap(err, "failed to set block payment")
	}

	return nil
}

func (s *Service) calcFeeRecipientRewards(ctx context.Context, block *execdb.Block, txs []*execdb.Transaction) (*big.Int, *big.Int, *big.Int, error) {
	costs := big.NewInt(0)
	tips := big.NewInt(0)
	payments := big.NewInt(0)

	// Fetch all balance changes to the proposer for the block.
	feeRecipientBalanceChanges, err := s.transactionBalanceChangesProvider.TransactionBalanceChanges(ctx, &execdb.TransactionBalanceChangeFilter{
		From:      &block.Height,
		To:        &block.Height,
		Addresses: [][]byte{block.FeeRecipient},
	})
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "failed to obtain transaction balance changes")
	}
	// Turn the balance changes in to a map for easy lookup.
	balanceChanges := make(map[string]*execdb.TransactionBalanceChange)
	for _, proposerBalanceChange := range feeRecipientBalanceChanges {
		balanceChanges[fmt.Sprintf("%x", proposerBalanceChange.TransactionHash)] = proposerBalanceChange
	}

	for _, tx := range txs {
		// Start off by calculating the gas fee.
		gasFee := new(big.Int).Mul(big.NewInt(int64(tx.GasPrice)), big.NewInt(int64(tx.GasUsed)))
		baseFee := new(big.Int).Mul(big.NewInt(int64(block.BaseFee)), big.NewInt(int64(tx.GasUsed)))
		tip := new(big.Int).Sub(gasFee, baseFee)

		if bytes.Equal(tx.From, block.FeeRecipient) {
			// This transaction is from the fee recipient, its base fee is a cost.
			costs = costs.Add(costs, baseFee)
			// Tips are net zero so ignore.
		} else {
			// This transaction is not from the fee recipient, the tip comes to us.
			tips = tips.Add(tips, tip)

			balanceChange, exists := balanceChanges[fmt.Sprintf("%x", tx.Hash)]
			if exists {
				transfer := new(big.Int).Sub(balanceChange.New, balanceChange.Old)
				payment := new(big.Int).Sub(transfer, tip)
				if payment.Cmp(big.NewInt(0)) > 0 {
					log.Trace().Uint32("block", block.Height).Uint32("tx_index", tx.Index).Stringer("tip", tip).Stringer("transfer", transfer).Stringer("payment", payment).Msg("Found payment to fee recipient")
					payments = payments.Add(payments, payment)
				}
			}
			//			// Check the actual payment made to see if it differs, and if so record it.
			//			balanceChanges, err := s.transactionBalanceChangesProvider.TransactionBalanceChanges(ctx, &execdb.TransactionBalanceChangeFilter{
			//				TxHashes:  [][]byte{tx.Hash},
			//				Addresses: [][]byte{block.FeeRecipient},
			//			})
			//			if err != nil {
			//				return nil, nil, nil, errors.Wrap(err, "failed to obtain transaction balance changes")
			//			}
			//			for _, balanceChange := range balanceChanges {
			//				transfer := new(big.Int).Sub(balanceChange.New, balanceChange.Old)
			//				payment := new(big.Int).Sub(transfer, tip)
			//				if payment.Cmp(big.NewInt(0)) > 0 {
			//					log.Trace().Uint32("block", block.Height).Uint32("tx_index", tx.Index).Stringer("tip", tip).Stringer("transfer", transfer).Stringer("payment", payment).Msg("Found payment to fee recipient")
			//					payments = payments.Add(payments, payment)
			//				}
			//			}
		}
	}

	return costs, tips, payments, nil
}

func (s *Service) calcProposerPayments(ctx context.Context, proposer []byte, block *execdb.Block, txs []*execdb.Transaction) (*big.Int, error) {
	payments := big.NewInt(0)

	// Fetch all balance changes to the proposer for the block.
	proposerBalanceChanges, err := s.transactionBalanceChangesProvider.TransactionBalanceChanges(ctx, &execdb.TransactionBalanceChangeFilter{
		From:      &block.Height,
		To:        &block.Height,
		Addresses: [][]byte{proposer},
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to obtain transaction balance changes")
	}
	// Turn the balance changes in to a map for easy lookup.
	balanceChanges := make(map[string]*execdb.TransactionBalanceChange)
	for _, proposerBalanceChange := range proposerBalanceChanges {
		balanceChanges[fmt.Sprintf("%x", proposerBalanceChange.TransactionHash)] = proposerBalanceChange
	}

	for _, tx := range txs {
		// If this transaction is not from the proposer we have to look for value transfers within it.
		if !bytes.Equal(tx.From, proposer) {
			// Check the actual payment made and record it.
			balanceChange, exists := balanceChanges[fmt.Sprintf("%x", tx.Hash)]
			if exists {
				log.Trace().Str("tx_hash", fmt.Sprintf("%#x", tx.Hash)).Stringer("payment", new(big.Int).Sub(balanceChange.New, balanceChange.Old)).Msg("TX payment to proposer")
				payments = payments.Add(payments, new(big.Int).Sub(balanceChange.New, balanceChange.Old))
			}
			//			// Check the actual payment made and record it.
			//			balanceChanges, err := s.transactionBalanceChangesProvider.TransactionBalanceChanges(ctx, &execdb.TransactionBalanceChangeFilter{
			//				TxHashes:  [][]byte{tx.Hash},
			//				Addresses: [][]byte{proposer},
			//			})
			//			if err != nil {
			//				return nil, errors.Wrap(err, "failed to obtain transaction balance changes")
			//			}
			//			for _, balanceChange := range balanceChanges {
			//				log.Trace().Str("tx_hash", fmt.Sprintf("%#x", tx.Hash)).Stringer("payment", new(big.Int).Sub(balanceChange.New, balanceChange.Old)).Msg("TX payment to proposer")
			//				payments = payments.Add(payments, new(big.Int).Sub(balanceChange.New, balanceChange.Old))
			//			}
		}
	}

	return payments, nil
}

func (s *Service) checkPoorBidSelection(ctx context.Context, slot phase0.Slot) error {
	dbSlot := uint32(slot)
	deliveredBids, err := s.deliveredBidsProvider.DeliveredBids(ctx, &comptrollerdb.DeliveredBidFilter{
		FromSlot: &dbSlot,
		ToSlot:   &dbSlot,
	})
	if err != nil {
		return errors.Wrap(err, "failed to obtain delivered bids")
	}
	if len(deliveredBids) == 0 {
		// We can still work here in future.
		return nil
	}
	deliveredBid := deliveredBids[0]

	receivedBids, err := s.receivedBidsProvider.ReceivedBids(ctx, &comptrollerdb.ReceivedBidFilter{
		FromSlot: &dbSlot,
		ToSlot:   &dbSlot,
	})
	if err != nil {
		return errors.Wrap(err, "failed to obtain received bids")
	}

	var bestBid *comptrollerdb.ReceivedBid
	for _, bid := range receivedBids {
		if bid.Value.Cmp(deliveredBid.Value) > 0 {
			// This is higher value than the delivered bid.
			if bestBid == nil || bid.Value.Cmp(bestBid.Value) > 0 {
				// This is the best bid so far.
				bestBid = bid
			}
		}
	}

	if bestBid != nil {
		slotStartDelta := bestBid.Timestamp.Unix() - s.chainTime.StartOfSlot(slot).Unix()
		log.Trace().
			Uint64("slot", uint64(slot)).
			Stringer("value", deliveredBid.Value).
			Str("relay", deliveredBid.Relay).
			Int64("slot_start_delta", slotStartDelta).
			Stringer("bid_value", bestBid.Value).
			Str("bid_relay", bestBid.Relay).
			Stringer("diff", new(big.Int).Sub(bestBid.Value, deliveredBid.Value)).
			Msg("Found a better bid")
		if err := s.alternateBidsSetter.SetAlternateBid(ctx, &comptrollerdb.AlternateBid{
			Slot:          uint32(slot),
			SelectedRelay: deliveredBid.Relay,
			SelectedValue: deliveredBid.Value,
			BestRelay:     bestBid.Relay,
			BestValue:     bestBid.Value,
		}); err != nil {
			return errors.Wrap(err, "failed to set alternate bid")
		}
	}
	return nil
}
