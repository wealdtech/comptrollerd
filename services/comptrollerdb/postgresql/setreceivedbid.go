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

package postgresql

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
	"github.com/wealdtech/comptrollerd/services/comptrollerdb"
	"go.opentelemetry.io/otel"
)

// SetReceivedBids sets multiple bids received by a relay.
func (s *Service) SetReceivedBids(ctx context.Context, bids []*comptrollerdb.ReceivedBid) error {
	ctx, span := otel.Tracer("wealdtech.comptrollerd.services.comptrollerdb.postgresql").Start(ctx, "SetReceivedBids")
	defer span.End()

	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	// Create a savepoint in case the copy fails.
	nestedTx, err := tx.Begin(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to create nested transaction")
	}

	_, err = nestedTx.CopyFrom(ctx,
		pgx.Identifier{"t_received_bids"},
		[]string{
			"f_slot",
			"f_relay",
			"f_parent_hash",
			"f_block_hash",
			"f_builder_pubkey",
			"f_timestamp",
			"f_proposer_pubkey",
			"f_proposer_fee_recipient",
			"f_gas_limit",
			"f_gas_used",
			"f_value",
		},
		pgx.CopyFromSlice(len(bids), func(i int) ([]any, error) {
			return []any{
				bids[i].Slot,
				bids[i].Relay,
				bids[i].ParentHash,
				bids[i].BlockHash,
				bids[i].BuilderPubkey,
				bids[i].Timestamp,
				bids[i].ProposerPubkey,
				bids[i].ProposerFeeRecipient,
				bids[i].GasLimit,
				bids[i].GasUsed,
				decimal.NewFromBigInt(bids[i].Value, 0),
			}, nil
		}))

	if err == nil {
		if err := nestedTx.Commit(ctx); err != nil {
			return errors.Wrap(err, "failed to commit nested transaction")
		}
	} else {
		if err := nestedTx.Rollback(ctx); err != nil {
			return errors.Wrap(err, "failed to roll back nested transaction")
		}

		log.Debug().Err(err).Msg("Failed to copy insert received bids; applying one at a time")
		for _, bid := range bids {
			if err := s.SetReceivedBid(ctx, bid); err != nil {
				log.Error().Err(err).Msg("Failure to insert individual received bid")
				return err
			}
		}

		// Succeeded so clear the error.
		err = nil
	}

	return err
}

// SetReceivedBid sets a bid received by a relay.
func (s *Service) SetReceivedBid(ctx context.Context, bid *comptrollerdb.ReceivedBid) error {
	ctx, span := otel.Tracer("wealdtech.comptrollerd.services.comptrollerdb.postgresql").Start(ctx, "SetReceivedBid")
	defer span.End()

	if bid == nil {
		return errors.New("bid nil")
	}

	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	value := decimal.NewFromBigInt(bid.Value, 0)
	_, err := tx.Exec(ctx, `
INSERT INTO t_received_bids(f_slot
                           ,f_relay
                           ,f_parent_hash
                           ,f_block_hash
                           ,f_builder_pubkey
                           ,f_timestamp
                           ,f_proposer_pubkey
                           ,f_proposer_fee_recipient
                           ,f_gas_limit
                           ,f_gas_used
                           ,f_value
                          )
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
ON CONFLICT (f_slot,f_relay,f_parent_hash,f_block_hash,f_builder_pubkey,f_timestamp) DO
UPDATE
SET f_proposer_pubkey = excluded.f_proposer_pubkey
   ,f_proposer_fee_recipient = excluded.f_proposer_fee_recipient
   ,f_gas_limit = excluded.f_gas_limit
   ,f_gas_used = excluded.f_gas_used
   ,f_value = excluded.f_value
`,
		bid.Slot,
		bid.Relay,
		bid.ParentHash,
		bid.BlockHash,
		bid.BuilderPubkey,
		bid.Timestamp,
		bid.ProposerPubkey,
		bid.ProposerFeeRecipient,
		bid.GasLimit,
		bid.GasUsed,
		value,
	)
	if err != nil {
		return err
	}

	return nil
}
