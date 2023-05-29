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

	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
	"github.com/wealdtech/comptrollerd/services/comptrollerdb"
	"go.opentelemetry.io/otel"
)

// SetDeliveredBid sets a bid delivered by a relay.
func (s *Service) SetDeliveredBid(ctx context.Context, bid *comptrollerdb.DeliveredBid) error {
	ctx, span := otel.Tracer("wealdtech.comptrollerd.services.comptrollerdb.postgresql").Start(ctx, "SetDeliveredBid")
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
INSERT INTO t_delivered_bids(f_slot
                            ,f_relay
                            ,f_parent_hash
                            ,f_block_hash
                            ,f_builder_pubkey
                            ,f_proposer_pubkey
                            ,f_proposer_fee_recipient
                            ,f_gas_limit
                            ,f_gas_used
                            ,f_value
                           )
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
ON CONFLICT (f_slot,f_relay,f_parent_hash,f_block_hash,f_builder_pubkey) DO
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
