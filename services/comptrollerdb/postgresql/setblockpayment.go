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
)

// SetBlockPayment sets a block payment.
func (s *Service) SetBlockPayment(ctx context.Context, payment *comptrollerdb.BlockPayment) error {
	if payment == nil {
		return errors.New("payment nil")
	}

	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	proposerPayments := &decimal.NullDecimal{}
	if payment.ProposerPayments != nil {
		proposerPayments.Valid = true
		proposerPayments.Decimal = decimal.NewFromBigInt(payment.ProposerPayments, 0)
	}
	_, err := tx.Exec(ctx, `
INSERT INTO t_block_payments(f_height
                            ,f_hash
                            ,f_slot
                            ,f_fee_recipient
                            ,f_fee_recipient_rewards
                            ,f_proposer_payments
                            )
VALUES($1,$2,$3,$4,$5,$6)
ON CONFLICT (f_height,f_hash) DO
UPDATE
SET f_slot = excluded.f_slot
   ,f_fee_recipient = excluded.f_fee_recipient
   ,f_fee_recipient_rewards = excluded.f_fee_recipient_rewards
   ,f_proposer_payments = excluded.f_proposer_payments
`,
		payment.Height,
		payment.Hash,
		payment.Slot,
		payment.FeeRecipient,
		decimal.NewFromBigInt(payment.FeeRecipientRewards, 0),
		proposerPayments,
	)
	if err != nil {
		return err
	}

	return nil
}
