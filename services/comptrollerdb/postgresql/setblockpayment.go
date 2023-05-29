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

// SetBlockPayment sets a block payment.
func (s *Service) SetBlockPayment(ctx context.Context, payment *comptrollerdb.BlockPayment) error {
	ctx, span := otel.Tracer("wealdtech.comptrollerd.services.comptrollerdb.postgresql").Start(ctx, "SetBlockPayment")
	defer span.End()

	if payment == nil {
		return errors.New("payment nil")
	}

	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	proposerExpectedPayment := &decimal.NullDecimal{}
	if payment.ProposerExpectedPayment != nil {
		proposerExpectedPayment.Valid = true
		proposerExpectedPayment.Decimal = decimal.NewFromBigInt(payment.ProposerExpectedPayment, 0)
	}
	proposerPayment := decimal.NewFromBigInt(payment.ProposerPayment, 0)
	builderPayment := &decimal.NullDecimal{}
	if payment.BuilderPayment != nil {
		builderPayment.Valid = true
		builderPayment.Decimal = decimal.NewFromBigInt(payment.BuilderPayment, 0)
	}
	_, err := tx.Exec(ctx, `
INSERT INTO t_block_payments(f_height
                            ,f_hash
                            ,f_slot
	                        ,f_proposer_fee_recipient
                            ,f_proposer_expected_payment
                            ,f_proposer_payment
	                        ,f_builder_fee_recipient
                            ,f_builder_payment
                            )
VALUES($1,$2,$3,$4,$5,$6,$7,$8)
ON CONFLICT (f_height,f_hash) DO
UPDATE
SET f_slot = excluded.f_slot
   ,f_proposer_fee_recipient = excluded.f_proposer_fee_recipient
   ,f_proposer_expected_payment = excluded.f_proposer_expected_payment
   ,f_proposer_payment = excluded.f_proposer_payment
   ,f_builder_fee_recipient = excluded.f_builder_fee_recipient
   ,f_builder_payment = excluded.f_builder_payment
`,
		payment.Height,
		payment.Hash,
		payment.Slot,
		payment.ProposerFeeRecipient,
		proposerExpectedPayment,
		proposerPayment,
		payment.BuilderFeeRecipient,
		builderPayment,
	)
	if err != nil {
		return err
	}

	return nil
}
