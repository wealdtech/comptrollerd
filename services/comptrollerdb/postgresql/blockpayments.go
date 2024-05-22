// Copyright © 2022, 2023 Weald Technology Trading.
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
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
	"github.com/wealdtech/comptrollerd/services/comptrollerdb"
	"go.opentelemetry.io/otel"
)

// LatestBlockPaymentSlot provides the slot of the latest block payment in the database.
func (s *Service) LatestBlockPaymentSlot(ctx context.Context) (phase0.Slot, error) {
	ctx, span := otel.Tracer("wealdtech.comptrollerd.services.comptrollerdb.postgresql").Start(ctx, "LatestBlockPaymentSlot")
	defer span.End()

	var err error

	tx := s.tx(ctx)
	if tx == nil {
		ctx, err = s.BeginROTx(ctx)
		if err != nil {
			return 0, errors.Wrap(err, "failed to begin transaction")
		}
		tx = s.tx(ctx)
		defer s.CommitROTx(ctx)
	}

	var latest uint64
	err = tx.QueryRow(ctx, `
SELECT MAX(f_slot)
FROM t_block_payments`).Scan(&latest)
	if err != nil {
		return 0, err
	}

	return phase0.Slot(latest), nil
}

// BlockPayments returns block payments matching the supplied filter.
func (s *Service) BlockPayments(ctx context.Context,
	filter *comptrollerdb.BlockPaymentFilter,
) (
	[]*comptrollerdb.BlockPayment,
	error,
) {
	tx := s.tx(ctx)
	if tx == nil {
		ctx, err := s.BeginROTx(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "failed to begin transaction")
		}
		tx = s.tx(ctx)
		defer s.CommitROTx(ctx)
	}

	// Build the query.
	queryBuilder := strings.Builder{}
	queryVals := make([]any, 0)

	_, _ = queryBuilder.WriteString(`
SELECT f_height
      ,f_hash
      ,f_slot
	  ,f_proposer_fee_recipient
      ,f_proposer_expected_payment
      ,f_proposer_payment
	  ,f_builder_fee_recipient
      ,f_builder_payment
FROM t_block_payments`)

	wherestr := "WHERE"

	if filter.FromHeight != nil {
		queryVals = append(queryVals, *filter.FromHeight)
		_, _ = queryBuilder.WriteString(fmt.Sprintf(`
%s f_height >= $%d`, wherestr, len(queryVals)))
		wherestr = "  AND"
	}

	if filter.ToHeight != nil {
		queryVals = append(queryVals, *filter.ToHeight)
		_, _ = queryBuilder.WriteString(fmt.Sprintf(`
%s f_height <= $%d`, wherestr, len(queryVals)))
	}

	if filter.FromSlot != nil {
		queryVals = append(queryVals, *filter.FromSlot)
		_, _ = queryBuilder.WriteString(fmt.Sprintf(`
%s f_slot >= $%d`, wherestr, len(queryVals)))
		wherestr = "  AND"
	}

	if filter.ToSlot != nil {
		queryVals = append(queryVals, *filter.ToSlot)
		_, _ = queryBuilder.WriteString(fmt.Sprintf(`
%s f_slot <= $%d`, wherestr, len(queryVals)))
	}

	// 	if len(filter.FeeRecipients) != 0 {
	// 		queryVals = append(queryVals, filter.FeeRecipients)
	// 		_, _ = queryBuilder.WriteString(fmt.Sprintf(`
	// %s f_fee_recipient = ANY($%d)`, wherestr, len(queryVals)))
	// 	}

	switch filter.Order {
	case comptrollerdb.OrderEarliest:
		_, _ = queryBuilder.WriteString(`
ORDER BY f_height,f_hash`)
	case comptrollerdb.OrderLatest:
		_, _ = queryBuilder.WriteString(`
ORDER BY f_height DESC,f_hash DESC`)
	default:
		return nil, errors.New("no order specified")
	}

	if filter.Limit != 0 {
		queryVals = append(queryVals, filter.Limit)
		_, _ = queryBuilder.WriteString(fmt.Sprintf(`
LIMIT $%d`, len(queryVals)))
	}

	if e := log.Trace(); e.Enabled() {
		params := make([]string, len(queryVals))
		for i := range queryVals {
			params[i] = fmt.Sprintf("%v", queryVals[i])
		}
		e.Str("query", strings.ReplaceAll(queryBuilder.String(), "\n", " ")).Strs("params", params).Msg("SQL query")
	}

	rows, err := tx.Query(ctx,
		queryBuilder.String(),
		queryVals...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	payments := make([]*comptrollerdb.BlockPayment, 0)
	for rows.Next() {
		var proposerPayment decimal.Decimal
		var proposerExpectedPayment decimal.NullDecimal
		var builderPayment decimal.NullDecimal
		payment := &comptrollerdb.BlockPayment{}
		err := rows.Scan(
			&payment.Height,
			&payment.Hash,
			&payment.Slot,
			&payment.ProposerFeeRecipient,
			&proposerExpectedPayment,
			&proposerPayment,
			&payment.BuilderFeeRecipient,
			&builderPayment,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		if proposerExpectedPayment.Valid {
			payment.ProposerExpectedPayment = proposerExpectedPayment.Decimal.BigInt()
		}
		payment.ProposerPayment = proposerPayment.BigInt()
		if builderPayment.Valid {
			payment.BuilderPayment = builderPayment.Decimal.BigInt()
		}
		payments = append(payments, payment)
	}

	// Always return order of height then hash.
	sort.Slice(payments, func(i int, j int) bool {
		if payments[i].Height != payments[j].Height {
			return payments[i].Height < payments[j].Height
		}

		return bytes.Compare(payments[i].Hash, payments[j].Hash) < 0
	})

	return payments, nil
}
