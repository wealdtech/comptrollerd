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

// LatestDeliveredBidSlot provides the slot of the latest delivered bid in the database.
func (s *Service) LatestDeliveredBidSlot(ctx context.Context) (phase0.Slot, error) {
	ctx, span := otel.Tracer("wealdtech.comptrollerd.services.comptrollerdb.postgresql").Start(ctx, "LatestDeliveredBidSlot")
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
FROM t_delivered_bids`).Scan(&latest)
	if err != nil {
		return 0, err
	}

	return phase0.Slot(latest), nil
}

// DeliveredBids returns delivered bids matching the supplied filter.
func (s *Service) DeliveredBids(ctx context.Context,
	filter *comptrollerdb.DeliveredBidFilter,
) (
	[]*comptrollerdb.DeliveredBid,
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
SELECT f_slot
      ,f_relay
      ,f_parent_hash
      ,f_block_hash
      ,f_builder_pubkey
      ,f_proposer_pubkey
      ,f_proposer_fee_recipient
      ,f_gas_limit
      ,f_gas_used
      ,f_value
FROM t_delivered_bids`)

	wherestr := "WHERE"

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
		wherestr = "  AND"
	}

	if len(filter.Relays) > 0 {
		queryVals = append(queryVals, filter.Relays)
		_, _ = queryBuilder.WriteString(fmt.Sprintf(`
%s f_relay = ANY($%d)`, wherestr, len(queryVals)))
		wherestr = "  AND"
	}

	if len(filter.ProposerFeeRecipients) > 0 {
		queryVals = append(queryVals, filter.ProposerFeeRecipients)
		_, _ = queryBuilder.WriteString(fmt.Sprintf(`
%s f_proposer_fee_recipient = ANY($%d)`, wherestr, len(queryVals)))
		wherestr = "  AND"
	}

	if len(filter.BuilderPubkeys) > 0 {
		queryVals = append(queryVals, filter.BuilderPubkeys)
		_, _ = queryBuilder.WriteString(fmt.Sprintf(`
%s f_builder_pubkey = ANY($%d)`, wherestr, len(queryVals)))
		wherestr = "  AND"
	}

	if len(filter.ProposerPubkeys) > 0 {
		queryVals = append(queryVals, filter.ProposerPubkeys)
		_, _ = queryBuilder.WriteString(fmt.Sprintf(`
%s f_proposer_pubkey = ANY($%d)`, wherestr, len(queryVals)))
		wherestr = "  AND"
	}

	if len(filter.BlockHashes) > 0 {
		queryVals = append(queryVals, filter.BlockHashes)
		_, _ = queryBuilder.WriteString(fmt.Sprintf(`
%s f_block_hash = ANY($%d)`, wherestr, len(queryVals)))
		// wherestr = "  AND"
	}

	switch filter.Order {
	case comptrollerdb.OrderEarliest:
		_, _ = queryBuilder.WriteString(`
ORDER BY f_slot,f_relay,f_parent_hash,f_block_hash,f_builder_pubkey`)
	case comptrollerdb.OrderLatest:
		_, _ = queryBuilder.WriteString(`
ORDER BY f_slot DESC,f_relay DESC,f_parent_hash DESC,f_block_hash DESC,f_builder_pubkey DESC`)
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

	bids := make([]*comptrollerdb.DeliveredBid, 0)
	for rows.Next() {
		bid := &comptrollerdb.DeliveredBid{}
		value := decimal.Decimal{}
		err := rows.Scan(
			&bid.Slot,
			&bid.Relay,
			&bid.ParentHash,
			&bid.BlockHash,
			&bid.BuilderPubkey,
			&bid.ProposerPubkey,
			&bid.ProposerFeeRecipient,
			&bid.GasLimit,
			&bid.GasUsed,
			&value,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		bid.Value = value.BigInt()
		bids = append(bids, bid)
	}

	// Always return order of slot then value then relay then proposer public key.
	sort.Slice(bids, func(i int, j int) bool {
		if bids[i].Slot != bids[j].Slot {
			return bids[i].Slot < bids[j].Slot
		}
		if cmp := bids[i].Value.Cmp(bids[j].Value); cmp != 0 {
			return cmp < 0
		}
		if cmp := strings.Compare(bids[i].Relay, bids[j].Relay); cmp != 0 {
			return cmp < 0
		}

		return bytes.Compare(bids[i].ProposerPubkey, bids[j].ProposerPubkey) < 0
	})

	return bids, nil
}
