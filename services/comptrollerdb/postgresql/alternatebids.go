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
	"fmt"
	"sort"
	"strings"

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
	"github.com/wealdtech/comptrollerd/services/comptrollerdb"
	"go.opentelemetry.io/otel"
)

// LatestAlternateBidSlot provides the slot of the latest alternate bid in the database.
func (s *Service) LatestAlternateBidSlot(ctx context.Context) (phase0.Slot, error) {
	ctx, span := otel.Tracer("wealdtech.comptrollerd.services.comptrollerdb.postgresql").Start(ctx, "LatestAlternateBidSlot")
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
FROM t_alternate_bids`).Scan(&latest)
	if err != nil {
		return 0, err
	}

	return phase0.Slot(latest), nil
}

// AlternateBids returns alternate bids matching the supplied filter.
func (s *Service) AlternateBids(ctx context.Context,
	filter *comptrollerdb.AlternateBidFilter,
) (
	[]*comptrollerdb.AlternateBid,
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
      ,f_selected_relay
      ,f_selected_value
      ,f_best_relay
      ,f_best_value
FROM t_alternate_bids`)

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

	if len(filter.SelectedRelays) != 0 {
		queryVals = append(queryVals, filter.SelectedRelays)
		_, _ = queryBuilder.WriteString(fmt.Sprintf(`
%s f_selected_relay = ANY($%d)`, wherestr, len(queryVals)))
		wherestr = "  AND"
	}

	if len(filter.BestRelays) != 0 {
		queryVals = append(queryVals, filter.BestRelays)
		_, _ = queryBuilder.WriteString(fmt.Sprintf(`
%s f_best_relay = ANY($%d)`, wherestr, len(queryVals)))
	}

	switch filter.Order {
	case comptrollerdb.OrderEarliest:
		_, _ = queryBuilder.WriteString(`
ORDER BY f_slot`)
	case comptrollerdb.OrderLatest:
		_, _ = queryBuilder.WriteString(`
ORDER BY f_slot DESC`)
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

	alternateBids := make([]*comptrollerdb.AlternateBid, 0)
	for rows.Next() {
		var selectedValue decimal.Decimal
		var bestValue decimal.Decimal
		alternateBid := &comptrollerdb.AlternateBid{}
		err := rows.Scan(
			&alternateBid.Slot,
			&alternateBid.SelectedRelay,
			&selectedValue,
			&alternateBid.BestRelay,
			&bestValue,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		alternateBid.SelectedValue = selectedValue.BigInt()
		alternateBid.BestValue = bestValue.BigInt()
		alternateBids = append(alternateBids, alternateBid)
	}

	// Always return order of slot.
	sort.Slice(alternateBids, func(i int, j int) bool {
		return alternateBids[i].Slot < alternateBids[j].Slot
	})

	return alternateBids, nil
}
