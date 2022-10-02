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
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/pkg/errors"
	"github.com/wealdtech/comptrollerd/services/comptrollerdb"
)

// ValidatorRegistrations returns validator registrations matching the supplied filter.
func (s *Service) ValidatorRegistrations(ctx context.Context,
	filter *comptrollerdb.ValidatorRegistrationFilter,
) (
	[]*comptrollerdb.ValidatorRegistration,
	error,
) {
	tx := s.tx(ctx)
	if tx == nil {
		ctx, cancel, err := s.BeginTx(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "failed to begin transaction")
		}
		tx = s.tx(ctx)
		defer cancel()
	}

	// Build the query.
	queryBuilder := strings.Builder{}
	queryVals := make([]interface{}, 0)

	queryBuilder.WriteString(`
SELECT f_slot
      ,f_relay
      ,f_validator_pubkey
      ,f_fee_recipient
      ,f_gas_limit
      ,f_timestamp
      ,f_signature
FROM t_validator_registrations`)

	wherestr := "WHERE"

	if len(filter.Relays) > 0 {
		queryVals = append(queryVals, filter.Relays)
		queryBuilder.WriteString(fmt.Sprintf(`
%s f_relay = ANY($%d)`, wherestr, len(queryVals)))
		wherestr = "  AND"
	}

	if filter.FromSlot != nil {
		queryVals = append(queryVals, *filter.FromSlot)
		queryBuilder.WriteString(fmt.Sprintf(`
%s f_slot >= $%d`, wherestr, len(queryVals)))
		wherestr = "  AND"
	}

	if filter.ToSlot != nil {
		queryVals = append(queryVals, *filter.ToSlot)
		queryBuilder.WriteString(fmt.Sprintf(`
%s f_slot <= $%d`, wherestr, len(queryVals)))
	}

	if len(filter.FeeRecipients) > 0 {
		queryVals = append(queryVals, filter.FeeRecipients)
		queryBuilder.WriteString(fmt.Sprintf(`
%s f_fee_recipient = ANY($%d)`, wherestr, len(queryVals)))
	}

	if len(filter.ValidatorPubkeys) > 0 {
		queryVals = append(queryVals, filter.ValidatorPubkeys)
		queryBuilder.WriteString(fmt.Sprintf(`
%s f_validator_pubkey = ANY($%d)`, wherestr, len(queryVals)))
	}

	switch filter.Order {
	case comptrollerdb.OrderEarliest:
		queryBuilder.WriteString(`
ORDER BY f_slot,f_relay,f_validator_pubkey`)
	case comptrollerdb.OrderLatest:
		queryBuilder.WriteString(`
ORDER BY f_slot DESC,f_relay DESC,f_validator_pubkey DESC`)
	default:
		return nil, errors.New("no order specified")
	}

	if filter.Limit != 0 {
		queryVals = append(queryVals, filter.Limit)
		queryBuilder.WriteString(fmt.Sprintf(`
LIMIT $%d`, len(queryVals)))
	}

	if e := log.Trace(); e.Enabled() {
		params := make([]string, len(queryVals))
		for i := range queryVals {
			params[i] = fmt.Sprintf("%v", queryVals[i])
		}
		log.Trace().Str("query", strings.ReplaceAll(queryBuilder.String(), "\n", " ")).Strs("params", params).Msg("SQL query")
	}

	rows, err := tx.Query(ctx,
		queryBuilder.String(),
		queryVals...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	registrations := make([]*comptrollerdb.ValidatorRegistration, 0)
	for rows.Next() {
		registration := &comptrollerdb.ValidatorRegistration{}
		err := rows.Scan(
			&registration.Slot,
			&registration.Relay,
			&registration.ValidatorPubkey,
			&registration.FeeRecipient,
			&registration.GasLimit,
			&registration.Timestamp,
			&registration.Signature,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		registrations = append(registrations, registration)
	}

	// Always return order of slot then relay then validator public key.
	sort.Slice(registrations, func(i int, j int) bool {
		if registrations[i].Slot != registrations[j].Slot {
			return registrations[i].Slot < registrations[j].Slot
		}
		if x := strings.Compare(registrations[i].Relay, registrations[j].Relay); x != 0 {
			return x < 0
		}
		return bytes.Compare(registrations[i].ValidatorPubkey, registrations[j].ValidatorPubkey) < 0
	})
	return registrations, nil
}
