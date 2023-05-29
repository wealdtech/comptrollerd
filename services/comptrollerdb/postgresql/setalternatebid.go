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

// SetAlternateBid sets an alternate bid.
func (s *Service) SetAlternateBid(ctx context.Context, alternateBid *comptrollerdb.AlternateBid) error {
	ctx, span := otel.Tracer("wealdtech.comptrollerd.services.comptrollerdb.postgresql").Start(ctx, "SetAlternateBid")
	defer span.End()

	if alternateBid == nil {
		return errors.New("alternate bid nil")
	}

	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	_, err := tx.Exec(ctx, `
INSERT INTO t_alternate_bids(f_slot
                            ,f_selected_relay
                            ,f_selected_value
	                        ,f_best_relay
                            ,f_best_value
                            )
VALUES($1,$2,$3,$4,$5)
ON CONFLICT (f_slot,f_selected_relay) DO
UPDATE
SET f_selected_value = excluded.f_selected_value
   ,f_best_relay = excluded.f_best_relay
   ,f_best_value = excluded.f_best_value
`,
		alternateBid.Slot,
		alternateBid.SelectedRelay,
		decimal.NewFromBigInt(alternateBid.SelectedValue, 0),
		alternateBid.BestRelay,
		decimal.NewFromBigInt(alternateBid.BestValue, 0),
	)
	if err != nil {
		return err
	}

	return nil
}
