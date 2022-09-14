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
	"github.com/wealdtech/comptrollerd/services/comptrollerdb"
)

// SetValidatorRegistration sets a validator registration.
func (s *Service) SetValidatorRegistration(ctx context.Context, registration *comptrollerdb.ValidatorRegistration) error {
	if registration == nil {
		return errors.New("registration nil")
	}

	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	_, err := tx.Exec(ctx, `
INSERT INTO t_validator_registrations(f_slot
                                     ,f_relay
                                     ,f_validator_pubkey
                                     ,f_fee_recipient
                                     ,f_gas_limit
                                     ,f_timestamp
                                     ,f_signature
                          )
VALUES($1,$2,$3,$4,$5,$6,$7)
ON CONFLICT (f_slot,f_relay,f_validator_pubkey) DO
UPDATE
SET f_fee_recipient = excluded.f_fee_recipient
   ,f_gas_limit = excluded.f_gas_limit
   ,f_timestamp = excluded.f_timestamp
   ,f_signature = excluded.f_signature
`,
		registration.Slot,
		registration.Relay,
		registration.ValidatorPubkey,
		registration.FeeRecipient,
		registration.GasLimit,
		registration.Timestamp,
		registration.Signature,
	)
	if err != nil {
		return err
	}

	return nil
}
