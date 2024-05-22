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
	"encoding/json"

	"github.com/pkg/errors"
)

type schemaMetadata struct {
	Version uint64 `json:"version"`
}

var currentVersion = uint64(1)

type upgrade struct {
	funcs []func(context.Context, *Service) error
}

var upgrades = map[uint64]*upgrade{}

// Upgrade upgrades the database.
func (s *Service) Upgrade(ctx context.Context) error {
	// See if we have anything at all.
	tableExists, err := s.tableExists(ctx, "t_metadata")
	if err != nil {
		return errors.Wrap(err, "failed to check presence of tables")
	}
	if !tableExists {
		return s.Init(ctx)
	}

	version, err := s.version(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to obtain version")
	}

	log.Trace().
		Uint64("current_version", version).
		Uint64("required_version", currentVersion).
		Msg("Checking if database upgrade is required")
	if version == currentVersion {
		// Nothing to do.
		return nil
	}

	ctx, cancel, err := s.BeginTx(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to begin upgrade transaction")
	}

	for i := version + 1; i <= currentVersion; i++ {
		log.Info().Uint64("target_version", i).Msg("Upgrading database")
		if upgrade, exists := upgrades[i]; exists {
			for i, upgradeFunc := range upgrade.funcs {
				log.Info().Int("current", i+1).Int("total", len(upgrade.funcs)).Msg("Running upgrade function")
				if err := upgradeFunc(ctx, s); err != nil {
					cancel()
					return errors.Wrap(err, "failed to upgrade")
				}
			}
		}
	}

	if err := s.setVersion(ctx, currentVersion); err != nil {
		cancel()
		return errors.Wrap(err, "failed to set latest schema version")
	}

	if err := s.CommitTx(ctx); err != nil {
		cancel()
		return errors.Wrap(err, "failed to commit upgrade transaction")
	}

	log.Info().Msg("Upgrade complete")

	return nil
}

// // columnExists returns true if the given column exists in the given table.
// func (s *Service) columnExists(ctx context.Context, tableName string, columnName string) (bool, error) {
// 	tx := s.tx(ctx)
// 	if tx == nil {
// 		ctx, cancel, err := s.BeginTx(ctx)
// 		if err != nil {
// 			return false, errors.Wrap(err, "failed to begin transaction")
// 		}
// 		tx = s.tx(ctx)
// 		defer cancel()
// 	}
//
// 	query := fmt.Sprintf(`SELECT true
// FROM pg_attribute
// WHERE attrelid = '%s'::regclass
//   AND attname = '%s'
//   AND NOT attisdropped`, tableName, columnName)
//
// 	rows, err := tx.Query(ctx, query)
// 	if err != nil {
// 		return false, err
// 	}
// 	defer rows.Close()
//
// 	found := false
// 	if rows.Next() {
// 		err = rows.Scan(
// 			&found,
// 		)
// 		if err != nil {
// 			return false, errors.Wrap(err, "failed to scan row")
// 		}
// 	}
// 	return found, nil
// }

// tableExists returns true if the given table exists.
func (s *Service) tableExists(ctx context.Context, tableName string) (bool, error) {
	tx := s.tx(ctx)
	if tx == nil {
		ctx, err := s.BeginROTx(ctx)
		if err != nil {
			return false, errors.Wrap(err, "failed to begin transaction")
		}
		tx = s.tx(ctx)
		//nolint:errcheck
		defer s.CommitTx(ctx)
	}

	rows, err := tx.Query(ctx, `SELECT true
FROM information_schema.tables
WHERE table_schema = (SELECT current_schema())
  AND table_name = $1`, tableName)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	found := false
	if rows.Next() {
		err = rows.Scan(
			&found,
		)
		if err != nil {
			return false, errors.Wrap(err, "failed to scan row")
		}
	}

	return found, nil
}

// version obtains the version of the schema.
func (s *Service) version(ctx context.Context) (uint64, error) {
	data, err := s.Metadata(ctx, "schema")
	if err != nil {
		return 0, errors.Wrap(err, "failed to obtain schema metadata")
	}

	// No data means it's version 0 of the schema.
	if len(data) == 0 {
		return 0, nil
	}

	var metadata schemaMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return 0, errors.Wrap(err, "failed to unmarshal metadata JSON")
	}

	return metadata.Version, nil
}

// setVersion sets the version of the schema.
func (s *Service) setVersion(ctx context.Context, version uint64) error {
	if tx := s.tx(ctx); tx == nil {
		return ErrNoTransaction
	}

	metadata := &schemaMetadata{
		Version: version,
	}
	data, err := json.Marshal(metadata)
	if err != nil {
		return errors.Wrap(err, "failed to marshal metadata")
	}

	return s.SetMetadata(ctx, "schema", data)
}

// Init initialises the database.
func (s *Service) Init(ctx context.Context) error {
	ctx, cancel, err := s.BeginTx(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to begin initial tables transaction")
	}
	tx := s.tx(ctx)
	if tx == nil {
		cancel()
		return ErrNoTransaction
	}

	if _, err := tx.Exec(ctx, `
-- t_metadata stores data about processing functions.
CREATE TABLE t_metadata (
  f_key    TEXT NOT NULL PRIMARY KEY
 ,f_value JSONB NOT NULL
);
CREATE UNIQUE INDEX i_metadata_1 ON t_metadata(f_key);
INSERT INTO t_metadata VALUES('schema', '{"version": 1}');

-- t_validator_registrations contains validator registrations to relays.
CREATE TABLE t_validator_registrations (
  f_slot             BIGINT NOT NULL
 ,f_relay            TEXT NOT NULL
 ,f_validator_pubkey BYTEA NOT NULL
 ,f_fee_recipient    BYTEA NOT NULL
 ,f_gas_limit        BIGINT NOT NULL
 ,f_timestamp        TIMESTAMPTZ NOT NULL
 ,f_signature        BYTEA NOT NULL
);
CREATE UNIQUE INDEX i_validator_registrations_1 ON t_validator_registrations(f_slot,f_relay,f_validator_pubkey);

-- t_alternate_bids contains information about alternate bids a proposer could have taken.
CREATE TABLE t_alternate_bids (
  f_slot           BIGINT NOT NULL
 ,f_selected_relay TEXT NOT NULL
 ,f_selected_value NUMERIC NOT NULL
 ,f_best_relay     TEXT NOT NULL
 ,f_best_value     NUMERIC NOT NULL
);
CREATE UNIQUE INDEX i_alternate_bids_1 ON t_alternate_bids(f_slot,f_selected_relay);

-- t_block_payments contains information about the payments made in a block.
CREATE TABLE t_block_payments (
  f_height                    BIGINT NOT NULL
 ,f_hash                      BYTEA NOT NULL
 ,f_slot                      BIGINT NOT NULL
 ,f_proposer_fee_recipient    BYTEA NOT NULL
 ,f_proposer_expected_payment NUMERIC
 ,f_proposer_payment          NUMERIC NOT NULL
 ,f_builder_fee_recipient     BYTEA
 ,f_builder_payment           NUMERIC
);
CREATE UNIQUE INDEX i_block_payments_1 ON t_block_payments(f_height,f_hash);

-- t_received_bids contains information about bids received by relays.
CREATE TABLE t_received_bids (
  f_slot                   BIGINT NOT NULL
 ,f_relay                  TEXT NOT NULL
 ,f_parent_hash            BYTEA NOT NULL
 ,f_block_hash             BYTEA NOT NULL
 ,f_builder_pubkey         BYTEA NOT NULL
 ,f_timestamp              TIMESTAMPTZ NOT NULL
 ,f_proposer_pubkey        BYTEA NOT NULL
 ,f_proposer_fee_recipient BYTEA NOT NULL
 ,f_gas_limit              BIGINT NOT NULL
 ,f_gas_used               BIGINT NOT NULL
 ,f_value                  NUMERIC NOT NULL
);
CREATE UNIQUE INDEX i_received_bids_1 ON t_received_bids(f_slot,f_relay,f_parent_hash,f_block_hash,f_builder_pubkey,f_timestamp);

-- t_delivered_bids contains information about bids delivered by relays.
CREATE TABLE t_delivered_bids (
  f_slot                   BIGINT NOT NULL
 ,f_relay                  TEXT NOT NULL
 ,f_parent_hash            BYTEA NOT NULL
 ,f_block_hash             BYTEA NOT NULL
 ,f_builder_pubkey         BYTEA NOT NULL
 ,f_proposer_pubkey        BYTEA NOT NULL
 ,f_proposer_fee_recipient BYTEA NOT NULL
 ,f_gas_limit              BIGINT NOT NULL
 ,f_gas_used               BIGINT NOT NULL
 ,f_value                  NUMERIC NOT NULL
);
CREATE UNIQUE INDEX i_delivered_bids_1 ON t_delivered_bids(f_slot,f_relay,f_parent_hash,f_block_hash,f_builder_pubkey);
`); err != nil {
		cancel()
		return errors.Wrap(err, "failed to create initial tables")
	}

	if err := s.CommitTx(ctx); err != nil {
		cancel()
		return errors.Wrap(err, "failed to commit initial tables transaction")
	}

	return nil
}
