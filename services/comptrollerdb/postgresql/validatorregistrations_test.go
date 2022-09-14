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

package postgresql_test

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"github.com/wealdtech/comptrollerd/services/comptrollerdb"
	"github.com/wealdtech/comptrollerd/services/comptrollerdb/postgresql"
)

func TestValidatorRegistrations(t *testing.T) {
	ctx := context.Background()
	s, err := postgresql.New(ctx,
		postgresql.WithLogLevel(zerolog.Disabled),
		postgresql.WithServer(os.Getenv("COMPTROLLERDB_SERVER")),
		postgresql.WithPort(atoi(os.Getenv("COMPTROLLERDB_PORT"))),
		postgresql.WithUser(os.Getenv("COMPTROLLERDB_USER")),
		postgresql.WithPassword(os.Getenv("COMPTROLLERDB_PASSWORD")),
	)
	require.NoError(t, err)

	ctx, cancel, err := s.BeginTx(ctx)
	require.NoError(t, err)
	defer cancel()

	registration1 := &comptrollerdb.ValidatorRegistration{
		Slot:            111,
		Relay:           "relay1",
		ValidatorPubkey: byteArray("0x000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f"),
		FeeRecipient:    byteArray("0x000102030405060708090a0b0c0d0e0f10111213"),
		GasLimit:        30000000,
		Timestamp:       time.Unix(1680000000, 0),
		Signature:       byteArray("0x000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f"),
	}
	require.NoError(t, s.SetValidatorRegistration(ctx, registration1))

	registration2 := &comptrollerdb.ValidatorRegistration{
		Slot:            111,
		Relay:           "relay2",
		ValidatorPubkey: byteArray("0x000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f"),
		FeeRecipient:    byteArray("0x000102030405060708090a0b0c0d0e0f10111213"),
		GasLimit:        30000000,
		Timestamp:       time.Unix(1680000000, 0),
		Signature:       byteArray("0x000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f"),
	}
	require.NoError(t, s.SetValidatorRegistration(ctx, registration2))

	registration3 := &comptrollerdb.ValidatorRegistration{
		Slot:            112,
		Relay:           "relay1",
		ValidatorPubkey: byteArray("0x000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f"),
		FeeRecipient:    byteArray("0x000102030405060708090a0b0c0d0e0f10111213"),
		GasLimit:        30000000,
		Timestamp:       time.Unix(1680000000, 0),
		Signature:       byteArray("0x000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f"),
	}
	require.NoError(t, s.SetValidatorRegistration(ctx, registration3))

	tests := []struct {
		name          string
		filter        *comptrollerdb.ValidatorRegistrationFilter
		registrations string
		err           string
	}{
		{
			name: "SingleRegistration",
			filter: &comptrollerdb.ValidatorRegistrationFilter{
				Limit:    uint32Ptr(1),
				Order:    comptrollerdb.OrderEarliest,
				FromSlot: uint32Ptr(111),
				ToSlot:   uint32Ptr(111),
			},
			registrations: `[{"Slot":111,"Relay":"relay1","ValidatorPubkey":"AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4v","FeeRecipient":"AAECAwQFBgcICQoLDA0ODxAREhM=","GasLimit":30000000,"Timestamp":"2023-03-28T11:40:00+01:00","Signature":"AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4vMDEyMzQ1Njc4OTo7PD0+P0BBQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWltcXV5f"}]`,
		},
		{
			name: "SingleRelay",
			filter: &comptrollerdb.ValidatorRegistrationFilter{
				Order:  comptrollerdb.OrderEarliest,
				Relays: []string{"relay1"},
			},
			registrations: `[{"Slot":111,"Relay":"relay1","ValidatorPubkey":"AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4v","FeeRecipient":"AAECAwQFBgcICQoLDA0ODxAREhM=","GasLimit":30000000,"Timestamp":"2023-03-28T11:40:00+01:00","Signature":"AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4vMDEyMzQ1Njc4OTo7PD0+P0BBQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWltcXV5f"},{"Slot":112,"Relay":"relay1","ValidatorPubkey":"AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4v","FeeRecipient":"AAECAwQFBgcICQoLDA0ODxAREhM=","GasLimit":30000000,"Timestamp":"2023-03-28T11:40:00+01:00","Signature":"AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4vMDEyMzQ1Njc4OTo7PD0+P0BBQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWltcXV5f"}]`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := s.ValidatorRegistrations(ctx, test.filter)
			if test.err != "" {
				require.NotNil(t, err)
				require.EqualError(t, err, test.err)
			} else {
				require.NoError(t, err)
				registrations, err := json.Marshal(res)
				require.NoError(t, err)
				require.Equal(t, test.registrations, string(registrations))
			}
		})
	}
}
