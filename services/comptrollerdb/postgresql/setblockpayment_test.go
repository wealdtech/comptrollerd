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
	"math/big"
	"os"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"github.com/wealdtech/comptrollerd/services/comptrollerdb"
	"github.com/wealdtech/comptrollerd/services/comptrollerdb/postgresql"
)

func TestSetBlockPayment(t *testing.T) {
	ctx := context.Background()
	s, err := postgresql.New(ctx,
		postgresql.WithLogLevel(zerolog.Disabled),
		postgresql.WithServer(os.Getenv("COMPTROLLERDB_SERVER")),
		postgresql.WithPort(atoi(os.Getenv("COMPTROLLERDB_PORT"))),
		postgresql.WithUser(os.Getenv("COMPTROLLERDB_USER")),
		postgresql.WithPassword(os.Getenv("COMPTROLLERDB_PASSWORD")),
	)
	require.NoError(t, err)

	tests := []struct {
		name    string
		payment *comptrollerdb.BlockPayment
		err     string
	}{
		{
			name: "Nil",
			err:  "payment nil",
		},
		{
			name: "Good",
			payment: &comptrollerdb.BlockPayment{
				Height:              12345,
				Hash:                byteArray("0x000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"),
				Slot:                1234,
				FeeRecipient:        byteArray("0x000102030405060708090a0b0c0d0e0f10111213"),
				FeeRecipientRewards: big.NewInt(22222222),
				ProposerPayments:    big.NewInt(11111111),
			},
		},
		{
			name: "NilProposerPayment",
			payment: &comptrollerdb.BlockPayment{
				Height:              12345,
				Hash:                byteArray("0x000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"),
				Slot:                1234,
				FeeRecipient:        byteArray("0x000102030405060708090a0b0c0d0e0f10111213"),
				FeeRecipientRewards: big.NewInt(22222222),
			},
		},
	}

	ctx, cancel, err := s.BeginTx(ctx)
	require.NoError(t, err)
	defer cancel()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := s.SetBlockPayment(ctx, test.payment)
			if test.err != "" {
				require.NotNil(t, err)
				require.EqualError(t, err, test.err)
			} else {
				require.Nil(t, err)
			}
		})
	}
}
