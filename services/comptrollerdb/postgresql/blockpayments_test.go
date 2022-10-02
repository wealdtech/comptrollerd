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
	"math/big"
	"os"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"github.com/wealdtech/comptrollerd/services/comptrollerdb"
	"github.com/wealdtech/comptrollerd/services/comptrollerdb/postgresql"
)

func TestBlockPayments(t *testing.T) {
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

	payment1 := &comptrollerdb.BlockPayment{
		Height:              12345,
		Hash:                byteArray("0x000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"),
		Slot:                1234,
		FeeRecipient:        byteArray("0x000102030405060708090a0b0c0d0e0f10111213"),
		FeeRecipientRewards: big.NewInt(22222222),
		ProposerPayments:    big.NewInt(11111111),
	}
	require.NoError(t, s.SetBlockPayment(ctx, payment1))

	payment2 := &comptrollerdb.BlockPayment{
		Height:              12346,
		Hash:                byteArray("0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"),
		Slot:                1236,
		FeeRecipient:        byteArray("0x000102030405060708090a0b0c0d0e0f10111213"),
		FeeRecipientRewards: big.NewInt(22222222),
	}
	require.NoError(t, s.SetBlockPayment(ctx, payment2))

	payment3 := &comptrollerdb.BlockPayment{
		Height:              12347,
		Hash:                byteArray("0x02030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f2021"),
		Slot:                1240,
		FeeRecipient:        byteArray("0x0102030405060708090a0b0c0d0e0f1011121314"),
		FeeRecipientRewards: big.NewInt(22222222),
		ProposerPayments:    big.NewInt(22222222),
	}
	require.NoError(t, s.SetBlockPayment(ctx, payment3))

	tests := []struct {
		name     string
		filter   *comptrollerdb.BlockPaymentFilter
		payments string
		err      string
	}{
		{
			name: "SinglePayment",
			filter: &comptrollerdb.BlockPaymentFilter{
				Limit:      1,
				Order:      comptrollerdb.OrderEarliest,
				FromHeight: uint32Ptr(12346),
				ToHeight:   uint32Ptr(12346),
			},
			payments: `[{"Height":12346,"Hash":"AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA=","Slot":1236,"FeeRecipient":"AAECAwQFBgcICQoLDA0ODxAREhM=","FeeRecipientRewards":22222222,"ProposerPayments":null}]`,
		},
		{
			name: "RangePayments",
			filter: &comptrollerdb.BlockPaymentFilter{
				Order:      comptrollerdb.OrderEarliest,
				FromHeight: uint32Ptr(12345),
				ToHeight:   uint32Ptr(12347),
			},
			payments: `[{"Height":12345,"Hash":"AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=","Slot":1234,"FeeRecipient":"AAECAwQFBgcICQoLDA0ODxAREhM=","FeeRecipientRewards":22222222,"ProposerPayments":11111111},{"Height":12346,"Hash":"AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA=","Slot":1236,"FeeRecipient":"AAECAwQFBgcICQoLDA0ODxAREhM=","FeeRecipientRewards":22222222,"ProposerPayments":null},{"Height":12347,"Hash":"AgMEBQYHCAkKCwwNDg8QERITFBUWFxgZGhscHR4fICE=","Slot":1240,"FeeRecipient":"AQIDBAUGBwgJCgsMDQ4PEBESExQ=","FeeRecipientRewards":22222222,"ProposerPayments":22222222}]`,
		},
		{
			name: "FeeRecipient",
			filter: &comptrollerdb.BlockPaymentFilter{
				Order: comptrollerdb.OrderEarliest,
				FeeRecipients: [][]byte{
					byteArray("0x0102030405060708090a0b0c0d0e0f1011121314"),
				},
			},
			payments: `[{"Height":12347,"Hash":"AgMEBQYHCAkKCwwNDg8QERITFBUWFxgZGhscHR4fICE=","Slot":1240,"FeeRecipient":"AQIDBAUGBwgJCgsMDQ4PEBESExQ=","FeeRecipientRewards":22222222,"ProposerPayments":22222222}]`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := s.BlockPayments(ctx, test.filter)
			if test.err != "" {
				require.NotNil(t, err)
				require.EqualError(t, err, test.err)
			} else {
				require.NoError(t, err)
				registrations, err := json.Marshal(res)
				require.NoError(t, err)
				require.Equal(t, test.payments, string(registrations))
			}
		})
	}
}
