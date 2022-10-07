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

func TestAlternateBids(t *testing.T) {
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

	payment1 := &comptrollerdb.AlternateBid{
		Slot:          12345,
		SelectedRelay: "Relay 1",
		SelectedValue: big.NewInt(1000000),
		BestRelay:     "Relay 2",
		BestValue:     big.NewInt(2000000),
	}
	require.NoError(t, s.SetAlternateBid(ctx, payment1))

	payment2 := &comptrollerdb.AlternateBid{
		Slot:          12346,
		SelectedRelay: "Relay 2",
		SelectedValue: big.NewInt(3000000),
		BestRelay:     "Relay 3",
		BestValue:     big.NewInt(4000000),
	}
	require.NoError(t, s.SetAlternateBid(ctx, payment2))

	payment3 := &comptrollerdb.AlternateBid{
		Slot:          12347,
		SelectedRelay: "Relay 1",
		SelectedValue: big.NewInt(5000000),
		BestRelay:     "Relay 3",
		BestValue:     big.NewInt(6000000),
	}
	require.NoError(t, s.SetAlternateBid(ctx, payment3))

	tests := []struct {
		name     string
		filter   *comptrollerdb.AlternateBidFilter
		payments string
		err      string
	}{
		{
			name: "SingleSlot",
			filter: &comptrollerdb.AlternateBidFilter{
				Limit:    1,
				Order:    comptrollerdb.OrderEarliest,
				FromSlot: uint32Ptr(12346),
				ToSlot:   uint32Ptr(12346),
			},
			payments: `[{"Slot":12346,"SelectedRelay":"Relay 2","SelectedValue":3000000,"BestRelay":"Relay 3","BestValue":4000000}]`,
		},
		{
			name: "MultipleSlots",
			filter: &comptrollerdb.AlternateBidFilter{
				Order:    comptrollerdb.OrderEarliest,
				FromSlot: uint32Ptr(12345),
				ToSlot:   uint32Ptr(12347),
			},
			payments: `[{"Slot":12345,"SelectedRelay":"Relay 1","SelectedValue":1000000,"BestRelay":"Relay 2","BestValue":2000000},{"Slot":12346,"SelectedRelay":"Relay 2","SelectedValue":3000000,"BestRelay":"Relay 3","BestValue":4000000},{"Slot":12347,"SelectedRelay":"Relay 1","SelectedValue":5000000,"BestRelay":"Relay 3","BestValue":6000000}]`,
		},
		{
			name: "SelectedRelays",
			filter: &comptrollerdb.AlternateBidFilter{
				Order:          comptrollerdb.OrderEarliest,
				SelectedRelays: []string{"Relay 1"},
			},
			payments: `[{"Slot":12345,"SelectedRelay":"Relay 1","SelectedValue":1000000,"BestRelay":"Relay 2","BestValue":2000000},{"Slot":12347,"SelectedRelay":"Relay 1","SelectedValue":5000000,"BestRelay":"Relay 3","BestValue":6000000}]`,
		},
		{
			name: "BestRelays",
			filter: &comptrollerdb.AlternateBidFilter{
				Order:      comptrollerdb.OrderEarliest,
				BestRelays: []string{"Relay 1", "Relay 2"},
			},
			payments: `[{"Slot":12345,"SelectedRelay":"Relay 1","SelectedValue":1000000,"BestRelay":"Relay 2","BestValue":2000000}]`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := s.AlternateBids(ctx, test.filter)
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
