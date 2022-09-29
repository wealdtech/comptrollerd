// Copyright © 2022 Weald Technology Limited.
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
	"encoding/hex"
	"math/big"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"github.com/wealdtech/comptrollerd/services/comptrollerdb"
	"github.com/wealdtech/comptrollerd/services/comptrollerdb/postgresql"
)

func bytesFromStr(input string) []byte {
	res, err := hex.DecodeString(strings.TrimPrefix(input, "0x"))
	if err != nil {
		panic(err)
	}
	return res
}

func TestSetReceivedBid(t *testing.T) {
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
		name string
		bid  *comptrollerdb.ReceivedBid
		err  string
	}{
		{
			name: "Nil",
			err:  "bid nil",
		},
		{
			name: "Good",
			bid: &comptrollerdb.ReceivedBid{
				Slot:                 12345,
				Relay:                "Test relay",
				ParentHash:           bytesFromStr("0x0035c9bc07200d4cdc6e8031a2eddf1af78523cefa4e05d1a69d2222294cc044"),
				BlockHash:            bytesFromStr("0x0855abacc7f5ca5c4e2fb0c07e22593b3a5dec56368738df9f0500bf90ce7149"),
				BuilderPubkey:        bytesFromStr("0xa1dead01e65f0a0eee7b5170223f20c8f0cbf122eac3324d61afbdb33a8885ff8cab2ef514ac2c7698ae0d6289ef27fc"),
				Timestamp:            time.Unix(1680000000, 0),
				ProposerPubkey:       bytesFromStr("0x909176c597d4d4ac3eebd017db6e23920d1b34dd7725b80974e15dd97d39b1905ff70ba7f95d5320d624b3ab74408c50"),
				ProposerFeeRecipient: bytesFromStr("0x40638c53d6ef529243a981844b7192744640dd40"),
				GasLimit:             30000000,
				GasUsed:              10000000,
				Value:                big.NewInt(123000000000),
			},
		},
	}

	ctx, cancel, err := s.BeginTx(ctx)
	require.NoError(t, err)
	defer cancel()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := s.SetReceivedBid(ctx, test.bid)
			if test.err != "" {
				require.NotNil(t, err)
				require.EqualError(t, err, test.err)
			} else {
				require.Nil(t, err)
			}
		})
	}
}
