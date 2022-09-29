// Copyright © 2022 Attestant Limited.
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
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"github.com/wealdtech/comptrollerd/services/comptrollerdb"
	"github.com/wealdtech/comptrollerd/services/comptrollerdb/postgresql"
)

func TestReceivedBids(t *testing.T) {
	ctx := context.Background()
	s, err := postgresql.New(ctx,
		postgresql.WithLogLevel(zerolog.Disabled),
		postgresql.WithServer(os.Getenv("COMPTROLLERDB_SERVER")),
		postgresql.WithPort(atoi(os.Getenv("COMPTROLLERDB_PORT"))),
		postgresql.WithUser(os.Getenv("COMPTROLLERDB_USER")),
		postgresql.WithPassword(os.Getenv("COMPTROLLERDB_PASSWORD")),
	)
	require.NoError(t, err)

	// Fetch all bids (the database may not be clean so find the base number).
	originalBids, err := s.ReceivedBids(ctx, &comptrollerdb.ReceivedBidFilter{})
	require.NoError(t, err)

	// Hold the entire test within a transaction that will be cancelled to clean up the test data.
	ctx, cancel, err := s.BeginTx(ctx)
	require.NoError(t, err)
	defer cancel()

	// Create some test bids.
	bid1 := &comptrollerdb.ReceivedBid{
		Slot:                 12345,
		Relay:                "Relay 1",
		ParentHash:           bytesFromStr("0x0035c9bc07200d4cdc6e8031a2eddf1af78523cefa4e05d1a69d2222294cc044"),
		BlockHash:            bytesFromStr("0x0855abacc7f5ca5c4e2fb0c07e22593b3a5dec56368738df9f0500bf90ce7149"),
		BuilderPubkey:        bytesFromStr("0xa1dead01e65f0a0eee7b5170223f20c8f0cbf122eac3324d61afbdb33a8885ff8cab2ef514ac2c7698ae0d6289ef27fc"),
		Timestamp:            time.Unix(1680000000, 0),
		ProposerPubkey:       bytesFromStr("0x909176c597d4d4ac3eebd017db6e23920d1b34dd7725b80974e15dd97d39b1905ff70ba7f95d5320d624b3ab74408c50"),
		ProposerFeeRecipient: bytesFromStr("0x40638c53d6ef529243a981844b7192744640dd40"),
		GasLimit:             30000000,
		GasUsed:              10000000,
		Value:                big.NewInt(123000000000),
	}
	require.NoError(t, s.SetReceivedBid(ctx, bid1))

	bid2 := &comptrollerdb.ReceivedBid{
		Slot:                 12346,
		Relay:                "Relay 2",
		ParentHash:           bytesFromStr("0x15de7a3b63003104b73db98188f4dc760ed1cf18d531943d48de17b8290b1be8"),
		BlockHash:            bytesFromStr("0xf68a33316e114ef70fc1ba5ad79868ce6c86358cb35c4a9489f9182b2fa18e85"),
		BuilderPubkey:        bytesFromStr("0x8b8edce58fafe098763e4fabdeb318d347f9238845f22c507e813186ea7d44adecd3028f9288048f9ad3bc7c7c735fba"),
		Timestamp:            time.Unix(1680000012, 0),
		ProposerPubkey:       bytesFromStr("0x98b5ce8f75d054d9b1bd4ead8829b1aa9a94bc891c7afc65924917df8549728208d16e1d044cc38f467da5f4db8303fa"),
		ProposerFeeRecipient: bytesFromStr("0x34f4261360d0372176d1d521bf99bf803ced4f6b"),
		GasLimit:             30000000,
		GasUsed:              11810640,
		Value:                big.NewInt(96183027092245231),
	}
	require.NoError(t, s.SetReceivedBid(ctx, bid2))

	bid3 := &comptrollerdb.ReceivedBid{
		Slot:                 12347,
		Relay:                "Relay 1",
		ParentHash:           bytesFromStr("0xdbedd1d915de4a4dab565be7f4bc764e4503b409401900162498c6f898b567a9"),
		BlockHash:            bytesFromStr("0xfe3c824bd622e10cdc991b21f47ac48d3462aaddc90981e6d7d9dd1a63c142a8"),
		BuilderPubkey:        bytesFromStr("0x94aa4ee318f39b56547a253700917982f4b737a49fc3f99ce08fa715e488e673d88a60f7d2cf9145a05127f17dcb7c67"),
		Timestamp:            time.Unix(1680000024, 0),
		ProposerPubkey:       bytesFromStr("0x929fe297734ce1480c373b2e4784307cd232f76506b7052369b80a19b89c34fc43dfa13c90e59239855c5242f7df417f"),
		ProposerFeeRecipient: bytesFromStr("0x000095e79eac4d76aab57cb2c1f091d553b36ca0"),
		GasLimit:             30000000,
		GasUsed:              8664928,
		Value:                big.NewInt(27646761797863433),
	}
	require.NoError(t, s.SetReceivedBid(ctx, bid3))

	// Fetch all bids.
	bids, err := s.ReceivedBids(ctx, &comptrollerdb.ReceivedBidFilter{})
	require.NoError(t, err)
	require.Len(t, bids, len(originalBids)+3)
	require.Contains(t, bids, bid1)
	require.Contains(t, bids, bid2)
	require.Contains(t, bids, bid3)

	// Fetch all bids for a given slot.
	bids, err = s.ReceivedBids(ctx, &comptrollerdb.ReceivedBidFilter{
		FromSlot: &bid1.Slot,
		ToSlot:   &bid1.Slot,
	})
	require.NoError(t, err)
	require.Len(t, bids, 1)
	require.Equal(t, bid1, bids[0])

	// Fetch all bids for a slot range.
	bids, err = s.ReceivedBids(ctx, &comptrollerdb.ReceivedBidFilter{
		FromSlot: &bid1.Slot,
		ToSlot:   &bid2.Slot,
	})
	require.NoError(t, err)
	require.Len(t, bids, 2)
	require.Equal(t, bid1, bids[0])
	require.Equal(t, bid2, bids[1])

	// Fetch all bids for a relay.
	bids, err = s.ReceivedBids(ctx, &comptrollerdb.ReceivedBidFilter{
		Relays: []string{"Relay 1"},
	})
	require.NoError(t, err)
	require.Len(t, bids, 2)
	require.Equal(t, bid1, bids[0])
	require.Equal(t, bid3, bids[1])

	// Fetch all bids for a proposer fee recipient.
	bids, err = s.ReceivedBids(ctx, &comptrollerdb.ReceivedBidFilter{
		ProposerFeeRecipients: [][]byte{bytesFromStr(("0x34f4261360d0372176d1d521bf99bf803ced4f6b"))},
	})
	require.NoError(t, err)
	require.Len(t, bids, 1)
	require.Equal(t, bid2, bids[0])
}
