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

package comptrollerdb

import (
	"math/big"
	"time"
)

// ValidatorRegistration holds information about a validator registration with a relay.
type ValidatorRegistration struct {
	Slot            uint32
	Relay           string
	ValidatorPubkey []byte
	FeeRecipient    []byte
	GasLimit        uint64
	Timestamp       time.Time
	Signature       []byte
}

// BlockPayment holds information about the payments for a block.
type BlockPayment struct {
	Height uint32
	Hash   []byte
	Slot   uint32
	// ProposerFeeRecipient is the fee recipient address supplied by the proposer.
	ProposerFeeRecipient []byte
	// ProposerExpectedPayment is the payment expected by the proposer.
	ProposerExpectedPayment *big.Int
	// ProposerdPayment is the payment made to the proposer.
	ProposerPayment *big.Int
	// BuilderFeeRecipient is the fee recipient address supplied by the builder.
	BuilderFeeRecipient []byte
	// BuilderPayment is the payment made to the builder.
	BuilderPayment *big.Int
}

// AlternateBid holds information on alternate bids that could have been better.
type AlternateBid struct {
	Slot          uint32
	SelectedRelay string
	SelectedValue *big.Int
	BestRelay     string
	BestValue     *big.Int
}

// ReceivedBid holds information on a bid received by a relay from a builder.
type ReceivedBid struct {
	Slot                 uint32
	Relay                string
	ParentHash           []byte
	BlockHash            []byte
	BuilderPubkey        []byte
	Timestamp            time.Time
	ProposerPubkey       []byte
	ProposerFeeRecipient []byte
	GasLimit             uint64
	GasUsed              uint64
	Value                *big.Int
}

// DeliveredBid holds information on a bid delivered by a relay to a proposer.
type DeliveredBid struct {
	Slot                 uint32
	Relay                string
	ParentHash           []byte
	BlockHash            []byte
	BuilderPubkey        []byte
	ProposerPubkey       []byte
	ProposerFeeRecipient []byte
	GasLimit             uint64
	GasUsed              uint64
	Value                *big.Int
}
