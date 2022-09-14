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
	Height              uint32
	Hash                []byte
	Slot                uint32
	FeeRecipient        []byte
	FeeRecipientRewards *big.Int
	ProposerPayments    *big.Int
}
