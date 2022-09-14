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

// Order is the order in which results should be fetched (N.B. fetched, not returned).
type Order uint8

const (
	// OrderEarliest fetches earliest transactions first.
	OrderEarliest Order = iota
	// OrderLatest fetches latest transactions first.
	OrderLatest
)

// BlockPaymentFilter defines a filter for fetching block rewards.
// Filter elements are ANDed together.
// Results are always returned in ascending block height order.
type BlockPaymentFilter struct {
	// Limit is the maximum number of blocks to return.
	// If nil then there is no limit.
	Limit *uint32

	// Order is either OrderEarliest, in which case the earliest results
	// that match the filter are returned, or OrderLatest, in which case the
	// latest results that match the filter are returned.
	// The default is OrderEarliest.
	Order Order

	// FromHeight is the earliest height from which to fetch rewards.
	// If nil then there is no earliest height.
	FromHeight *uint32

	// ToHeight is the latest height to which to fetch rewards.
	// If nil then there is no latest height.
	ToHeight *uint32

	// FeeRecipients are the fee recipients of the rewards.
	// If nil then there is no fee recipient filter.
	FeeRecipients [][]byte
}

// ValidatorRegistrationFilter defines a filter for fetching validator registrations.
// Filter elements are ANDed together.
// Results are always returned in ascending block height order.
type ValidatorRegistrationFilter struct {
	// Limit is the maximum number of blocks to return.
	// If nil then there is no limit.
	Limit *uint32

	// Order is either OrderEarliest, in which case the earliest results
	// that match the filter are returned, or OrderLatest, in which case the
	// latest results that match the filter are returned.
	// The default is OrderEarliest.
	Order Order

	// Relay is the address of the relays to fetch.
	// If nil then there is no relay filter.
	Relays []string

	// FromSlot is the earliest slot from which to fetch registrations.
	// If nil then there is no earliest slot.
	FromSlot *uint32

	// ToSlot is the latest slot to which to fetch registrations.
	// If nil then there is no latest slot.
	ToSlot *uint32

	// ValidatorPubkeys are the validator public keys of the registrations.
	// If nil then there is no validator public key filter.
	ValidatorPubkeys [][]byte

	// FeeRecipients are the fee recipients of the registrations.
	// If nil then there is no fee recipient filter.
	FeeRecipients [][]byte
}
