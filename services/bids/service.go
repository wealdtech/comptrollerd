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

package bids

import (
	"context"

	"github.com/attestantio/go-eth2-client/spec/phase0"
)

// Service defines a service that obtains bid information from relays.
type Service any

// ReceivedHandler defines the interface for handlers triggered by received bids.
type ReceivedHandler interface {
	// BidsReceived is called whenever the bids for a given slot have been obtained.
	BidsReceived(ctx context.Context, slot phase0.Slot)
}
