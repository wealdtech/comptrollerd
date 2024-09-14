// Copyright © 2022, 2024 Weald Technology Trading.
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

package standard

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/pkg/errors"
)

// metadata stored about this service.
type metadata struct {
	// mu controls access to the metadata.
	mu          sync.RWMutex
	LatestSlots map[string]int64 `json:"latest_slots"`
	// LatestSlot is deprecated.
	LatestSlot int64 `json:"latest_slot,omitempty"`
}

// metadataKey is the key for the metadata.
var metadataKey = "bids.standard"

// getMetadata gets metadata for this service.
func (s *Service) getMetadata(ctx context.Context) (*metadata, error) {
	md := &metadata{
		LatestSlots: make(map[string]int64),
	}
	mdJSON, err := s.receivedBidsSetter.Metadata(ctx, metadataKey)
	if err != nil {
		return nil, errors.Wrap(err, "failed to fetch metadata")
	}
	if mdJSON == nil {
		return md, nil
	}
	if err := json.Unmarshal(mdJSON, md); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal metadata")
	}

	if len(md.LatestSlots) == 0 {
		// Upgrade.
		for _, provider := range s.receivedBidTracesProviders {
			md.LatestSlots[provider.Name()] = md.LatestSlot
		}
		md.LatestSlot = 0
	}

	return md, nil
}

// setMetadata sets metadata for this service.
func (s *Service) setMetadata(ctx context.Context, md *metadata) error {
	mdJSON, err := json.Marshal(md)
	if err != nil {
		return errors.Wrap(err, "failed to marshal metadata")
	}
	if err := s.receivedBidsSetter.SetMetadata(ctx, metadataKey, mdJSON); err != nil {
		return errors.Wrap(err, "failed to update metadata")
	}

	return nil
}
