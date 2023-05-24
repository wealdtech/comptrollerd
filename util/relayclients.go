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

package util

import (
	"context"
	"sync"

	relayclient "github.com/attestantio/go-relay-client"
	httprelayclient "github.com/attestantio/go-relay-client/http"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

var (
	relayClients   map[string]relayclient.Service
	relayClientsMu sync.Mutex
)

// FetchRelayClient fetches a relay client service, instantiating it if required.
func FetchRelayClient(ctx context.Context, address string) (relayclient.Service, error) {
	relayClientsMu.Lock()
	defer relayClientsMu.Unlock()
	if relayClients == nil {
		relayClients = make(map[string]relayclient.Service)
	}

	var client relayclient.Service
	var exists bool
	if client, exists = relayClients[address]; !exists {
		var err error
		client, err = httprelayclient.New(ctx,
			httprelayclient.WithLogLevel(LogLevel("relayclient")),
			httprelayclient.WithTimeout(viper.GetDuration("relayclient.timeout")),
			httprelayclient.WithAddress(address))
		if err != nil {
			return nil, errors.Wrap(err, "failed to initiate relay client")
		}
		// Confirm that the client provides the required interfaces.
		if err := confirmRelayClientInterfaces(ctx, client); err != nil {
			return nil, errors.Wrap(err, "missing required interface")
		}
		relayClients[address] = client
	}

	return client, nil
}

func confirmRelayClientInterfaces(_ context.Context, client relayclient.Service) error {
	if _, isProvider := client.(relayclient.QueuedProposersProvider); !isProvider {
		return errors.New("client is not a QueuedProposersProvider")
	}

	return nil
}
