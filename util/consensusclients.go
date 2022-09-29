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

	consensusclient "github.com/attestantio/go-eth2-client"
	httpconsensusclient "github.com/attestantio/go-eth2-client/http"
	"github.com/pkg/errors"
)

var consensusClients map[string]consensusclient.Service
var consensusClientsMu sync.Mutex

// FetchConsensusClient fetches a consensus client, instantiating it if required.
func FetchConsensusClient(ctx context.Context, address string) (consensusclient.Service, error) {
	if address == "" {
		return nil, errors.New("no address supplied")
	}

	consensusClientsMu.Lock()
	defer consensusClientsMu.Unlock()
	if consensusClients == nil {
		consensusClients = make(map[string]consensusclient.Service)
	}

	var client consensusclient.Service
	var exists bool
	if client, exists = consensusClients[address]; !exists {
		var err error
		client, err = httpconsensusclient.New(ctx,
			httpconsensusclient.WithLogLevel(LogLevel("consensusclient")),
			httpconsensusclient.WithTimeout(Timeout("consensusclient")),
			httpconsensusclient.WithAddress(address))
		if err != nil {
			return nil, errors.Wrap(err, "failed to initiate consensus client")
		}
		consensusClients[address] = client
	}
	return client, nil
}
