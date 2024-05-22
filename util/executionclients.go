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

package util

import (
	"context"
	"sync"

	execclient "github.com/attestantio/go-execution-client"
	"github.com/attestantio/go-execution-client/jsonrpc"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

var (
	executionClients   map[string]execclient.Service
	executionClientsMu sync.Mutex
)

// FetchExecutionClient fetches an execution client service, instantiating it if required.
func FetchExecutionClient(ctx context.Context, address string) (execclient.Service, error) {
	executionClientsMu.Lock()
	defer executionClientsMu.Unlock()
	if executionClients == nil {
		executionClients = make(map[string]execclient.Service)
	}

	var client execclient.Service
	var exists bool
	if client, exists = executionClients[address]; !exists {
		var err error
		client, err = jsonrpc.New(ctx,
			jsonrpc.WithLogLevel(LogLevel("execclient")),
			jsonrpc.WithTimeout(viper.GetDuration("execclient.timeout")),
			jsonrpc.WithAddress(address))
		if err != nil {
			return nil, errors.Wrap(err, "failed to initiate client")
		}
		// Confirm that the client provides the required interfaces.
		if err := confirmExecutionClientInterfaces(ctx, client); err != nil {
			return nil, errors.Wrap(err, "missing required interface")
		}
		executionClients[address] = client
	}

	return client, nil
}

func confirmExecutionClientInterfaces(_ context.Context, client execclient.Service) error {
	if _, isProvider := client.(execclient.BlocksProvider); !isProvider {
		return errors.New("client is not a BlocksProvider")
	}

	return nil
}
