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

	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/wealdtech/chaind/services/chaindb"
	postgresqlchaindb "github.com/wealdtech/chaind/services/chaindb/postgresql"
	postgresqlcomptrollerdb "github.com/wealdtech/comptrollerd/services/comptrollerdb/postgresql"
	"github.com/wealdtech/execd/services/execdb"
	postgresqlexecdb "github.com/wealdtech/execd/services/execdb/postgresql"
	majordomo "github.com/wealdtech/go-majordomo"
)

// InitChainDB initialises the chain database.
func InitChainDB(ctx context.Context, majordomo majordomo.Service) (chaindb.Service, error) {
	opts := []postgresqlchaindb.Parameter{
		postgresqlchaindb.WithLogLevel(LogLevel("chaindb")),
		postgresqlchaindb.WithServer(viper.GetString("chaindb.server")),
		postgresqlchaindb.WithUser(viper.GetString("chaindb.user")),
		postgresqlchaindb.WithPassword(viper.GetString("chaindb.password")),
		postgresqlchaindb.WithPort(viper.GetInt32("chaindb.port")),
	}

	if viper.GetString("chaindb.client-cert") != "" {
		clientCert, err := majordomo.Fetch(ctx, viper.GetString("chaindb.client-cert"))
		if err != nil {
			return nil, errors.Wrap(err, "failed to read client certificate")
		}
		opts = append(opts, postgresqlchaindb.WithClientCert(clientCert))
	}

	if viper.GetString("chaindb.client-key") != "" {
		clientKey, err := majordomo.Fetch(ctx, viper.GetString("chaindb.client-key"))
		if err != nil {
			return nil, errors.Wrap(err, "failed to read client key")
		}
		opts = append(opts, postgresqlchaindb.WithClientKey(clientKey))
	}

	if viper.GetString("chaindb.ca-cert") != "" {
		caCert, err := majordomo.Fetch(ctx, viper.GetString("chaindb.ca-cert"))
		if err != nil {
			return nil, errors.Wrap(err, "failed to read certificate authority certificate")
		}
		opts = append(opts, postgresqlchaindb.WithCACert(caCert))
	}

	return postgresqlchaindb.New(ctx, opts...)
}

// InitExecDB initialises the exec database.
func InitExecDB(ctx context.Context, majordomo majordomo.Service) (execdb.Service, error) {
	opts := []postgresqlexecdb.Parameter{
		postgresqlexecdb.WithLogLevel(LogLevel("execdb")),
		postgresqlexecdb.WithServer(viper.GetString("execdb.server")),
		postgresqlexecdb.WithUser(viper.GetString("execdb.user")),
		postgresqlexecdb.WithPassword(viper.GetString("execdb.password")),
		postgresqlexecdb.WithPort(viper.GetInt32("execdb.port")),
	}

	if viper.GetString("execdb.client-cert") != "" {
		clientCert, err := majordomo.Fetch(ctx, viper.GetString("execdb.client-cert"))
		if err != nil {
			return nil, errors.Wrap(err, "failed to read client certificate")
		}
		opts = append(opts, postgresqlexecdb.WithClientCert(clientCert))
	}

	if viper.GetString("execdb.client-key") != "" {
		clientKey, err := majordomo.Fetch(ctx, viper.GetString("execdb.client-key"))
		if err != nil {
			return nil, errors.Wrap(err, "failed to read client key")
		}
		opts = append(opts, postgresqlexecdb.WithClientKey(clientKey))
	}

	if viper.GetString("execdb.ca-cert") != "" {
		caCert, err := majordomo.Fetch(ctx, viper.GetString("execdb.ca-cert"))
		if err != nil {
			return nil, errors.Wrap(err, "failed to read certificate authority certificate")
		}
		opts = append(opts, postgresqlexecdb.WithCACert(caCert))
	}

	return postgresqlexecdb.New(ctx, opts...)
}

// InitComptrollerDB initialises the comptroller database.
func InitComptrollerDB(ctx context.Context, majordomo majordomo.Service) (*postgresqlcomptrollerdb.Service, error) {
	opts := []postgresqlcomptrollerdb.Parameter{
		postgresqlcomptrollerdb.WithLogLevel(LogLevel("comptrollerdb")),
		postgresqlcomptrollerdb.WithServer(viper.GetString("comptrollerdb.server")),
		postgresqlcomptrollerdb.WithUser(viper.GetString("comptrollerdb.user")),
		postgresqlcomptrollerdb.WithPassword(viper.GetString("comptrollerdb.password")),
		postgresqlcomptrollerdb.WithPort(viper.GetInt32("comptrollerdb.port")),
		postgresqlcomptrollerdb.WithMaxConnections(viper.GetUint("comptrollerdb.max-connections")),
	}

	if viper.GetString("comptrollerdb.client-cert") != "" {
		clientCert, err := majordomo.Fetch(ctx, viper.GetString("comptrollerdb.client-cert"))
		if err != nil {
			return nil, errors.Wrap(err, "failed to read client certificate")
		}
		opts = append(opts, postgresqlcomptrollerdb.WithClientCert(clientCert))
	}

	if viper.GetString("comptrollerdb.client-key") != "" {
		clientKey, err := majordomo.Fetch(ctx, viper.GetString("comptrollerdb.client-key"))
		if err != nil {
			return nil, errors.Wrap(err, "failed to read client key")
		}
		opts = append(opts, postgresqlcomptrollerdb.WithClientKey(clientKey))
	}

	if viper.GetString("comptrollerdb.ca-cert") != "" {
		caCert, err := majordomo.Fetch(ctx, viper.GetString("comptrollerdb.ca-cert"))
		if err != nil {
			return nil, errors.Wrap(err, "failed to read certificate authority certificate")
		}
		opts = append(opts, postgresqlcomptrollerdb.WithCACert(caCert))
	}

	return postgresqlcomptrollerdb.New(ctx, opts...)
}
