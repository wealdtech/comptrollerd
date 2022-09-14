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

package postgresql

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"strings"

	"github.com/jackc/pgtype"
	shopspring "github.com/jackc/pgtype/ext/shopspring-numeric"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	zerologger "github.com/rs/zerolog/log"
)

// Service is an comptroller database service.
type Service struct {
	pool *pgxpool.Pool
}

// module-wide log.
var log zerolog.Logger

// New creates a new service.
func New(ctx context.Context, params ...Parameter) (*Service, error) {
	parameters, err := parseAndCheckParameters(params...)
	if err != nil {
		return nil, errors.Wrap(err, "problem with parameters")
	}

	// Set logging.
	log = zerologger.With().Str("service", "comptrollerdb").Str("impl", "postgresql").Logger().Level(parameters.logLevel)

	var pool *pgxpool.Pool
	dsnItems := make([]string, 0, 16)
	dsnItems = append(dsnItems, fmt.Sprintf("host=%s", parameters.server))
	dsnItems = append(dsnItems, fmt.Sprintf("user=%s", parameters.user))
	if parameters.password != "" {
		dsnItems = append(dsnItems, fmt.Sprintf("password=%s", parameters.password))
	}
	dsnItems = append(dsnItems, fmt.Sprintf("port=%d", parameters.port))

	var tlsConfig *tls.Config
	if parameters.caCert != nil || parameters.clientCert != nil {
		dsnItems = append(dsnItems, "sslmode=verify-full")

		// Add TLS configuration.
		tlsConfig = &tls.Config{
			ServerName: parameters.server,
			MinVersion: tls.VersionTLS13,
		}
		if parameters.clientCert != nil {
			clientPair, err := tls.X509KeyPair(parameters.clientCert, parameters.clientKey)
			if err != nil {
				return nil, errors.Wrap(err, "failed to create client certificate")
			}
			tlsConfig.Certificates = []tls.Certificate{clientPair}
		}
		if parameters.caCert != nil {
			rootCAs := x509.NewCertPool()
			if !rootCAs.AppendCertsFromPEM(parameters.caCert) {
				return nil, errors.New("failed to append root CA certificates")
			}
			tlsConfig.RootCAs = rootCAs
		}
	}

	config, err := pgxpool.ParseConfig(strings.Join(dsnItems, " "))
	if err != nil {
		return nil, errors.Wrap(err, "failed to generate pgx config")
	}
	config.AfterConnect = registerCustomTypes
	config.ConnConfig.TLSConfig = tlsConfig

	pool, err = pgxpool.ConnectConfig(context.Background(), config)
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect to database")
	}

	go func() {
		<-ctx.Done()
		log.Trace().Msg("Context done; closing pool")
		pool.Close()
	}()

	s := &Service{
		pool: pool,
	}

	return s, nil
}

func registerCustomTypes(ctx context.Context, conn *pgx.Conn) error {
	conn.ConnInfo().RegisterDataType(pgtype.DataType{
		Value: &shopspring.Numeric{},
		Name:  "numeric",
		OID:   pgtype.NumericOID,
	})
	return nil
}
