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

package postgresql_test

import (
	"context"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wealdtech/comptrollerd/services/comptrollerdb"
	"github.com/wealdtech/comptrollerd/services/comptrollerdb/postgresql"
)

func atoi(input string) int32 {
	val, err := strconv.ParseInt(input, 10, 32)
	if err != nil {
		val = -1
	}
	return int32(val)
}

func TestService(t *testing.T) {
	tests := []struct {
		name     string
		server   string
		port     int32
		user     string
		password string
		err      string
	}{
		{
			name: "ServerMissing",
			err:  "problem with parameters: no server specified",
		},
		{
			name:     "Good",
			server:   os.Getenv("COMPTROLLERDB_SERVER"),
			port:     atoi(os.Getenv("COMPTROLLERDB_PORT")),
			user:     os.Getenv("COMPTROLLERDB_USER"),
			password: os.Getenv("COMPTROLLERDB_PASSWORD"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			_, err := postgresql.New(ctx,
				postgresql.WithServer(test.server),
				postgresql.WithPort(test.port),
				postgresql.WithUser(test.user),
				postgresql.WithPassword(test.password),
			)
			if test.err != "" {
				assert.EqualError(t, err, test.err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestInterfaces(t *testing.T) {
	ctx := context.Background()
	s, err := postgresql.New(ctx,
		postgresql.WithServer(os.Getenv("COMPTROLLERDB_SERVER")),
		postgresql.WithPort(atoi(os.Getenv("COMPTROLLERDB_PORT"))),
		postgresql.WithUser(os.Getenv("COMPTROLLERDB_USER")),
		postgresql.WithPassword(os.Getenv("COMPTROLLERDB_PASSWORD")),
	)
	require.NoError(t, err)

	require.Implements(t, (*comptrollerdb.Service)(nil), s)
	require.Implements(t, (*comptrollerdb.ValidatorRegistrationsProvider)(nil), s)
	require.Implements(t, (*comptrollerdb.ValidatorRegistrationsSetter)(nil), s)
}
