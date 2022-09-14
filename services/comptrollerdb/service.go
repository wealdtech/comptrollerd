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
	"context"
)

// Service defines a minimal comptroller database service.
type Service interface {
	// BeginTx begins a transaction.
	BeginTx(ctx context.Context) (context.Context, context.CancelFunc, error)

	// CommitTx commits a transaction.
	CommitTx(ctx context.Context) error

	// SetMetadata sets a metadata key to a JSON value.
	SetMetadata(ctx context.Context, key string, value []byte) error

	// Metadata obtains the JSON value from a metadata key.
	Metadata(ctx context.Context, key string) ([]byte, error)
}

// BlockPaymentsProvider defines functions to provide block payment information.
type BlockPaymentsProvider interface {
	// BlockPayments returns block payments matching the supplied filter.
	BlockPayments(ctx context.Context, filter *BlockPaymentFilter) ([]*BlockPayment, error)
}

// BlockPaymentsSetter defines functions to create and update block payments.
type BlockPaymentsSetter interface {
	Service

	// SetBlockPayment sets a block payment.
	SetBlockPayment(ctx context.Context, payment *BlockPayment) error
}

// ValidatorRegistrationsProvider defines functions to provide validator registration information.
type ValidatorRegistrationsProvider interface {
	// ValidatorRegistrations returns validator registrations matching the supplied filter.
	ValidatorRegistrations(ctx context.Context, filter *ValidatorRegistrationFilter) ([]*ValidatorRegistration, error)
}

// ValidatorRegistrationsSetter defines functions to create and update validator registrations.
type ValidatorRegistrationsSetter interface {
	Service

	// SetValidatorRegistration sets a validator registration.
	SetValidatorRegistration(ctx context.Context, registration *ValidatorRegistration) error
}
