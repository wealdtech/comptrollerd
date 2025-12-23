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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	relayclient "github.com/attestantio/go-relay-client"

	chaintimeMocks "github.com/wealdtech/comptrollerd/mocks/chaintime"
	clientMocks "github.com/wealdtech/comptrollerd/mocks/client"
	comptrollerdbMocks "github.com/wealdtech/comptrollerd/mocks/comptrollerdb"
	schedulerMocks "github.com/wealdtech/comptrollerd/mocks/scheduler"
)

func TestParseAndCheckParametersValid(t *testing.T) {
	params, err := parseAndCheckParameters(
		WithScheduler(schedulerMocks.NewMockService(t)),
		WithChainTime(chaintimeMocks.NewMockService(t)),
		WithReceivedBidTracesProviders([]relayclient.ReceivedBidTracesProvider{clientMocks.NewMockReceivedBidTracesProvider(t)}),
		WithReceivedBidsSetter(comptrollerdbMocks.NewMockReceivedBidsSetter(t)),
		WithDeliveredBidTraceProviders([]relayclient.DeliveredBidTraceProvider{clientMocks.NewMockDeliveredBidTraceProvider(t)}),
		WithDeliveredBidsSetter(comptrollerdbMocks.NewMockDeliveredBidsSetter(t)),
		WithInterval(time.Second),
	)
	require.NoError(t, err)
	require.NotNil(t, params)
}

func TestParseAndCheckParametersMissingScheduler(t *testing.T) {
	_, err := parseAndCheckParameters(
		WithChainTime(chaintimeMocks.NewMockService(t)),
		WithReceivedBidTracesProviders([]relayclient.ReceivedBidTracesProvider{clientMocks.NewMockReceivedBidTracesProvider(t)}),
		WithReceivedBidsSetter(comptrollerdbMocks.NewMockReceivedBidsSetter(t)),
		WithDeliveredBidTraceProviders([]relayclient.DeliveredBidTraceProvider{clientMocks.NewMockDeliveredBidTraceProvider(t)}),
		WithDeliveredBidsSetter(comptrollerdbMocks.NewMockDeliveredBidsSetter(t)),
		WithInterval(time.Second),
	)
	require.EqualError(t, err, "no scheduler specified")
}

func TestParseAndCheckParametersMissingChainTime(t *testing.T) {
	_, err := parseAndCheckParameters(
		WithScheduler(schedulerMocks.NewMockService(t)),
		WithReceivedBidTracesProviders([]relayclient.ReceivedBidTracesProvider{clientMocks.NewMockReceivedBidTracesProvider(t)}),
		WithReceivedBidsSetter(comptrollerdbMocks.NewMockReceivedBidsSetter(t)),
		WithDeliveredBidTraceProviders([]relayclient.DeliveredBidTraceProvider{clientMocks.NewMockDeliveredBidTraceProvider(t)}),
		WithDeliveredBidsSetter(comptrollerdbMocks.NewMockDeliveredBidsSetter(t)),
		WithInterval(time.Second),
	)
	require.EqualError(t, err, "no chain time specified")
}

func TestParseAndCheckParametersMissingReceivedBidTracesProviders(t *testing.T) {
	_, err := parseAndCheckParameters(
		WithScheduler(schedulerMocks.NewMockService(t)),
		WithChainTime(chaintimeMocks.NewMockService(t)),
		WithReceivedBidsSetter(comptrollerdbMocks.NewMockReceivedBidsSetter(t)),
		WithDeliveredBidTraceProviders([]relayclient.DeliveredBidTraceProvider{clientMocks.NewMockDeliveredBidTraceProvider(t)}),
		WithDeliveredBidsSetter(comptrollerdbMocks.NewMockDeliveredBidsSetter(t)),
		WithInterval(time.Second),
	)
	require.EqualError(t, err, "no received bid traces providers specified")
}

func TestParseAndCheckParametersMissingReceivedBidsSetter(t *testing.T) {
	_, err := parseAndCheckParameters(
		WithScheduler(schedulerMocks.NewMockService(t)),
		WithChainTime(chaintimeMocks.NewMockService(t)),
		WithReceivedBidTracesProviders([]relayclient.ReceivedBidTracesProvider{clientMocks.NewMockReceivedBidTracesProvider(t)}),
		WithDeliveredBidTraceProviders([]relayclient.DeliveredBidTraceProvider{clientMocks.NewMockDeliveredBidTraceProvider(t)}),
		WithDeliveredBidsSetter(comptrollerdbMocks.NewMockDeliveredBidsSetter(t)),
		WithInterval(time.Second),
	)
	require.EqualError(t, err, "no received bids setter specified")
}

func TestParseAndCheckParametersMissingDeliveredBidTraceProviders(t *testing.T) {
	_, err := parseAndCheckParameters(
		WithScheduler(schedulerMocks.NewMockService(t)),
		WithChainTime(chaintimeMocks.NewMockService(t)),
		WithReceivedBidTracesProviders([]relayclient.ReceivedBidTracesProvider{clientMocks.NewMockReceivedBidTracesProvider(t)}),
		WithReceivedBidsSetter(comptrollerdbMocks.NewMockReceivedBidsSetter(t)),
		WithDeliveredBidsSetter(comptrollerdbMocks.NewMockDeliveredBidsSetter(t)),
		WithInterval(time.Second),
	)
	require.EqualError(t, err, "no delivered bid trace providers specified")
}

func TestParseAndCheckParametersMissingDeliveredBidsSetter(t *testing.T) {
	_, err := parseAndCheckParameters(
		WithScheduler(schedulerMocks.NewMockService(t)),
		WithChainTime(chaintimeMocks.NewMockService(t)),
		WithReceivedBidTracesProviders([]relayclient.ReceivedBidTracesProvider{clientMocks.NewMockReceivedBidTracesProvider(t)}),
		WithReceivedBidsSetter(comptrollerdbMocks.NewMockReceivedBidsSetter(t)),
		WithDeliveredBidTraceProviders([]relayclient.DeliveredBidTraceProvider{clientMocks.NewMockDeliveredBidTraceProvider(t)}),
		WithInterval(time.Second),
	)
	require.EqualError(t, err, "no delivered bids setter specified")
}

func TestParseAndCheckParametersMissingInterval(t *testing.T) {
	_, err := parseAndCheckParameters(
		WithScheduler(schedulerMocks.NewMockService(t)),
		WithChainTime(chaintimeMocks.NewMockService(t)),
		WithReceivedBidTracesProviders([]relayclient.ReceivedBidTracesProvider{clientMocks.NewMockReceivedBidTracesProvider(t)}),
		WithReceivedBidsSetter(comptrollerdbMocks.NewMockReceivedBidsSetter(t)),
		WithDeliveredBidTraceProviders([]relayclient.DeliveredBidTraceProvider{clientMocks.NewMockDeliveredBidTraceProvider(t)}),
		WithDeliveredBidsSetter(comptrollerdbMocks.NewMockDeliveredBidsSetter(t)),
	)
	require.EqualError(t, err, "no interval specified")
}

func TestParseAndCheckParametersWithStartSlot(t *testing.T) {
	params, err := parseAndCheckParameters(
		WithScheduler(schedulerMocks.NewMockService(t)),
		WithChainTime(chaintimeMocks.NewMockService(t)),
		WithReceivedBidTracesProviders([]relayclient.ReceivedBidTracesProvider{clientMocks.NewMockReceivedBidTracesProvider(t)}),
		WithReceivedBidsSetter(comptrollerdbMocks.NewMockReceivedBidsSetter(t)),
		WithDeliveredBidTraceProviders([]relayclient.DeliveredBidTraceProvider{clientMocks.NewMockDeliveredBidTraceProvider(t)}),
		WithDeliveredBidsSetter(comptrollerdbMocks.NewMockDeliveredBidsSetter(t)),
		WithInterval(time.Second),
		WithStartSlot(1000),
	)
	require.NoError(t, err)
	require.NotNil(t, params)
	require.Equal(t, int64(1000), params.startSlot)
}
