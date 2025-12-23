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
	"fmt"
	"testing"
	"time"

	relayclient "github.com/attestantio/go-relay-client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	bidsSvc "github.com/wealdtech/comptrollerd/services/bids"
	chaintimeSvc "github.com/wealdtech/comptrollerd/services/chaintime"
	comptrollerdbSvc "github.com/wealdtech/comptrollerd/services/comptrollerdb"
	schedulerSvc "github.com/wealdtech/comptrollerd/services/scheduler"

	chaintimeMocks "github.com/wealdtech/comptrollerd/mocks/chaintime"
	clientMocks "github.com/wealdtech/comptrollerd/mocks/client"
	comptrollerdbMocks "github.com/wealdtech/comptrollerd/mocks/comptrollerdb"
	schedulerMocks "github.com/wealdtech/comptrollerd/mocks/scheduler"
)

// TestNewWithoutStartSlot verifies that startSlot < 0 skips metadata update
func TestNewWithoutStartSlot(t *testing.T) {
	const numTestProviders = 2

	receivedSetter := comptrollerdbMocks.NewMockReceivedBidsSetter(t)
	receivedSetter.EXPECT().Metadata(mock.Anything, metadataKey).Return(nil, nil).Maybe()

	providers := make([]relayclient.ReceivedBidTracesProvider, 0, numTestProviders)
	for i := range numTestProviders {
		provider := clientMocks.NewMockReceivedBidTracesProvider(t)
		provider.EXPECT().Name().Return(fmt.Sprintf("relay_%d", i)).Maybe()
		providers = append(providers, provider)
	}

	deliveredProviders := make([]relayclient.DeliveredBidTraceProvider, 0, numTestProviders)
	for i := range numTestProviders {
		deliveredProvider := clientMocks.NewMockDeliveredBidTraceProvider(t)
		deliveredProvider.EXPECT().Name().Return(fmt.Sprintf("relay_%d", i)).Maybe()
		deliveredProviders = append(deliveredProviders, deliveredProvider)
	}

	svc, err := newTestService(t, -1, providers, deliveredProviders, receivedSetter, nil, nil, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, svc)

}

// TestNewWithStartSlot_Success verifies that startSlot >= 0 sets LatestSlots for each provider.
func TestNewWithStartSlot_Success(t *testing.T) {
	const numTestProviders = 2

	receivedSetter := comptrollerdbMocks.NewMockReceivedBidsSetter(t)
	receivedSetter.EXPECT().Metadata(mock.Anything, metadataKey).Return(nil, nil).Maybe()
	receivedSetter.EXPECT().BeginTx(mock.Anything).Return(t.Context(), func() {}, nil).Once()
	receivedSetter.EXPECT().SetMetadata(mock.Anything, metadataKey, mock.Anything).Return(nil).Once()
	receivedSetter.EXPECT().CommitTx(mock.Anything).Return(nil).Once()

	providers := make([]relayclient.ReceivedBidTracesProvider, 0, numTestProviders)
	for i := range numTestProviders {
		provider := clientMocks.NewMockReceivedBidTracesProvider(t)
		provider.EXPECT().Name().Return(fmt.Sprintf("relay_%d", i)).Maybe()
		providers = append(providers, provider)
	}

	deliveredProviders := make([]relayclient.DeliveredBidTraceProvider, 0, numTestProviders)
	for i := range numTestProviders {
		deliveredProvider := clientMocks.NewMockDeliveredBidTraceProvider(t)
		deliveredProvider.EXPECT().Name().Return(fmt.Sprintf("relay_%d", i)).Maybe()
		deliveredProviders = append(deliveredProviders, deliveredProvider)
	}

	svc, err := newTestService(t, 100, providers, deliveredProviders, receivedSetter, nil, nil, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, svc)
}

// TestNewWithStartSlot_BeginTxError verifies error handling when BeginTx fails.
func TestNewWithStartSlot_BeginTxError(t *testing.T) {
	schedMock := schedulerMocks.NewMockService(t)
	chainTime := chaintimeMocks.NewMockService(t)

	provider := clientMocks.NewMockReceivedBidTracesProvider(t)
	provider.EXPECT().Name().Return("relay1").Maybe()
	providers := []relayclient.ReceivedBidTracesProvider{provider}

	deliveredProvider := clientMocks.NewMockDeliveredBidTraceProvider(t)
	deliveredProvider.EXPECT().Name().Return("relay1").Maybe()
	deliveredProviders := []relayclient.DeliveredBidTraceProvider{deliveredProvider}

	receivedSetter := comptrollerdbMocks.NewMockReceivedBidsSetter(t)
	receivedSetter.EXPECT().Metadata(mock.Anything, metadataKey).Return(nil, nil).Maybe()
	receivedSetter.EXPECT().BeginTx(mock.Anything).Return(t.Context(), nil, assert.AnError).Once()

	svc, err := newTestService(t, 100, providers, deliveredProviders, receivedSetter, nil, schedMock, chainTime, nil)
	require.Error(t, err)
	require.Nil(t, svc)
	assert.Contains(t, err.Error(), "failed to begin transaction")
}

// TestNewWithStartSlot_SetMetadataError verifies error handling when SetMetadata fails.
func TestNewWithStartSlot_SetMetadataError(t *testing.T) {
	schedMock := schedulerMocks.NewMockService(t)
	chainTime := chaintimeMocks.NewMockService(t)

	provider := clientMocks.NewMockReceivedBidTracesProvider(t)
	provider.EXPECT().Name().Return("relay1").Maybe()
	providers := []relayclient.ReceivedBidTracesProvider{provider}

	deliveredProvider := clientMocks.NewMockDeliveredBidTraceProvider(t)
	deliveredProvider.EXPECT().Name().Return("relay1").Maybe()
	deliveredProviders := []relayclient.DeliveredBidTraceProvider{deliveredProvider}

	receivedSetter := comptrollerdbMocks.NewMockReceivedBidsSetter(t)
	receivedSetter.EXPECT().Metadata(mock.Anything, metadataKey).Return(nil, nil)
	receivedSetter.EXPECT().BeginTx(mock.Anything).Return(t.Context(), func() {}, nil)
	receivedSetter.EXPECT().SetMetadata(mock.Anything, metadataKey, mock.Anything).Return(assert.AnError)

	svc, err := newTestService(t, 100, providers, deliveredProviders, receivedSetter, nil, schedMock, chainTime, nil)
	require.Error(t, err)
	require.Nil(t, svc)
	assert.Contains(t, err.Error(), "failed to set metadata")
}

// TestNewWithStartSlot_CommitTxError verifies error handling when CommitTx fails.
func TestNewWithStartSlot_CommitTxError(t *testing.T) {
	schedMock := schedulerMocks.NewMockService(t)
	chainTime := chaintimeMocks.NewMockService(t)

	provider := clientMocks.NewMockReceivedBidTracesProvider(t)
	provider.EXPECT().Name().Return("relay1").Maybe()
	providers := []relayclient.ReceivedBidTracesProvider{provider}

	deliveredProvider := clientMocks.NewMockDeliveredBidTraceProvider(t)
	deliveredProvider.EXPECT().Name().Return("relay1").Maybe()
	deliveredProviders := []relayclient.DeliveredBidTraceProvider{deliveredProvider}

	receivedSetter := comptrollerdbMocks.NewMockReceivedBidsSetter(t)
	receivedSetter.EXPECT().Metadata(mock.Anything, metadataKey).Return(nil, nil)
	receivedSetter.EXPECT().BeginTx(mock.Anything).Return(t.Context(), func() {}, nil)
	receivedSetter.EXPECT().SetMetadata(mock.Anything, metadataKey, mock.Anything).Return(nil)
	receivedSetter.EXPECT().CommitTx(mock.Anything).Return(assert.AnError)

	svc, err := newTestService(t, 100, providers, deliveredProviders, receivedSetter, nil, schedMock, chainTime, nil)
	require.Error(t, err)
	require.Nil(t, svc)
	assert.Contains(t, err.Error(), "failed to commit metadata transaction")
}

// TestNewSchedulesPeriodicJob verifies that SchedulePeriodicJob is called correctly.
func TestNewSchedulesPeriodicJob(t *testing.T) {
	mockScheduler := schedulerMocks.NewMockService(t)
	mockScheduler.EXPECT().SchedulePeriodicJob(mock.Anything, "bids", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	mockChainTime := chaintimeMocks.NewMockService(t)
	mockChainTime.EXPECT().CurrentSlot().Return(0).Maybe()

	provider := clientMocks.NewMockReceivedBidTracesProvider(t)
	provider.EXPECT().Name().Return("relay1").Maybe()
	provider.EXPECT().ReceivedBidTraces(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	providers := []relayclient.ReceivedBidTracesProvider{provider}

	deliveredProvider := clientMocks.NewMockDeliveredBidTraceProvider(t)
	deliveredProvider.EXPECT().Name().Return("relay1").Maybe()
	deliveredProviders := []relayclient.DeliveredBidTraceProvider{deliveredProvider}

	receivedSetter := comptrollerdbMocks.NewMockReceivedBidsSetter(t)
	receivedSetter.EXPECT().Metadata(mock.Anything, metadataKey).Return(nil, nil).Maybe()

	svc, err := newTestService(t, -1, providers, deliveredProviders, receivedSetter, nil, mockScheduler, mockChainTime, nil)
	require.NoError(t, err)
	require.NotNil(t, svc)
}

// newTestService creates a Service instance for testing purposes.
// Parameters that are nil will be initialized with mock implementations:
// - schedulerSvc: mocked to accept SchedulePeriodicJob calls
// - chainTimeSvc: mocked to return incrementing slot numbers on each CurrentSlot() call
// Other parameters (providers, deliveredProviders, receivedSetter, handlers) can be nil or provided as needed.
// Returns the initialized Service and any error encountered during creation.
func newTestService(t *testing.T,
	startSlot int64,
	providers []relayclient.ReceivedBidTracesProvider,
	deliveredProviders []relayclient.DeliveredBidTraceProvider,
	receivedSetter comptrollerdbSvc.ReceivedBidsSetter,
	deliveredSetter comptrollerdbSvc.DeliveredBidsSetter,
	schedulerSvc schedulerSvc.Service,
	chainTimeSvc chaintimeSvc.Service,
	handlers []bidsSvc.ReceivedHandler,
) (*Service, error) {
	if deliveredSetter == nil {
		deliveredSetter = &noopDeliveredBidsSetter{}
	}

	if schedulerSvc == nil {
		schedulerMock := schedulerMocks.NewMockService(t)
		schedulerMock.EXPECT().SchedulePeriodicJob(
			t.Context(),   // context
			mock.Anything, // class
			mock.Anything, // name
			mock.Anything, // runtime
			mock.Anything, // runtimeData
			mock.Anything, // job
			mock.Anything, // jobData
		).Return(nil).Once()
		schedulerSvc = schedulerMock
	}

	if chainTimeSvc == nil {
		mockChainTime := chaintimeMocks.NewMockService(t)
		mockChainTime.EXPECT().CurrentSlot().Return(0).Maybe()
		chainTimeSvc = mockChainTime
	}

	params := []Parameter{
		WithScheduler(schedulerSvc),
		WithChainTime(chainTimeSvc),
		WithReceivedBidTracesProviders(providers),
		WithDeliveredBidTraceProviders(deliveredProviders),
		WithReceivedBidsSetter(receivedSetter),
		WithDeliveredBidsSetter(deliveredSetter),
		WithBidsReceivedHandlers(handlers),
		WithInterval(1 * time.Second),
		WithStartSlot(startSlot),
	}

	return New(t.Context(), params...)
}

// noopDeliveredBidsSetter is a minimal, in-test implementation of
// comptrollerdb.DeliveredBidsSetter used where tests do not need DB writes.
type noopDeliveredBidsSetter struct{}

func (n *noopDeliveredBidsSetter) BeginTx(ctx context.Context) (context.Context, context.CancelFunc, error) {
	return ctx, func() {}, nil
}

func (n *noopDeliveredBidsSetter) CommitTx(ctx context.Context) error {
	return nil
}

func (n *noopDeliveredBidsSetter) SetMetadata(ctx context.Context, key string, value []byte) error {
	return nil
}

func (n *noopDeliveredBidsSetter) Metadata(ctx context.Context, key string) ([]byte, error) {
	return nil, nil
}

func (n *noopDeliveredBidsSetter) SetDeliveredBid(ctx context.Context, bid *comptrollerdbSvc.DeliveredBid) error {
	return nil
}
