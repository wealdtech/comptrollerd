package standard

import (
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/wealdtech/comptrollerd/services/bids"
	"github.com/wealdtech/comptrollerd/services/comptrollerdb"

	"github.com/attestantio/go-eth2-client/spec/phase0"
	relayclient "github.com/attestantio/go-relay-client"
	v1 "github.com/attestantio/go-relay-client/api/v1"

	bidsMocks "github.com/wealdtech/comptrollerd/mocks/bids"
	chaintimeMocks "github.com/wealdtech/comptrollerd/mocks/chaintime"
	clientMocks "github.com/wealdtech/comptrollerd/mocks/client"
	comptrollerdbMocks "github.com/wealdtech/comptrollerd/mocks/comptrollerdb"
)

func TestHandleDeliveredBids_Success(t *testing.T) {
	ctx := t.Context()
	provider := "test-relay"
	providerIndex := 0
	slot := phase0.Slot(100)

	mockDeliveredBidTraceProvider := clientMocks.NewMockDeliveredBidTraceProvider(t)
	mockDeliveredBidsSetter := comptrollerdbMocks.NewMockDeliveredBidsSetter(t)

	bidTrace := &v1.BidTrace{
		Slot:                 slot,
		ParentHash:           [32]byte{1},
		BlockHash:            [32]byte{2},
		BuilderPubkey:        [48]byte{3},
		ProposerPubkey:       [48]byte{4},
		ProposerFeeRecipient: [20]byte{5},
		GasLimit:             30000000,
		GasUsed:              15000000,
		Value:                big.NewInt(1234567890),
	}

	mockDeliveredBidTraceProvider.EXPECT().DeliveredBidTrace(mock.Anything, slot).Return(bidTrace, nil)
	mockDeliveredBidsSetter.EXPECT().SetDeliveredBid(mock.Anything, mock.MatchedBy(func(bid *comptrollerdb.DeliveredBid) bool {
		return bid.Slot == uint32(slot) && bid.Relay == provider
	})).Return(nil)

	s := &Service{
		deliveredBidTraceProviders: []relayclient.DeliveredBidTraceProvider{mockDeliveredBidTraceProvider},
		deliveredBidsSetter:        mockDeliveredBidsSetter,
	}

	err := s.handleDeliveredBids(ctx, provider, providerIndex, slot)
	require.NoError(t, err)
	mockDeliveredBidTraceProvider.AssertExpectations(t)
	mockDeliveredBidsSetter.AssertExpectations(t)
}

func TestHandleDeliveredBids_NilBidTrace(t *testing.T) {
	ctx := t.Context()
	provider := "test-relay"
	providerIndex := 0
	slot := phase0.Slot(100)

	mockDeliveredBidTraceProvider := clientMocks.NewMockDeliveredBidTraceProvider(t)
	mockDeliveredBidTraceProvider.EXPECT().DeliveredBidTrace(mock.Anything, slot).Return(nil, nil)

	s := &Service{
		deliveredBidTraceProviders: []relayclient.DeliveredBidTraceProvider{mockDeliveredBidTraceProvider},
	}

	err := s.handleDeliveredBids(ctx, provider, providerIndex, slot)
	require.NoError(t, err)
	mockDeliveredBidTraceProvider.AssertExpectations(t)
}

func TestHandleDeliveredBids_ProviderError(t *testing.T) {
	ctx := t.Context()
	provider := "test-relay"
	providerIndex := 0
	slot := phase0.Slot(100)

	mockDeliveredBidTraceProvider := clientMocks.NewMockDeliveredBidTraceProvider(t)
	mockDeliveredBidTraceProvider.EXPECT().DeliveredBidTrace(mock.Anything, slot).Return(nil, errors.New("provider error"))

	s := &Service{
		deliveredBidTraceProviders: []relayclient.DeliveredBidTraceProvider{mockDeliveredBidTraceProvider},
	}

	err := s.handleDeliveredBids(ctx, provider, providerIndex, slot)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to obtain delivered bid trace")
	mockDeliveredBidTraceProvider.AssertExpectations(t)
}

func TestHandleDeliveredBids_SetDeliveredBidError(t *testing.T) {
	ctx := t.Context()
	provider := "test-relay"
	providerIndex := 0
	slot := phase0.Slot(100)

	mockDeliveredBidTraceProvider := clientMocks.NewMockDeliveredBidTraceProvider(t)
	mockDeliveredBidsSetter := comptrollerdbMocks.NewMockDeliveredBidsSetter(t)

	bidTrace := &v1.BidTrace{
		Slot:                 slot,
		ParentHash:           [32]byte{1},
		BlockHash:            [32]byte{2},
		BuilderPubkey:        [48]byte{3},
		ProposerPubkey:       [48]byte{4},
		ProposerFeeRecipient: [20]byte{5},
		GasLimit:             30000000,
		GasUsed:              15000000,
		Value:                big.NewInt(1234567890),
	}

	mockDeliveredBidTraceProvider.EXPECT().DeliveredBidTrace(mock.Anything, slot).Return(bidTrace, nil)
	mockDeliveredBidsSetter.EXPECT().SetDeliveredBid(mock.Anything, mock.Anything).Return(errors.New("db error"))

	s := &Service{
		deliveredBidTraceProviders: []relayclient.DeliveredBidTraceProvider{mockDeliveredBidTraceProvider},
		deliveredBidsSetter:        mockDeliveredBidsSetter,
	}

	err := s.handleDeliveredBids(ctx, provider, providerIndex, slot)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to set delivered bid")
}
func TestHandleReceivedBids_Success(t *testing.T) {
	ctx := t.Context()
	provider := "test-relay"
	providerIndex := 0
	slot := phase0.Slot(100)

	mockReceivedBidTraceProvider := clientMocks.NewMockReceivedBidTracesProvider(t)
	mockReceivedBidsSetter := comptrollerdbMocks.NewMockReceivedBidsSetter(t)

	bidTraces := []*v1.BidTraceWithTimestamp{
		{
			Slot:                 slot,
			ParentHash:           [32]byte{1},
			BlockHash:            [32]byte{2},
			BuilderPubkey:        [48]byte{3},
			ProposerPubkey:       [48]byte{4},
			ProposerFeeRecipient: [20]byte{5},
			GasLimit:             30000000,
			GasUsed:              15000000,
			Value:                big.NewInt(1234567890),
			Timestamp:            time.Unix(1000, 0),
		},
	}

	mockReceivedBidTraceProvider.EXPECT().ReceivedBidTraces(mock.Anything, slot).Return(bidTraces, nil)
	mockReceivedBidsSetter.EXPECT().SetReceivedBids(mock.Anything, mock.MatchedBy(func(bids []*comptrollerdb.ReceivedBid) bool {
		return len(bids) == 1 && bids[0].Slot == uint32(slot) && bids[0].Relay == provider
	})).Return(nil)

	s := &Service{
		receivedBidTracesProviders: []relayclient.ReceivedBidTracesProvider{mockReceivedBidTraceProvider},
		receivedBidsSetter:         mockReceivedBidsSetter,
	}

	err := s.handleReceivedBids(ctx, provider, providerIndex, slot)
	require.NoError(t, err)
	mockReceivedBidTraceProvider.AssertExpectations(t)
	mockReceivedBidsSetter.AssertExpectations(t)
}

func TestHandleReceivedBids_DeduplicateBids(t *testing.T) {
	ctx := t.Context()
	provider := "test-relay"
	providerIndex := 0
	slot := phase0.Slot(100)

	mockReceivedBidTraceProvider := clientMocks.NewMockReceivedBidTracesProvider(t)
	mockReceivedBidsSetter := comptrollerdbMocks.NewMockReceivedBidsSetter(t)

	// Same bid returned twice
	bidTrace := &v1.BidTraceWithTimestamp{
		Slot:                 slot,
		ParentHash:           [32]byte{1},
		BlockHash:            [32]byte{2},
		BuilderPubkey:        [48]byte{3},
		ProposerPubkey:       [48]byte{4},
		ProposerFeeRecipient: [20]byte{5},
		GasLimit:             30000000,
		GasUsed:              15000000,
		Value:                big.NewInt(1234567890),
		Timestamp:            time.Unix(1000, 0),
	}

	bidTraces := []*v1.BidTraceWithTimestamp{bidTrace, bidTrace}

	mockReceivedBidTraceProvider.EXPECT().ReceivedBidTraces(mock.Anything, slot).Return(bidTraces, nil)
	mockReceivedBidsSetter.EXPECT().SetReceivedBids(mock.Anything, mock.MatchedBy(func(bids []*comptrollerdb.ReceivedBid) bool {
		return len(bids) == 1
	})).Return(nil)

	s := &Service{
		receivedBidTracesProviders: []relayclient.ReceivedBidTracesProvider{mockReceivedBidTraceProvider},
		receivedBidsSetter:         mockReceivedBidsSetter,
	}

	err := s.handleReceivedBids(ctx, provider, providerIndex, slot)
	require.NoError(t, err)
	mockReceivedBidTraceProvider.AssertExpectations(t)
	mockReceivedBidsSetter.AssertExpectations(t)
}

func TestHandleReceivedBids_ProviderError(t *testing.T) {
	ctx := t.Context()
	provider := "test-relay"
	providerIndex := 0
	slot := phase0.Slot(100)

	mockReceivedBidTraceProvider := clientMocks.NewMockReceivedBidTracesProvider(t)
	mockReceivedBidTraceProvider.EXPECT().ReceivedBidTraces(mock.Anything, slot).Return(nil, errors.New("provider error"))

	s := &Service{
		receivedBidTracesProviders: []relayclient.ReceivedBidTracesProvider{mockReceivedBidTraceProvider},
	}

	err := s.handleReceivedBids(ctx, provider, providerIndex, slot)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to obtain received bid traces")
	mockReceivedBidTraceProvider.AssertExpectations(t)
}

func TestHandleReceivedBids_SetError(t *testing.T) {
	ctx := t.Context()
	provider := "test-relay"
	providerIndex := 0
	slot := phase0.Slot(100)

	mockReceivedBidTraceProvider := clientMocks.NewMockReceivedBidTracesProvider(t)
	mockReceivedBidsSetter := comptrollerdbMocks.NewMockReceivedBidsSetter(t)

	bidTraces := []*v1.BidTraceWithTimestamp{
		{
			Slot:                 slot,
			ParentHash:           [32]byte{1},
			BlockHash:            [32]byte{2},
			BuilderPubkey:        [48]byte{3},
			ProposerPubkey:       [48]byte{4},
			ProposerFeeRecipient: [20]byte{5},
			GasLimit:             30000000,
			GasUsed:              15000000,
			Value:                big.NewInt(1234567890),
			Timestamp:            time.Unix(1000, 0),
		},
	}

	mockReceivedBidTraceProvider.EXPECT().ReceivedBidTraces(mock.Anything, slot).Return(bidTraces, nil)
	mockReceivedBidsSetter.EXPECT().SetReceivedBids(mock.Anything, mock.Anything).Return(errors.New("db error"))

	s := &Service{
		receivedBidTracesProviders: []relayclient.ReceivedBidTracesProvider{mockReceivedBidTraceProvider},
		receivedBidsSetter:         mockReceivedBidsSetter,
	}

	err := s.handleReceivedBids(ctx, provider, providerIndex, slot)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to set received bids")
	mockReceivedBidTraceProvider.AssertExpectations(t)
	mockReceivedBidsSetter.AssertExpectations(t)
}

func TestHandleReceivedBids_EmptyBidTraces(t *testing.T) {
	ctx := t.Context()
	provider := "test-relay"
	providerIndex := 0
	slot := phase0.Slot(100)

	mockReceivedBidTraceProvider := clientMocks.NewMockReceivedBidTracesProvider(t)
	mockReceivedBidsSetter := comptrollerdbMocks.NewMockReceivedBidsSetter(t)

	mockReceivedBidTraceProvider.EXPECT().ReceivedBidTraces(mock.Anything, slot).Return([]*v1.BidTraceWithTimestamp{}, nil)
	mockReceivedBidsSetter.EXPECT().SetReceivedBids(mock.Anything, mock.MatchedBy(func(bids []*comptrollerdb.ReceivedBid) bool {
		return len(bids) == 0
	})).Return(nil)

	s := &Service{
		receivedBidTracesProviders: []relayclient.ReceivedBidTracesProvider{mockReceivedBidTraceProvider},
		receivedBidsSetter:         mockReceivedBidsSetter,
	}

	err := s.handleReceivedBids(ctx, provider, providerIndex, slot)
	require.NoError(t, err)
	mockReceivedBidTraceProvider.AssertExpectations(t)
	mockReceivedBidsSetter.AssertExpectations(t)
}
func TestHandleSlot_Success(t *testing.T) {
	ctx := t.Context()
	provider := "test-relay"
	providerIndex := 0
	slot := phase0.Slot(100)

	mockReceivedBidTraceProvider := clientMocks.NewMockReceivedBidTracesProvider(t)
	mockDeliveredBidTraceProvider := clientMocks.NewMockDeliveredBidTraceProvider(t)
	mockReceivedBidsSetter := comptrollerdbMocks.NewMockReceivedBidsSetter(t)
	mockDeliveredBidsSetter := comptrollerdbMocks.NewMockDeliveredBidsSetter(t)

	mockReceivedBidTraceProvider.EXPECT().ReceivedBidTraces(mock.Anything, slot).Return([]*v1.BidTraceWithTimestamp{}, nil)
	mockReceivedBidsSetter.EXPECT().SetReceivedBids(mock.Anything, mock.Anything).Return(nil)

	mockDeliveredBidTraceProvider.EXPECT().DeliveredBidTrace(mock.Anything, slot).Return(nil, nil)

	s := &Service{
		receivedBidTracesProviders: []relayclient.ReceivedBidTracesProvider{mockReceivedBidTraceProvider},
		deliveredBidTraceProviders: []relayclient.DeliveredBidTraceProvider{mockDeliveredBidTraceProvider},
		receivedBidsSetter:         mockReceivedBidsSetter,
		deliveredBidsSetter:        mockDeliveredBidsSetter,
	}

	err := s.handleSlot(ctx, provider, providerIndex, slot)
	require.NoError(t, err)
	mockReceivedBidTraceProvider.AssertExpectations(t)
	mockDeliveredBidTraceProvider.AssertExpectations(t)
	mockReceivedBidsSetter.AssertExpectations(t)
}

func TestHandleSlot_ReceivedBidsError(t *testing.T) {
	ctx := t.Context()
	provider := "test-relay"
	providerIndex := 0
	slot := phase0.Slot(100)

	mockReceivedBidTraceProvider := clientMocks.NewMockReceivedBidTracesProvider(t)
	mockReceivedBidTraceProvider.EXPECT().ReceivedBidTraces(mock.Anything, slot).Return(nil, errors.New("provider error"))

	s := &Service{
		receivedBidTracesProviders: []relayclient.ReceivedBidTracesProvider{mockReceivedBidTraceProvider},
	}

	err := s.handleSlot(ctx, provider, providerIndex, slot)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to handle received bids")
	mockReceivedBidTraceProvider.AssertExpectations(t)
}

func TestHandleSlot_DeliveredBidsError(t *testing.T) {
	ctx := t.Context()
	provider := "test-relay"
	providerIndex := 0
	slot := phase0.Slot(100)

	mockReceivedBidTraceProvider := clientMocks.NewMockReceivedBidTracesProvider(t)
	mockDeliveredBidTraceProvider := clientMocks.NewMockDeliveredBidTraceProvider(t)
	mockReceivedBidsSetter := comptrollerdbMocks.NewMockReceivedBidsSetter(t)

	mockReceivedBidTraceProvider.EXPECT().ReceivedBidTraces(mock.Anything, slot).Return([]*v1.BidTraceWithTimestamp{}, nil)
	mockReceivedBidsSetter.EXPECT().SetReceivedBids(mock.Anything, mock.Anything).Return(nil)

	mockDeliveredBidTraceProvider.EXPECT().DeliveredBidTrace(mock.Anything, slot).Return(nil, errors.New("provider error"))

	s := &Service{
		receivedBidTracesProviders: []relayclient.ReceivedBidTracesProvider{mockReceivedBidTraceProvider},
		deliveredBidTraceProviders: []relayclient.DeliveredBidTraceProvider{mockDeliveredBidTraceProvider},
		receivedBidsSetter:         mockReceivedBidsSetter,
	}

	err := s.handleSlot(ctx, provider, providerIndex, slot)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to handle delivered bid")
	mockReceivedBidTraceProvider.AssertExpectations(t)
	mockDeliveredBidTraceProvider.AssertExpectations(t)
	mockReceivedBidsSetter.AssertExpectations(t)
}
func TestCatchupProviderSlot_Success(t *testing.T) {
	ctx := t.Context()
	provider := "test-relay"
	providerIndex := 0
	slot := phase0.Slot(100)

	mockReceivedBidsSetter := comptrollerdbMocks.NewMockReceivedBidsSetter(t)
	mockReceivedBidTraceProvider := clientMocks.NewMockReceivedBidTracesProvider(t)
	mockDeliveredBidTraceProvider := clientMocks.NewMockDeliveredBidTraceProvider(t)

	mockReceivedBidsSetter.EXPECT().BeginTx(mock.Anything).Return(ctx, func() {}, nil)
	mockReceivedBidTraceProvider.EXPECT().ReceivedBidTraces(mock.Anything, slot).Return([]*v1.BidTraceWithTimestamp{}, nil)
	mockReceivedBidsSetter.EXPECT().SetReceivedBids(mock.Anything, mock.Anything).Return(nil)
	mockDeliveredBidTraceProvider.EXPECT().DeliveredBidTrace(mock.Anything, slot).Return(nil, nil)
	mockReceivedBidsSetter.EXPECT().SetMetadata(mock.Anything, metadataKey, mock.Anything).Return(nil)
	mockReceivedBidsSetter.EXPECT().CommitTx(mock.Anything).Return(nil)

	md := &metadata{
		LatestSlots: make(map[string]int64),
	}

	s := &Service{
		receivedBidTracesProviders: []relayclient.ReceivedBidTracesProvider{mockReceivedBidTraceProvider},
		deliveredBidTraceProviders: []relayclient.DeliveredBidTraceProvider{mockDeliveredBidTraceProvider},
		receivedBidsSetter:         mockReceivedBidsSetter,
		bidsReceivedHandlers:       []bids.ReceivedHandler{},
	}

	err := s.catchupProviderSlot(ctx, md, provider, providerIndex, slot)
	require.NoError(t, err)
	assert.Equal(t, int64(slot), md.LatestSlots[provider])
	mockReceivedBidsSetter.AssertExpectations(t)
	mockReceivedBidTraceProvider.AssertExpectations(t)
	mockDeliveredBidTraceProvider.AssertExpectations(t)
}

func TestCatchupProviderSlot_BeginTxError(t *testing.T) {
	ctx := t.Context()
	provider := "test-relay"
	providerIndex := 0
	slot := phase0.Slot(100)

	mockReceivedBidsSetter := comptrollerdbMocks.NewMockReceivedBidsSetter(t)
	mockReceivedBidsSetter.EXPECT().BeginTx(mock.Anything).Return(ctx, nil, errors.New("tx error"))

	md := &metadata{
		LatestSlots: make(map[string]int64),
	}

	s := &Service{
		receivedBidsSetter: mockReceivedBidsSetter,
	}

	err := s.catchupProviderSlot(ctx, md, provider, providerIndex, slot)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to begin transaction")
}

func TestCatchupProviderSlot_HandleSlotError(t *testing.T) {
	ctx := t.Context()
	provider := "test-relay"
	providerIndex := 0
	slot := phase0.Slot(100)

	mockReceivedBidsSetter := comptrollerdbMocks.NewMockReceivedBidsSetter(t)
	mockReceivedBidTraceProvider := clientMocks.NewMockReceivedBidTracesProvider(t)

	cancelCalled := false
	cancel := func() { cancelCalled = true }

	mockReceivedBidsSetter.EXPECT().BeginTx(mock.Anything).Return(ctx, cancel, nil)
	mockReceivedBidTraceProvider.EXPECT().ReceivedBidTraces(mock.Anything, slot).Return(nil, errors.New("provider error"))

	md := &metadata{
		LatestSlots: make(map[string]int64),
	}

	s := &Service{
		receivedBidTracesProviders: []relayclient.ReceivedBidTracesProvider{mockReceivedBidTraceProvider},
		receivedBidsSetter:         mockReceivedBidsSetter,
	}

	err := s.catchupProviderSlot(ctx, md, provider, providerIndex, slot)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to handle slot")
	assert.True(t, cancelCalled)
}

func TestCatchupProviderSlot_SetMetadataError(t *testing.T) {
	ctx := t.Context()
	provider := "test-relay"
	providerIndex := 0
	slot := phase0.Slot(100)

	mockReceivedBidsSetter := comptrollerdbMocks.NewMockReceivedBidsSetter(t)
	mockReceivedBidTraceProvider := clientMocks.NewMockReceivedBidTracesProvider(t)
	mockDeliveredBidTraceProvider := clientMocks.NewMockDeliveredBidTraceProvider(t)

	cancelCalled := false
	cancel := func() { cancelCalled = true }

	mockReceivedBidsSetter.EXPECT().BeginTx(mock.Anything).Return(ctx, cancel, nil)
	mockReceivedBidTraceProvider.EXPECT().ReceivedBidTraces(mock.Anything, slot).Return([]*v1.BidTraceWithTimestamp{}, nil)
	mockReceivedBidsSetter.EXPECT().SetReceivedBids(mock.Anything, mock.Anything).Return(nil)
	mockDeliveredBidTraceProvider.EXPECT().DeliveredBidTrace(mock.Anything, slot).Return(nil, nil)
	mockReceivedBidsSetter.EXPECT().SetMetadata(mock.Anything, metadataKey, mock.Anything).Return(errors.New("metadata error"))

	md := &metadata{
		LatestSlots: make(map[string]int64),
	}

	s := &Service{
		receivedBidTracesProviders: []relayclient.ReceivedBidTracesProvider{mockReceivedBidTraceProvider},
		deliveredBidTraceProviders: []relayclient.DeliveredBidTraceProvider{mockDeliveredBidTraceProvider},
		receivedBidsSetter:         mockReceivedBidsSetter,
	}

	err := s.catchupProviderSlot(ctx, md, provider, providerIndex, slot)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to set metadata")
	assert.True(t, cancelCalled)
}

func TestCatchupProviderSlot_CommitTxError(t *testing.T) {
	ctx := t.Context()
	provider := "test-relay"
	providerIndex := 0
	slot := phase0.Slot(100)

	mockReceivedBidsSetter := comptrollerdbMocks.NewMockReceivedBidsSetter(t)
	mockReceivedBidTraceProvider := clientMocks.NewMockReceivedBidTracesProvider(t)
	mockDeliveredBidTraceProvider := clientMocks.NewMockDeliveredBidTraceProvider(t)

	cancelCalled := false
	cancel := func() { cancelCalled = true }

	mockReceivedBidsSetter.EXPECT().BeginTx(mock.Anything).Return(ctx, cancel, nil)
	mockReceivedBidTraceProvider.EXPECT().ReceivedBidTraces(mock.Anything, slot).Return([]*v1.BidTraceWithTimestamp{}, nil)
	mockReceivedBidsSetter.EXPECT().SetReceivedBids(mock.Anything, mock.Anything).Return(nil)
	mockDeliveredBidTraceProvider.EXPECT().DeliveredBidTrace(mock.Anything, slot).Return(nil, nil)
	mockReceivedBidsSetter.EXPECT().SetMetadata(mock.Anything, metadataKey, mock.Anything).Return(nil)
	mockReceivedBidsSetter.EXPECT().CommitTx(mock.Anything).Return(errors.New("commit error"))

	md := &metadata{
		LatestSlots: make(map[string]int64),
	}

	s := &Service{
		receivedBidTracesProviders: []relayclient.ReceivedBidTracesProvider{mockReceivedBidTraceProvider},
		deliveredBidTraceProviders: []relayclient.DeliveredBidTraceProvider{mockDeliveredBidTraceProvider},
		receivedBidsSetter:         mockReceivedBidsSetter,
	}

	err := s.catchupProviderSlot(ctx, md, provider, providerIndex, slot)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to commit transaction")
	assert.True(t, cancelCalled)
}

func TestCatchupProviderSlot_WithHandlers(t *testing.T) {
	ctx := t.Context()
	provider := "test-relay"
	providerIndex := 0
	slot := phase0.Slot(100)

	mockReceivedBidsSetter := comptrollerdbMocks.NewMockReceivedBidsSetter(t)
	mockReceivedBidTraceProvider := clientMocks.NewMockReceivedBidTracesProvider(t)
	mockDeliveredBidTraceProvider := clientMocks.NewMockDeliveredBidTraceProvider(t)
	mockHandler := bidsMocks.NewMockReceivedHandler(t)

	mockReceivedBidsSetter.EXPECT().BeginTx(mock.Anything).Return(ctx, func() {}, nil)
	mockReceivedBidTraceProvider.EXPECT().ReceivedBidTraces(mock.Anything, slot).Return([]*v1.BidTraceWithTimestamp{}, nil)
	mockReceivedBidsSetter.EXPECT().SetReceivedBids(mock.Anything, mock.Anything).Return(nil)
	mockDeliveredBidTraceProvider.EXPECT().DeliveredBidTrace(mock.Anything, slot).Return(nil, nil)
	mockReceivedBidsSetter.EXPECT().SetMetadata(mock.Anything, metadataKey, mock.Anything).Return(nil)
	mockReceivedBidsSetter.EXPECT().CommitTx(mock.Anything).Return(nil)
	mockHandler.EXPECT().BidsReceived(mock.Anything, slot).Maybe()

	md := &metadata{
		LatestSlots: map[string]int64{provider: int64(slot)},
	}

	s := &Service{
		receivedBidTracesProviders: []relayclient.ReceivedBidTracesProvider{mockReceivedBidTraceProvider},
		deliveredBidTraceProviders: []relayclient.DeliveredBidTraceProvider{mockDeliveredBidTraceProvider},
		receivedBidsSetter:         mockReceivedBidsSetter,
		bidsReceivedHandlers:       []bids.ReceivedHandler{mockHandler},
	}

	err := s.catchupProviderSlot(ctx, md, provider, providerIndex, slot)
	require.NoError(t, err)
	mockHandler.AssertExpectations(t)
}

func TestCatchupProvider_Success(t *testing.T) {
	ctx := t.Context()
	provider := "test-relay"
	providerIndex := 0

	const targetSlot = 103

	mockChainTime := chaintimeMocks.NewMockService(t)
	mockChainTime.EXPECT().CurrentSlot().Return(phase0.Slot(targetSlot))

	mockReceivedBidsSetter := comptrollerdbMocks.NewMockReceivedBidsSetter(t)
	mockReceivedBidTraceProvider := clientMocks.NewMockReceivedBidTracesProvider(t)
	mockDeliveredBidTraceProvider := clientMocks.NewMockDeliveredBidTraceProvider(t)

	for slot := phase0.Slot(1); slot < targetSlot; slot++ {
		mockReceivedBidsSetter.EXPECT().BeginTx(mock.Anything).Return(ctx, func() {}, nil)
		mockReceivedBidTraceProvider.EXPECT().ReceivedBidTraces(mock.Anything, slot).Return([]*v1.BidTraceWithTimestamp{}, nil)
		mockReceivedBidsSetter.EXPECT().SetReceivedBids(mock.Anything, mock.Anything).Return(nil)
		mockDeliveredBidTraceProvider.EXPECT().DeliveredBidTrace(mock.Anything, slot).Return(nil, nil)
		mockReceivedBidsSetter.EXPECT().SetMetadata(mock.Anything, metadataKey, mock.Anything).Return(nil)
		mockReceivedBidsSetter.EXPECT().CommitTx(mock.Anything).Return(nil)
	}

	md := &metadata{
		LatestSlots: map[string]int64{provider: 0},
	}

	s := &Service{
		chainTime:                  mockChainTime,
		receivedBidTracesProviders: []relayclient.ReceivedBidTracesProvider{mockReceivedBidTraceProvider},
		deliveredBidTraceProviders: []relayclient.DeliveredBidTraceProvider{mockDeliveredBidTraceProvider},
		receivedBidsSetter:         mockReceivedBidsSetter,
		bidsReceivedHandlers:       []bids.ReceivedHandler{},
	}

	var wg sync.WaitGroup
	wg.Add(1)
	s.catchupProvider(ctx, &wg, md, provider, providerIndex)
	assert.Equal(t, int64(targetSlot-1), md.LatestSlots[provider])
}

func TestCatchupProvider_NoSlotsToProcess(t *testing.T) {
	ctx := t.Context()
	provider := "test-relay"
	providerIndex := 0

	mockChainTime := chaintimeMocks.NewMockService(t)
	mockChainTime.EXPECT().CurrentSlot().Return(phase0.Slot(100))

	md := &metadata{
		LatestSlots: map[string]int64{provider: 99},
	}

	s := &Service{
		chainTime:            mockChainTime,
		bidsReceivedHandlers: []bids.ReceivedHandler{},
	}

	var wg sync.WaitGroup
	wg.Add(1)
	s.catchupProvider(ctx, &wg, md, provider, providerIndex)
	assert.Equal(t, int64(99), md.LatestSlots[provider])
}

func TestCatchup_Success(t *testing.T) {
	ctx := t.Context()
	provider1 := "relay-1"
	provider2 := "relay-2"

	mockChainTime := chaintimeMocks.NewMockService(t)
	mockChainTime.EXPECT().CurrentSlot().Return(phase0.Slot(100)).Maybe()

	mockReceivedBidsSetter := comptrollerdbMocks.NewMockReceivedBidsSetter(t)
	mockProvider1 := clientMocks.NewMockReceivedBidTracesProvider(t)
	mockProvider2 := clientMocks.NewMockReceivedBidTracesProvider(t)
	mockDeliveredBidTraceProvider1 := clientMocks.NewMockDeliveredBidTraceProvider(t)
	mockDeliveredBidTraceProvider2 := clientMocks.NewMockDeliveredBidTraceProvider(t)

	mockProvider1.EXPECT().Name().Return(provider1)
	mockProvider2.EXPECT().Name().Return(provider2)

	mockReceivedBidsSetter.EXPECT().BeginTx(mock.Anything).Return(ctx, func() {}, nil).Maybe()
	mockProvider1.EXPECT().ReceivedBidTraces(mock.Anything, mock.Anything).Return([]*v1.BidTraceWithTimestamp{}, nil).Maybe()
	mockProvider2.EXPECT().ReceivedBidTraces(mock.Anything, mock.Anything).Return([]*v1.BidTraceWithTimestamp{}, nil).Maybe()
	mockReceivedBidsSetter.EXPECT().SetReceivedBids(mock.Anything, mock.Anything).Return(nil).Maybe()
	mockDeliveredBidTraceProvider1.EXPECT().DeliveredBidTrace(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	mockDeliveredBidTraceProvider2.EXPECT().DeliveredBidTrace(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	mockReceivedBidsSetter.EXPECT().SetMetadata(mock.Anything, metadataKey, mock.Anything).Return(nil).Maybe()
	mockReceivedBidsSetter.EXPECT().CommitTx(mock.Anything).Return(nil).Maybe()

	md := &metadata{
		LatestSlots: map[string]int64{provider1: 0, provider2: 0},
		mu:          sync.RWMutex{},
	}

	s := &Service{
		chainTime:                  mockChainTime,
		receivedBidTracesProviders: []relayclient.ReceivedBidTracesProvider{mockProvider1, mockProvider2},
		deliveredBidTraceProviders: []relayclient.DeliveredBidTraceProvider{mockDeliveredBidTraceProvider1, mockDeliveredBidTraceProvider2},
		receivedBidsSetter:         mockReceivedBidsSetter,
		bidsReceivedHandlers:       []bids.ReceivedHandler{},
	}

	s.catchup(ctx, md)
	mockProvider1.AssertExpectations(t)
	mockProvider2.AssertExpectations(t)
}

func TestCatchup_SingleProvider(t *testing.T) {
	ctx := t.Context()
	provider := "relay-1"

	mockChainTime := chaintimeMocks.NewMockService(t)
	mockChainTime.EXPECT().CurrentSlot().Return(phase0.Slot(50)).Maybe()

	mockReceivedBidsSetter := comptrollerdbMocks.NewMockReceivedBidsSetter(t)
	mockProvider := clientMocks.NewMockReceivedBidTracesProvider(t)
	mockDeliveredBidTraceProvider := clientMocks.NewMockDeliveredBidTraceProvider(t)

	mockProvider.EXPECT().Name().Return(provider)
	mockReceivedBidsSetter.EXPECT().BeginTx(mock.Anything).Return(ctx, func() {}, nil).Maybe()
	mockProvider.EXPECT().ReceivedBidTraces(mock.Anything, mock.Anything).Return([]*v1.BidTraceWithTimestamp{}, nil).Maybe()
	mockReceivedBidsSetter.EXPECT().SetReceivedBids(mock.Anything, mock.Anything).Return(nil).Maybe()
	mockDeliveredBidTraceProvider.EXPECT().DeliveredBidTrace(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	mockReceivedBidsSetter.EXPECT().SetMetadata(mock.Anything, metadataKey, mock.Anything).Return(nil).Maybe()
	mockReceivedBidsSetter.EXPECT().CommitTx(mock.Anything).Return(nil).Maybe()

	md := &metadata{
		LatestSlots: map[string]int64{provider: 0},
		mu:          sync.RWMutex{},
	}

	s := &Service{
		chainTime:                  mockChainTime,
		receivedBidTracesProviders: []relayclient.ReceivedBidTracesProvider{mockProvider},
		deliveredBidTraceProviders: []relayclient.DeliveredBidTraceProvider{mockDeliveredBidTraceProvider},
		receivedBidsSetter:         mockReceivedBidsSetter,
		bidsReceivedHandlers:       []bids.ReceivedHandler{},
	}

	s.catchup(ctx, md)
	mockProvider.AssertExpectations(t)
}

func TestCatchup_NoProviders(t *testing.T) {
	ctx := t.Context()

	md := &metadata{
		LatestSlots: make(map[string]int64),
		mu:          sync.RWMutex{},
	}

	s := &Service{
		receivedBidTracesProviders: []relayclient.ReceivedBidTracesProvider{},
		bidsReceivedHandlers:       []bids.ReceivedHandler{},
	}

	s.catchup(ctx, md)
}

func TestCatchup_MultipleProvidersParallel(t *testing.T) {
	ctx := t.Context()
	provider1 := "relay-1"
	provider2 := "relay-2"
	provider3 := "relay-3"

	mockChainTime := chaintimeMocks.NewMockService(t)
	mockChainTime.EXPECT().CurrentSlot().Return(phase0.Slot(100)).Maybe()

	mockReceivedBidsSetter := comptrollerdbMocks.NewMockReceivedBidsSetter(t)
	mockProvider1 := clientMocks.NewMockReceivedBidTracesProvider(t)
	mockProvider2 := clientMocks.NewMockReceivedBidTracesProvider(t)
	mockProvider3 := clientMocks.NewMockReceivedBidTracesProvider(t)
	mockDeliveredBidTraceProvider1 := clientMocks.NewMockDeliveredBidTraceProvider(t)
	mockDeliveredBidTraceProvider2 := clientMocks.NewMockDeliveredBidTraceProvider(t)
	mockDeliveredBidTraceProvider3 := clientMocks.NewMockDeliveredBidTraceProvider(t)

	mockProvider1.EXPECT().Name().Return(provider1)
	mockProvider2.EXPECT().Name().Return(provider2)
	mockProvider3.EXPECT().Name().Return(provider3)

	mockReceivedBidsSetter.EXPECT().BeginTx(mock.Anything).Return(ctx, func() {}, nil).Maybe()
	mockProvider1.EXPECT().ReceivedBidTraces(mock.Anything, mock.Anything).Return([]*v1.BidTraceWithTimestamp{}, nil).Maybe()
	mockProvider2.EXPECT().ReceivedBidTraces(mock.Anything, mock.Anything).Return([]*v1.BidTraceWithTimestamp{}, nil).Maybe()
	mockProvider3.EXPECT().ReceivedBidTraces(mock.Anything, mock.Anything).Return([]*v1.BidTraceWithTimestamp{}, nil).Maybe()
	mockReceivedBidsSetter.EXPECT().SetReceivedBids(mock.Anything, mock.Anything).Return(nil).Maybe()
	mockDeliveredBidTraceProvider1.EXPECT().DeliveredBidTrace(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	mockDeliveredBidTraceProvider2.EXPECT().DeliveredBidTrace(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	mockDeliveredBidTraceProvider3.EXPECT().DeliveredBidTrace(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	mockReceivedBidsSetter.EXPECT().SetMetadata(mock.Anything, metadataKey, mock.Anything).Return(nil).Maybe()
	mockReceivedBidsSetter.EXPECT().CommitTx(mock.Anything).Return(nil).Maybe()

	md := &metadata{
		LatestSlots: map[string]int64{provider1: 0, provider2: 0, provider3: 0},
		mu:          sync.RWMutex{},
	}

	s := &Service{
		chainTime:                  mockChainTime,
		receivedBidTracesProviders: []relayclient.ReceivedBidTracesProvider{mockProvider1, mockProvider2, mockProvider3},
		deliveredBidTraceProviders: []relayclient.DeliveredBidTraceProvider{mockDeliveredBidTraceProvider1, mockDeliveredBidTraceProvider2, mockDeliveredBidTraceProvider3},
		receivedBidsSetter:         mockReceivedBidsSetter,
		bidsReceivedHandlers:       []bids.ReceivedHandler{},
	}

	s.catchup(ctx, md)
	mockProvider1.AssertExpectations(t)
	mockProvider2.AssertExpectations(t)
	mockProvider3.AssertExpectations(t)
}
