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
	"encoding/json"
	"fmt"
	"testing"

	relayclient "github.com/attestantio/go-relay-client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	clientMocks "github.com/wealdtech/comptrollerd/mocks/client"
	comptrollerdbMocks "github.com/wealdtech/comptrollerd/mocks/comptrollerdb"
)

func TestGetMetadataNotFound(t *testing.T) {
	mockSetter := comptrollerdbMocks.NewMockReceivedBidsSetter(t)
	mockSetter.EXPECT().Metadata(mock.Anything, metadataKey).Return(nil, nil).Once()

	s := &Service{
		receivedBidsSetter: mockSetter,
	}

	md, err := s.getMetadata(t.Context())
	require.NoError(t, err)
	assert.NotNil(t, md)
	assert.Empty(t, md.LatestSlots)
	assert.Equal(t, int64(0), md.LatestSlot)
}

func TestGetMetadataMetadataError(t *testing.T) {
	mockSetter := comptrollerdbMocks.NewMockReceivedBidsSetter(t)
	mockSetter.EXPECT().Metadata(mock.Anything, metadataKey).Return(nil, assert.AnError).Once()

	s := &Service{
		receivedBidsSetter: mockSetter,
	}

	md, err := s.getMetadata(context.Background())
	require.Error(t, err)
	assert.Nil(t, md)
}

func TestGetMetadataUnmarshalError(t *testing.T) {
	mockSetter := comptrollerdbMocks.NewMockReceivedBidsSetter(t)
	mockSetter.EXPECT().Metadata(mock.Anything, metadataKey).Return([]byte("invalid json"), nil).Once()

	s := &Service{
		receivedBidsSetter: mockSetter,
	}

	md, err := s.getMetadata(context.Background())
	require.Error(t, err)
	assert.Nil(t, md)
}

func TestGetMetadata_Success(t *testing.T) {
	mdData := &metadata{
		LatestSlots: map[string]int64{"provider1": int64(100)},
		LatestSlot:  0,
	}
	mdJSON, _ := json.Marshal(mdData)

	mockSetter := comptrollerdbMocks.NewMockReceivedBidsSetter(t)
	mockSetter.EXPECT().Metadata(mock.Anything, metadataKey).Return(mdJSON, nil).Once()

	s := &Service{
		receivedBidsSetter: mockSetter,
	}

	md, err := s.getMetadata(context.Background())
	require.NoError(t, err)
	assert.NotNil(t, md)
	assert.Equal(t, int64(100), md.LatestSlots["provider1"])
}

func TestGetMetadata_Upgrade(t *testing.T) {
	const numTestProviders = 2

	mdData := &metadata{
		LatestSlots: map[string]int64{},
		LatestSlot:  50,
	}
	mdJSON, _ := json.Marshal(mdData)

	mockSetter := comptrollerdbMocks.NewMockReceivedBidsSetter(t)
	mockSetter.On("Metadata", mock.Anything, metadataKey).Return(mdJSON, nil)

	providers := make([]relayclient.ReceivedBidTracesProvider, 0, numTestProviders)
	for i := range numTestProviders {
		provider := clientMocks.NewMockReceivedBidTracesProvider(t)
		provider.EXPECT().Name().Return(fmt.Sprintf("relay_%d", i)).Maybe()
		providers = append(providers, provider)
	}

	s := &Service{
		receivedBidsSetter:         mockSetter,
		receivedBidTracesProviders: providers,
	}

	md, err := s.getMetadata(context.Background())
	require.NoError(t, err)
	assert.NotNil(t, md)
	for i := range numTestProviders {
		assert.Equal(t, int64(50), md.LatestSlots[fmt.Sprintf("relay_%d", i)])
	}
	assert.Equal(t, int64(0), md.LatestSlot)
}

func TestSetMetadataSuccess(t *testing.T) {
	mdData := &metadata{
		LatestSlots: map[string]int64{"provider1": int64(100)},
		LatestSlot:  0,
	}
	mdJSON, _ := json.Marshal(mdData)

	mockSetter := comptrollerdbMocks.NewMockReceivedBidsSetter(t)
	mockSetter.EXPECT().SetMetadata(mock.Anything, metadataKey, mdJSON).Return(nil).Once()

	s := &Service{
		receivedBidsSetter: mockSetter,
	}

	err := s.setMetadata(context.Background(), mdData)
	require.NoError(t, err)
}
func TestSetMetadataSetMetadataError(t *testing.T) {
	mdData := &metadata{
		LatestSlots: map[string]int64{"provider1": int64(100)},
		LatestSlot:  0,
	}
	mdJSON, _ := json.Marshal(mdData)

	mockSetter := comptrollerdbMocks.NewMockReceivedBidsSetter(t)
	mockSetter.EXPECT().SetMetadata(mock.Anything, metadataKey, mdJSON).Return(assert.AnError).Once()

	s := &Service{
		receivedBidsSetter: mockSetter,
	}

	err := s.setMetadata(context.Background(), mdData)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to update metadata")
}
