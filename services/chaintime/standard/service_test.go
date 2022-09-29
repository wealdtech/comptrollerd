// Copyright © 2020, 2021 Weald Technology Trading.
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

package standard_test

import (
	"context"
	"testing"
	"time"

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"github.com/wealdtech/chaind/services/chaintime"
	"github.com/wealdtech/chaind/services/chaintime/standard"
	"github.com/wealdtech/chaind/testing/mock"
)

func TestService(t *testing.T) {
	genesisTime := time.Now()
	slotDuration := 12 * time.Second
	slotsPerEpoch := uint64(32)
	epochsPerSyncCommitteePeriod := uint64(256)
	forkSchedule := []*phase0.Fork{
		{
			PreviousVersion: phase0.Version{0x01, 0x02, 0x03, 0x04},
			CurrentVersion:  phase0.Version{0x01, 0x02, 0x03, 0x04},
			Epoch:           0,
		},
		{
			PreviousVersion: phase0.Version{0x01, 0x02, 0x03, 0x04},
			CurrentVersion:  phase0.Version{0x05, 0x06, 0x07, 0x08},
			Epoch:           10,
		},
	}

	mockGenesisTimeProvider := mock.NewGenesisTimeProvider(genesisTime)
	mockSpecProvider := mock.NewSpecProvider(slotDuration, slotsPerEpoch, epochsPerSyncCommitteePeriod)
	mockForkScheduleProvider := mock.NewForkScheduleProvider(forkSchedule)

	tests := []struct {
		name   string
		params []standard.Parameter
		err    string
	}{
		{
			name: "GenesisTimeProviderMissing",
			params: []standard.Parameter{
				standard.WithLogLevel(zerolog.Disabled),
				standard.WithSpecProvider(mockSpecProvider),
				standard.WithForkScheduleProvider(mockForkScheduleProvider),
			},
			err: "problem with parameters: no genesis time provider specified",
		},
		{
			name: "SpecProviderMissing",
			params: []standard.Parameter{
				standard.WithLogLevel(zerolog.Disabled),
				standard.WithGenesisTimeProvider(mockGenesisTimeProvider),
				standard.WithForkScheduleProvider(mockForkScheduleProvider),
			},
			err: "problem with parameters: no spec provider specified",
		},
		{
			name: "ForkScheduleProviderMissing",
			params: []standard.Parameter{
				standard.WithLogLevel(zerolog.Disabled),
				standard.WithGenesisTimeProvider(mockGenesisTimeProvider),
				standard.WithSpecProvider(mockSpecProvider),
			},
			err: "problem with parameters: no fork schedule provider specified",
		},
		{
			name: "Good",
			params: []standard.Parameter{
				standard.WithLogLevel(zerolog.Disabled),
				standard.WithGenesisTimeProvider(mockGenesisTimeProvider),
				standard.WithSpecProvider(mockSpecProvider),
				standard.WithForkScheduleProvider(mockForkScheduleProvider),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := standard.New(context.Background(), test.params...)
			if test.err != "" {
				require.EqualError(t, err, test.err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// createService is a helper that creates a mock chaintime service.
func createService(genesisTime time.Time) (chaintime.Service, time.Duration, uint64, uint64, []*phase0.Fork, error) {
	slotDuration := 12 * time.Second
	slotsPerEpoch := uint64(32)
	epochsPerSyncCommitteePeriod := uint64(256)
	forkSchedule := []*phase0.Fork{
		{
			PreviousVersion: phase0.Version{0x01, 0x02, 0x03, 0x04},
			CurrentVersion:  phase0.Version{0x01, 0x02, 0x03, 0x04},
			Epoch:           0,
		},
		{
			PreviousVersion: phase0.Version{0x01, 0x02, 0x03, 0x04},
			CurrentVersion:  phase0.Version{0x05, 0x06, 0x07, 0x08},
			Epoch:           10,
		},
	}

	mockGenesisTimeProvider := mock.NewGenesisTimeProvider(genesisTime)
	mockSpecProvider := mock.NewSpecProvider(slotDuration, slotsPerEpoch, epochsPerSyncCommitteePeriod)
	mockForkScheduleProvider := mock.NewForkScheduleProvider(forkSchedule)
	s, err := standard.New(context.Background(),
		standard.WithGenesisTimeProvider(mockGenesisTimeProvider),
		standard.WithSpecProvider(mockSpecProvider),
		standard.WithForkScheduleProvider(mockForkScheduleProvider),
	)
	return s, slotDuration, slotsPerEpoch, epochsPerSyncCommitteePeriod, forkSchedule, err
}

func TestGenesisTime(t *testing.T) {
	genesisTime := time.Now()
	s, _, _, _, _, err := createService(genesisTime)
	require.NoError(t, err)

	require.Equal(t, genesisTime, s.GenesisTime())
}

func TestStartOfSlot(t *testing.T) {
	genesisTime := time.Now()
	s, slotDuration, _, _, _, err := createService(genesisTime)
	require.NoError(t, err)

	require.Equal(t, genesisTime, s.StartOfSlot(0))
	require.Equal(t, genesisTime.Add(1000*slotDuration), s.StartOfSlot(1000))
}

func TestStartOfEpoch(t *testing.T) {
	genesisTime := time.Now()
	s, slotDuration, slotsPerEpoch, _, _, err := createService(genesisTime)
	require.NoError(t, err)

	require.Equal(t, genesisTime, s.StartOfEpoch(0))
	require.Equal(t, genesisTime.Add(time.Duration(1000*slotsPerEpoch)*slotDuration), s.StartOfEpoch(1000))
}

func TestCurrentSlot(t *testing.T) {
	genesisTime := time.Now().Add(-60 * time.Second)
	s, _, _, _, _, err := createService(genesisTime)
	require.NoError(t, err)

	require.Equal(t, phase0.Slot(5), s.CurrentSlot())
}

func TestCurrentEpoch(t *testing.T) {
	genesisTime := time.Now().Add(-1000 * time.Second)
	s, _, _, _, _, err := createService(genesisTime)
	require.NoError(t, err)

	require.Equal(t, phase0.Epoch(2), s.CurrentEpoch())
}

func TestTimestampToSlot(t *testing.T) {
	genesisTime := time.Now()
	s, _, _, _, _, err := createService(genesisTime)
	require.NoError(t, err)

	tests := []struct {
		name      string
		timestamp time.Time
		slot      phase0.Slot
	}{
		{
			name:      "PreGenesis",
			timestamp: genesisTime.AddDate(0, 0, -1),
			slot:      0,
		},
		{
			name:      "Genesis",
			timestamp: genesisTime,
			slot:      0,
		},
		{
			name:      "Slot1",
			timestamp: genesisTime.Add(12 * time.Second),
			slot:      1,
		},
		{
			name:      "Slot999",
			timestamp: genesisTime.Add(999 * 12 * time.Second),
			slot:      999,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.slot, s.TimestampToSlot(test.timestamp))
		})
	}
}

func TestTimestampToEpoch(t *testing.T) {
	genesisTime := time.Now()
	s, _, _, _, _, err := createService(genesisTime)
	require.NoError(t, err)

	tests := []struct {
		name      string
		timestamp time.Time
		epoch     phase0.Epoch
	}{
		{
			name:      "PreGenesis",
			timestamp: genesisTime.AddDate(0, 0, -1),
			epoch:     0,
		},
		{
			name:      "Genesis",
			timestamp: genesisTime,
			epoch:     0,
		},
		{
			name:      "Epoch1",
			timestamp: genesisTime.Add(32 * 12 * time.Second),
			epoch:     1,
		},
		{
			name:      "Epoch1Boundary",
			timestamp: genesisTime.Add(64 * 12 * time.Second).Add(-1 * time.Millisecond),
			epoch:     1,
		},
		{
			name:      "Epoch999",
			timestamp: genesisTime.Add(999 * 32 * 12 * time.Second),
			epoch:     999,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.epoch, s.TimestampToEpoch(test.timestamp))
		})
	}
}
