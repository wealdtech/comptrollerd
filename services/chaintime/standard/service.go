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

package standard

import (
	"bytes"
	"context"
	"time"

	eth2client "github.com/attestantio/go-eth2-client"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	zerologger "github.com/rs/zerolog/log"
)

// Service provides chain time services.
type Service struct {
	genesisTime        time.Time
	slotDuration       time.Duration
	slotsPerEpoch      uint64
	altairForkEpoch    phase0.Epoch
	bellatrixForkEpoch phase0.Epoch
}

// module-wide log.
var log zerolog.Logger

// New creates a new chain time service.
func New(ctx context.Context, params ...Parameter) (*Service, error) {
	parameters, err := parseAndCheckParameters(params...)
	if err != nil {
		return nil, errors.Wrap(err, "problem with parameters")
	}

	// Set logging.
	log = zerologger.With().Str("service", "chaintime").Str("impl", "standard").Logger().Level(parameters.logLevel)

	genesisTime, err := parameters.genesisTimeProvider.GenesisTime(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to obtain genesis time")
	}
	log.Trace().Time("genesis_time", genesisTime).Msg("Obtained genesis time")

	spec, err := parameters.specProvider.Spec(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to obtain spec")
	}

	tmp, exists := spec["SECONDS_PER_SLOT"]
	if !exists {
		return nil, errors.New("SECONDS_PER_SLOT not found in spec")
	}
	slotDuration, ok := tmp.(time.Duration)
	if !ok {
		return nil, errors.New("SECONDS_PER_SLOT of unexpected type")
	}

	tmp, exists = spec["SLOTS_PER_EPOCH"]
	if !exists {
		return nil, errors.New("SLOTS_PER_EPOCH not found in spec")
	}
	slotsPerEpoch, ok := tmp.(uint64)
	if !ok {
		return nil, errors.New("SLOTS_PER_EPOCH of unexpected type")
	}

	altairForkEpoch, err := fetchAltairForkEpoch(ctx, parameters.forkScheduleProvider)
	if err != nil {
		// Set to far future epoch.
		altairForkEpoch = 0xffffffffffffffff
	}
	log.Trace().Uint64("epoch", uint64(altairForkEpoch)).Msg("Obtained Altair fork epoch")
	bellatrixForkEpoch, err := fetchBellatrixForkEpoch(ctx, parameters.forkScheduleProvider)
	if err != nil {
		// Set to far future epoch.
		bellatrixForkEpoch = 0xffffffffffffffff
	}
	log.Trace().Uint64("epoch", uint64(bellatrixForkEpoch)).Msg("Obtained Bellatrix fork epoch")

	s := &Service{
		genesisTime:        genesisTime,
		slotDuration:       slotDuration,
		slotsPerEpoch:      slotsPerEpoch,
		altairForkEpoch:    altairForkEpoch,
		bellatrixForkEpoch: bellatrixForkEpoch,
	}

	return s, nil
}

// GenesisTime provides the time of the chain's genesis.
func (s *Service) GenesisTime() time.Time {
	return s.genesisTime
}

// StartOfSlot provides the time at which the given slot starts.
func (s *Service) StartOfSlot(slot phase0.Slot) time.Time {
	return s.genesisTime.Add(time.Duration(slot) * s.slotDuration)
}

// StartOfEpoch provides the time at which the given epoch starts.
func (s *Service) StartOfEpoch(epoch phase0.Epoch) time.Time {
	return s.genesisTime.Add(time.Duration(uint64(epoch)*s.slotsPerEpoch) * s.slotDuration)
}

// CurrentSlot provides the current slot.
func (s *Service) CurrentSlot() phase0.Slot {
	if s.genesisTime.After(time.Now()) {
		return 0
	}
	return phase0.Slot(uint64(time.Since(s.genesisTime).Seconds()) / uint64(s.slotDuration.Seconds()))
}

// CurrentEpoch provides the current epoch.
func (s *Service) CurrentEpoch() phase0.Epoch {
	return phase0.Epoch(uint64(s.CurrentSlot()) / s.slotsPerEpoch)
}

// SlotToEpoch provides the epoch of a given slot.
func (s *Service) SlotToEpoch(slot phase0.Slot) phase0.Epoch {
	return phase0.Epoch(uint64(slot) / s.slotsPerEpoch)
}

// FirstSlotOfEpoch provides the first slot of the given epoch.
func (s *Service) FirstSlotOfEpoch(epoch phase0.Epoch) phase0.Slot {
	return phase0.Slot(uint64(epoch) * s.slotsPerEpoch)
}

// SlotOfTimestamp provides the slot of the given timestamp.
func (s *Service) SlotOfTimestamp(timestamp time.Time) phase0.Slot {
	if timestamp.Before(s.genesisTime) {
		return 0
	}
	secondsSinceGenesis := uint64(timestamp.Sub(s.genesisTime).Seconds())
	return phase0.Slot(secondsSinceGenesis / uint64(s.slotDuration.Seconds()))
}

// EpochOfTimestamp provides the epoch of the given timestamp.
func (s *Service) EpochOfTimestamp(timestamp time.Time) phase0.Epoch {
	if timestamp.Before(s.genesisTime) {
		return 0
	}
	secondsSinceGenesis := uint64(timestamp.Sub(s.genesisTime).Seconds())
	return phase0.Epoch(secondsSinceGenesis / uint64(s.slotDuration.Seconds()) / s.slotsPerEpoch)
}

// AltairInitialEpoch provides the epoch at which the Altair hard fork takes place.
func (s *Service) AltairInitialEpoch() phase0.Epoch {
	return s.altairForkEpoch
}

// BellatrixInitialEpoch provides the epoch at which the Bellatrix hard fork takes place.
func (s *Service) BellatrixInitialEpoch() phase0.Epoch {
	return s.bellatrixForkEpoch
}

func fetchAltairForkEpoch(ctx context.Context, provider eth2client.ForkScheduleProvider) (phase0.Epoch, error) {
	forkSchedule, err := provider.ForkSchedule(ctx)
	if err != nil {
		return 0, err
	}
	forkVersion := 0
	for i := range forkSchedule {
		if bytes.Equal(forkSchedule[i].CurrentVersion[:], forkSchedule[i].PreviousVersion[:]) {
			// This is the genesis fork; ignore it.
			continue
		}
		forkVersion++
		if forkVersion == 1 {
			return forkSchedule[i].Epoch, nil
		}
	}
	return 0, errors.New("no altair fork obtained")
}

func fetchBellatrixForkEpoch(ctx context.Context, provider eth2client.ForkScheduleProvider) (phase0.Epoch, error) {
	forkSchedule, err := provider.ForkSchedule(ctx)
	if err != nil {
		return 0, err
	}
	forkVersion := 0
	for i := range forkSchedule {
		if bytes.Equal(forkSchedule[i].CurrentVersion[:], forkSchedule[i].PreviousVersion[:]) {
			// This is the genesis fork; ignore it.
			continue
		}
		forkVersion++
		if forkVersion == 2 {
			return forkSchedule[i].Epoch, nil
		}
	}
	return 0, errors.New("no bellatrix fork obtained")
}
