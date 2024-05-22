// Copyright © 2022, 2023 Weald Technology Trading.
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

package main

import (
	"context"
	"fmt"
	"time"

	// #nosec G108
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"strings"
	"syscall"

	consensusclient "github.com/attestantio/go-eth2-client"
	relayclient "github.com/attestantio/go-relay-client"
	homedir "github.com/mitchellh/go-homedir"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/wealdtech/comptrollerd/services/bids"
	standardbids "github.com/wealdtech/comptrollerd/services/bids/standard"
	standardblockpayments "github.com/wealdtech/comptrollerd/services/blockpayments/standard"
	"github.com/wealdtech/comptrollerd/services/chaintime"
	standardchaintime "github.com/wealdtech/comptrollerd/services/chaintime/standard"
	"github.com/wealdtech/comptrollerd/services/comptrollerdb"
	"github.com/wealdtech/comptrollerd/services/metrics"
	nullmetrics "github.com/wealdtech/comptrollerd/services/metrics/null"
	prometheusmetrics "github.com/wealdtech/comptrollerd/services/metrics/prometheus"
	standardrelays "github.com/wealdtech/comptrollerd/services/relays/standard"
	"github.com/wealdtech/comptrollerd/services/scheduler"
	standardscheduler "github.com/wealdtech/comptrollerd/services/scheduler/standard"
	"github.com/wealdtech/comptrollerd/util"
	"github.com/wealdtech/execd/services/execdb"
	majordomo "github.com/wealdtech/go-majordomo"
)

// ReleaseVersion is the release version for the code.
var ReleaseVersion = "0.2.11-dev"

func main() {
	os.Exit(main2())
}

func main2() int {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := fetchConfig(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to fetch configuration: %v\n", err)
		return 1
	}

	majordomoSvc, err := util.InitMajordomo(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to initialise majordomo: %v\n", err)
		return 1
	}

	if err := initLogging(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to initialise logging: %v\n", err)
		return 1
	}

	// runCommands will not return if a command is run.
	runCommands(ctx)

	logModules()
	log.Info().Str("version", ReleaseVersion).Msg("Starting comptrollerd")

	if err := initTracing(ctx, majordomoSvc); err != nil {
		log.Error().Err(err).Msg("Failed to initialise tracing")
		return 1
	}

	runtime.GOMAXPROCS(runtime.NumCPU() * 8)

	log.Trace().Msg("Starting metrics service")
	monitor, err := startMonitor(ctx)
	if err != nil {
		log.Error().Err(err).Msg("Failed to start metrics service")
		return 1
	}
	if err := registerMetrics(ctx, monitor); err != nil {
		log.Error().Err(err).Msg("Failed to register metrics")
		return 1
	}
	setRelease(ctx, ReleaseVersion)
	setReady(ctx, false)

	if err := startServices(ctx, monitor, majordomoSvc); err != nil {
		log.Error().Err(err).Msg("Failed to initialise services")
		return 1
	}
	setReady(ctx, true)

	log.Info().Msg("All services operational")

	// Wait for signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)
	for {
		sig := <-sigCh
		if sig == syscall.SIGINT || sig == syscall.SIGTERM || sig == os.Interrupt || sig == os.Kill {
			break
		}
	}

	log.Info().Msg("Stopping comptrollerd")

	return 0
}

// fetchConfig fetches configuration from various sources.
func fetchConfig() error {
	pflag.String("base-dir", "", "base directory for configuration files")
	pflag.Bool("version", false, "show version and exit")
	pflag.String("log-level", "info", "minimum level of messsages to log")
	pflag.String("log-file", "", "redirect log output to a file")
	pflag.String("profile-address", "", "Address on which to run Go profile server")
	pflag.String("tracing-address", "", "Address to which to send tracing data")
	pflag.Duration("relayclient.timeout", 60*time.Second, "Timeout for relay client requests")
	pflag.Int64("bids.start-slot", -1, "First slot for which to obtain or re-obtain bids")
	pflag.Int64("blockpayments.start-slot", -1, "First slot for which to calculate or re-calculate block payments")
	pflag.Int64("blockpayments.replay-slot", -1, "Replay block payments for a single slot and then exit")
	pflag.Uint("comptrollerdb.max-connections", 32, "maximum number of concurrent database connections")
	pflag.Parse()
	if err := viper.BindPFlags(pflag.CommandLine); err != nil {
		return errors.Wrap(err, "failed to bind pflags to viper")
	}

	if viper.GetString("base-dir") != "" {
		// User-defined base directory.
		viper.AddConfigPath(util.ResolvePath(""))
		viper.SetConfigName("comptrollerd")
	} else {
		// Home directory.
		home, err := homedir.Dir()
		if err != nil {
			return errors.Wrap(err, "failed to obtain home directory")
		}
		viper.AddConfigPath(home)
		viper.SetConfigName(".comptrollerd")
	}

	// Environment settings.
	viper.SetEnvPrefix("COMPTROLLERD")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_", ".", "_"))
	viper.AutomaticEnv()

	// Defaults.
	viper.SetDefault("timeout", 60*time.Second)
	viper.SetDefault("process-concurrency", int64(runtime.GOMAXPROCS(-1)))
	viper.SetDefault("relays.interval", 384*time.Second)
	viper.SetDefault("bids.interval", 12*time.Second)
	viper.SetDefault("blockpayments.track-distance", 96)

	if err := viper.ReadInConfig(); err != nil {
		switch {
		case errors.As(err, &viper.ConfigFileNotFoundError{}):
			// It is allowable for comptrollerd to not have a configuration file, but only if
			// we have the information from elsewhere (e.g. environment variables).  Check
			// to see if we have a comptroller server configured, as if not we aren't going to
			// get very far anyway.
			if viper.Get("version") == nil && viper.GetString("comptrollerdb.server") == "" {
				// Assume the underlying issue is that the configuration file is missing.
				return errors.Wrap(err, "could not find the configuration file")
			}
		case errors.As(err, &viper.ConfigParseError{}):
			return errors.Wrap(err, "could not parse the configuration file")
		default:
			return errors.Wrap(err, "failed to obtain configuration")
		}
	}

	return nil
}

func startMonitor(ctx context.Context) (metrics.Service, error) {
	var monitor metrics.Service
	if viper.Get("metrics.prometheus.listen-address") != nil {
		var err error
		monitor, err = prometheusmetrics.New(ctx,
			prometheusmetrics.WithLogLevel(util.LogLevel("metrics.prometheus")),
			prometheusmetrics.WithAddress(viper.GetString("metrics.prometheus.listen-address")),
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to start prometheus metrics service")
		}
		log.Info().
			Str("listen_address", viper.GetString("metrics.prometheus.listen-address")).
			Msg("Started prometheus metrics service")
	} else {
		log.Debug().Msg("No metrics service supplied; monitor not starting")
		monitor = &nullmetrics.Service{}
	}

	return monitor, nil
}

func startServices(ctx context.Context, monitor metrics.Service, majordomoSvc majordomo.Service) error {
	log.Trace().Msg("Starting execution database")
	execDB, err := util.InitExecDB(ctx, majordomoSvc)
	if err != nil {
		return errors.Wrap(err, "failed to start execution database")
	}

	comptrollerDB, err := util.InitComptrollerDB(ctx, majordomoSvc)
	if err != nil {
		return errors.Wrap(err, "failed to start comptroller database")
	}
	log.Trace().Msg("Checking for comptroller schema upgrades")
	if err := comptrollerDB.Upgrade(ctx); err != nil {
		return errors.Wrap(err, "failed to upgrade comptroller database")
	}

	schedulerSvc, err := standardscheduler.New(ctx,
		standardscheduler.WithLogLevel(util.LogLevel("scheduler")),
		standardscheduler.WithMonitor(monitor),
	)
	if err != nil {
		return errors.Wrap(err, "failed to start scheduler service")
	}

	consensusClient, err := util.FetchConsensusClient(ctx, viper.GetString("consensusclient.address"))
	if err != nil {
		return errors.Wrap(err, "failed to start consensus client")
	}

	chainTime, err := standardchaintime.New(ctx,
		standardchaintime.WithLogLevel(util.LogLevel("chaintime")),
		standardchaintime.WithGenesisProvider(consensusClient.(consensusclient.GenesisProvider)),
		standardchaintime.WithSpecProvider(consensusClient.(consensusclient.SpecProvider)),
		standardchaintime.WithForkScheduleProvider(consensusClient.(consensusclient.ForkScheduleProvider)),
	)
	if err != nil {
		return errors.Wrap(err, "failed to start chain time service")
	}

	log.Trace().Msg("Starting bids service")
	if err := startBids(ctx, comptrollerDB, monitor, schedulerSvc, chainTime, execDB); err != nil {
		return errors.Wrap(err, "failed to start bids service")
	}

	log.Trace().Msg("Starting relays service")
	if err := startRelays(ctx, comptrollerDB, monitor, schedulerSvc); err != nil {
		return errors.Wrap(err, "failed to start relays service")
	}

	return nil
}

func logModules() {
	buildInfo, ok := debug.ReadBuildInfo()
	if ok {
		log.Trace().Str("path", buildInfo.Path).Msg("Main package")
		for _, dep := range buildInfo.Deps {
			log := log.Trace()
			if dep.Replace == nil {
				log = log.Str("path", dep.Path).Str("version", dep.Version)
			} else {
				log = log.Str("path", dep.Replace.Path).Str("version", dep.Replace.Version)
			}
			log.Msg("Dependency")
		}
	}
}

func startRelays(
	ctx context.Context,
	comptrollerDB comptrollerdb.Service,
	monitor metrics.Service,
	schedulerSvc scheduler.Service,
) error {
	queuedProposerProviders := make([]relayclient.QueuedProposersProvider, 0)
	for _, relayAddress := range viper.GetStringSlice("relays.addresses") {
		relayClient, err := util.FetchRelayClient(ctx, relayAddress)
		if err != nil {
			log.Error().Str("address", relayAddress).Err(err).Msg("Failed to instantiate relay client; skipping")
			continue
		}
		queuedProposerProviders = append(queuedProposerProviders, relayClient.(relayclient.QueuedProposersProvider))
	}
	_, err := standardrelays.New(ctx,
		standardrelays.WithLogLevel(util.LogLevel("relays")),
		standardrelays.WithMonitor(monitor),
		standardrelays.WithScheduler(schedulerSvc),
		standardrelays.WithQueuedProposersProviders(queuedProposerProviders),
		standardrelays.WithValidatorRegistrationsProvider(comptrollerDB.(comptrollerdb.ValidatorRegistrationsProvider)),
		standardrelays.WithValidatorRegistrationsSetter(comptrollerDB.(comptrollerdb.ValidatorRegistrationsSetter)),
		standardrelays.WithInterval(viper.GetDuration("relays.interval")),
	)
	if err != nil {
		return errors.Wrap(err, "failed to create relays service")
	}

	return nil
}

func startBids(
	ctx context.Context,
	comptrollerDB comptrollerdb.Service,
	monitor metrics.Service,
	schedulerSvc scheduler.Service,
	chainTime chaintime.Service,
	execDB execdb.Service,
) error {
	blockPayments, err := standardblockpayments.New(ctx,
		standardblockpayments.WithLogLevel(util.LogLevel("blockpayments")),
		standardblockpayments.WithMonitor(monitor),
		standardblockpayments.WithChainTime(chainTime),
		standardblockpayments.WithReceivedBidsProvider(comptrollerDB.(comptrollerdb.ReceivedBidsProvider)),
		standardblockpayments.WithDeliveredBidsProvider(comptrollerDB.(comptrollerdb.DeliveredBidsProvider)),
		standardblockpayments.WithBlockPaymentsSetter(comptrollerDB.(comptrollerdb.BlockPaymentsSetter)),
		standardblockpayments.WithAlternateBidsSetter(comptrollerDB.(comptrollerdb.AlternateBidsSetter)),
		standardblockpayments.WithBlocksProvider(execDB.(execdb.BlocksProvider)),
		standardblockpayments.WithBalancesProvider(execDB.(execdb.BalancesProvider)),
		standardblockpayments.WithTransactionsProvider(execDB.(execdb.TransactionsProvider)),
		standardblockpayments.WithTransactionBalanceChangesProvider(execDB.(execdb.TransactionBalanceChangesProvider)),
		standardblockpayments.WithTrackDistance(viper.GetUint32("blockpayments.track-distance")),
		standardblockpayments.WithStartSlot(viper.GetInt64("blockpayments.start-slot")),
		standardblockpayments.WithReplaySlot(viper.GetInt64("blockpayments.replay-slot")),
	)
	if err != nil {
		return errors.Wrap(err, "failed to create block payments service")
	}

	receivedBidTracesProviders := make([]relayclient.ReceivedBidTracesProvider, 0)
	for _, relayAddress := range viper.GetStringSlice("bids.addresses") {
		relayClient, err := util.FetchRelayClient(ctx, relayAddress)
		if err != nil {
			log.Error().Str("address", relayAddress).Err(err).Msg("Failed to instantiate relay client; skipping")
			continue
		}
		receivedBidTracesProviders = append(receivedBidTracesProviders, relayClient.(relayclient.ReceivedBidTracesProvider))
	}
	deliveredBidTraceProviders := make([]relayclient.DeliveredBidTraceProvider, 0)
	for _, relayAddress := range viper.GetStringSlice("bids.addresses") {
		relayClient, err := util.FetchRelayClient(ctx, relayAddress)
		if err != nil {
			log.Error().Str("address", relayAddress).Err(err).Msg("Failed to instantiate relay client; skipping")
			continue
		}
		deliveredBidTraceProviders = append(deliveredBidTraceProviders, relayClient.(relayclient.DeliveredBidTraceProvider))
	}
	if _, err := standardbids.New(ctx,
		standardbids.WithLogLevel(util.LogLevel("bids")),
		standardbids.WithMonitor(monitor),
		standardbids.WithScheduler(schedulerSvc),
		standardbids.WithChainTime(chainTime),
		standardbids.WithDeliveredBidTraceProviders(deliveredBidTraceProviders),
		standardbids.WithReceivedBidTracesProviders(receivedBidTracesProviders),
		standardbids.WithDeliveredBidsSetter(comptrollerDB.(comptrollerdb.DeliveredBidsSetter)),
		standardbids.WithReceivedBidsSetter(comptrollerDB.(comptrollerdb.ReceivedBidsSetter)),
		standardbids.WithBidsReceivedHandlers([]bids.ReceivedHandler{blockPayments}),
		standardbids.WithInterval(viper.GetDuration("bids.interval")),
		standardbids.WithStartSlot(viper.GetInt64("bids.start-slot")),
	); err != nil {
		return errors.Wrap(err, "failed to create bids service")
	}

	return nil
}

func runCommands(_ context.Context) {
	if viper.GetBool("version") {
		fmt.Fprintf(os.Stdout, "%s\n", ReleaseVersion)
		//nolint:revive
		os.Exit(0)
	}
}
