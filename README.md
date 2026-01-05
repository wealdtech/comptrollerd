# `comptrollerd`

`comptrollerd` is a service that collects and manages relay bid/delivery information and block payment data for Ethereum.
It runs periodic jobs to fetch traces from relays, updates metadata and state, and persists all data to a PostgreSQL database.

## Overview

`comptrollerd` monitors the Ethereum beacon chain and tracks:

- **Received bids**: Bids submitted to relays by builders
- **Delivered bids**: Bids that were actually built and delivered as blocks
- **Block payments**: Payment information for blocks built and delivered
- **Validator registrations**: Registration information for validators

The service is designed to maintain consistency across multiple relay providers through transactional metadata updates and comprehensive state tracking.

## Prerequisites

- Go 1.25.0 or later
- PostgreSQL 12 or later
- Access to:
  - Ethereum consensus client (for chain time and slots)
  - Ethereum execution client (for block data)
  - One or more relay servers (for bid/delivery traces)

## Development Setup

### 1. Clone the repository

```bash
git clone https://github.com/wealdtech/comptrollerd.git
cd comptrollerd
```

### 2. Set up PostgreSQL databases

Create two PostgreSQL databases (one for execution data and one for comptroller data):

```bash
createdb execdb
createdb comptrollerdb
```

### 3. Configure comptrollerd

Create a configuration file at `~/.comptrollerd.toml`:

```toml
[execdb]
server = "localhost"
port = 5432
user = "postgres"
password = "securepassword"

[comptrollerdb]
server = "localhost"
port = 5432
user = "postgres"
password = "securepassword"

[consensusclient]
address = "https://mainnet-checkpoint-sync.attestant.io"

[relays]
    [relays.relay]
    address = "wss://relay.example.com"
    timeout = "10s"
    log-level = "debug"
```

Configuration can also be provided via command-line flags or environment variables:

- Command-line flags: `--flag-name value`
- Environment variables: `COMPTROLLERD_FLAG_NAME=value`

### 4. Build and run

```bash
# Build
go build ./...

# Run
./comptrollerd
```

## Architecture

`comptrollerd` is a modular Go application with a clean separation of concerns:

### Service Layer

The service layer (`services/`) contains the core business logic, organized by feature:

- **bids** (`services/bids/`): Collects received and delivered bid traces from relays
  - Implements periodic fetching from relay providers
  - Maintains per-provider slot tracking via metadata
  - Handles bid deduplication and validation
  - Triggers handlers when bids are received

- **blockpayments** (`services/blockpayments/`): Calculates and tracks block payment data
  - Processes delivered bids into payment information
  - Maintains consensus around payment totals

- **chaintime** (`services/chaintime/`): Provides beacon chain time utilities
  - Slot and epoch calculations
  - Time-based conversions

- **comptrollerdb** (`services/comptrollerdb/`): Database abstraction layer
  - PostgreSQL persistence
  - Transactional metadata management
  - Schema migrations and upgrades

- **scheduler** (`services/scheduler/`): Job scheduling
  - Periodic job scheduling
  - Job lifecycle management

- **metrics** (`services/metrics/`): Metrics collection and export
  - Prometheus metrics support
  - Custom metric registration

### Utilities

The `util/` package provides shared helpers:

- Consensus and execution client initialization
- Relay client configuration and management
- Timeout utilities
- Concurrency helpers
- Logging utilities

### Database Layer

The `services/comptrollerdb/postgresql/` directory contains:

- Schema definitions and migrations (`upgrader.go`)
- Query implementations for each data type
- Transactional helpers (`tx.go`)
- Metadata serialization/deserialization

## Configuration Reference

### Core Settings

- `log-level`: Minimum log level (trace, debug, info, warn, error)
- `log-file`: Optional file path for log output
- `profile-address`: Address for Go pprof profiler
- `tracing-address`: OpenTelemetry tracing endpoint

### Bids Service

- `bids.interval`: Polling interval for bid updates (default: 12s)
- `bids.start-slot`: Initial slot to start bidding from (default: -1, use stored state)

### Block Payments Service

- `blockpayments.interval`: Update interval for block payments
- `blockpayments.start-slot`: Initial slot for payment tracking
- `blockpayments.track-distance`: Number of slots to track back (default: 96)

### Database

- `comptrollerdb.server`: PostgreSQL host
- `comptrollerdb.port`: PostgreSQL port (default: 5432)
- `comptrollerdb.user`: Database user
- `comptrollerdb.password`: Database password
- `comptrollerdb.max-connections`: Connection pool size (default: 32)

### Relays

- `relays.interval`: Polling interval for relay updates (default: 384s)
- `relays.<name>.address`: Relay server address
- `relays.<name>.timeout`: Request timeout
- `relays.<name>.log-level`: Logging level for this relay

### Consensus Client

- `consensusclient.address`: Consensus client URL/address

## Testing

Run all tests:

```bash
go test ./...
```

Run tests for a specific package:

```bash
go test ./services/bids/standard -v
```

Run a specific test:

```bash
go test ./services/bids/standard -run TestNewWithStartSlot_Success -v
```

The test suite uses `testify` for assertions and mocking. Generated mocks are in the `mocks/` directory and are created using `mockery`.

## License

Copyright © 2022 - 2026 Weald Technology Trading.

Licensed under the Apache License, Version 2.0. See LICENSE file for details.
