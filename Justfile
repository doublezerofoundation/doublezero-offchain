# Export required env
export SERVICEABILITY_PROGRAM_ID := "devnet"

# Fail on warnings
export RUSTFLAGS := "-Dwarnings"

# Default (list of commands)
default:
    just -l

# Run fmt
fmt:
    @rustup component add rustfmt
    @cargo fmt --all -- --config imports_granularity=Crate,group_imports=StdExternalCrate

# Check fmt
fmt-check:
	@rustup component add rustfmt
	@cargo fmt --all -- --check --config imports_granularity=Crate,group_imports=StdExternalCrate || (echo "Formatting check failed. Please run 'just fmt' to fix formatting issues." && exit 1)

# Build (release)
build:
    cargo build --release

# Run clippy
clippy:
    cargo clippy --all-features --all-targets -- -Dclippy::all

# Run tests
test:
    cargo nextest run

# Clean
clean:
    cargo clean

# Coverage
cov:
    cargo llvm-cov nextest --lcov --output-path lcov.info

# Coverage check (fail if below threshold)
cov-check:
    cargo llvm-cov nextest --fail-under-lines 25

# Check Elixir formatting
elixir-fmt-check:
    cd scheduler && mix format --check-formatted

# Format Elixir code
elixir-fmt:
    cd scheduler && mix format

# Compile Elixir with warnings as errors
elixir-compile:
    cd scheduler && mix compile --warnings-as-errors

# Run Credo (strict)
elixir-credo:
    cd scheduler && mix credo --strict

# Run Elixir tests
elixir-test:
    cd scheduler && mix test

# Run CI pipeline
ci:
    @just fmt-check
    @just clippy
    @just test
    @just cov-check

# Run unit tests only (fast, no external dependencies)
test-unit:
    cargo nextest run
    cd scheduler && mix test --cover

# Run integration tests (requires local validator)
test-integration:
    cargo test --features integration -p doublezero-solana-validator-debt

# Run end-to-end tests (Docker-based, requires Docker)
e2e-test:
    make -C e2e test

# Run e2e tests with debug logging
e2e-test-debug:
    DEBUG=1 make -C e2e test

# Run e2e tests (images must already be built)
e2e-test-nobuild:
    make -C e2e test-nobuild

# Build e2e container images
e2e-build:
    make -C e2e build

# Build e2e container images with verbose output
e2e-build-debug:
    make -C e2e build-debug

# Run all tests (unit + integration + e2e)
test-all:
    @just test-unit
    @just test-integration
    @just e2e-test
