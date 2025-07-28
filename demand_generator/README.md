# Demand Generator

This component fetches validator data from Solana, enriches it with geolocation information, and generates demand matrices based on stake distribution for use in Shapley value calculations.

## Overview

The Demand Generator performs the following key functions:

1. **Validator Data Collection**: Fetches active validator information from Solana RPC endpoints
2. **IP Enrichment**: Enriches validator IPs with geolocation data using the ipinfo.io API
3. **Geographic Aggregation**: Groups validators by city to create network topology
4. **Demand Matrix Generation**: Creates traffic demand patterns based on stake distribution

## Architecture

### Component Structure

```
demand_generator/
├── src/
│   ├── generator.rs       # Main orchestration logic
│   ├── types.rs          # Data structures and models
│   ├── demand_matrix.rs  # Demand calculation algorithms
│   ├── city_aggregator.rs # Geographic aggregation logic
│   ├── settings.rs       # Configuration management
│   ├── constants.rs      # Static values and defaults
│   └── lib.rs           # Library exports
└── Cargo.toml
```

### Data Flow

```
Solana RPC → Validator Data → IP Enrichment → City Aggregation → Demand Matrix
     ↓              ↓                ↓               ↓                  ↓
Vote Accounts   IP Addresses    Geolocation    City Groups      Traffic Patterns
```

## Usage

### As a Library

```rust
use demand_generator::{DemandGenerator, Settings};

#[tokio::main]
async fn main() -> Result<()> {
    // Load settings from environment
    let settings = Settings::from_env()?;

    // Create generator instance
    let generator = DemandGenerator::new(settings);

    // Generate demand matrix
    let demands = generator.generate().await?;

    // Or get both enriched validators and demands
    let (validators, demands) = generator.generate_with_validators().await?;

    Ok(())
}
```

### Integration Example

```rust
use demand_generator::{DemandGenerator, write_demand_csv};

// Generate and export demand data
let generator = DemandGenerator::new(settings);
let (validators, demands) = generator.generate_with_validators().await?;

// Write to CSV
write_demand_csv(&demands, "output/demands.csv").await?;
```

## Configuration

### Settings Structure

The component uses a hierarchical configuration system:

```toml
# config/default.toml
[demand_generator]
solana_rpc_url = "https://api.mainnet-beta.solana.com"
concurrent_api_requests = 5

[demand_generator.ip_info]
base_url = "https://ipinfo.io"

[demand_generator.backoff]
factor = 2.0
min_delay_ms = 100
max_delay_ms = 30000
max_times = 3
```

### Environment Variables

Configuration can be overridden using environment variables with the `DZ__` prefix:

| Variable                                        | Description                          | Required |
| ----------------------------------------------- | ------------------------------------ | -------- |
| `DZ__DEMAND_GENERATOR__IP_INFO__API_TOKEN`      | ipinfo.io API token                  | Yes      |
| `DZ__DEMAND_GENERATOR__SOLANA_RPC_URL`          | Solana RPC endpoint                  | No       |
| `DZ__DEMAND_GENERATOR__CONCURRENT_API_REQUESTS` | Max concurrent API calls             | No       |
| `DZ_ENV`                                        | Environment (devnet/testnet/mainnet) | No       |

## Technical Details

### Demand Matrix Algorithm

The demand matrix generation follows these steps:

1. **Stake Calculation**: Aggregate validator stakes by city
2. **Traffic Generation**: Calculate bidirectional traffic between city pairs using the formula (subject to evolve overtime):
   ```
   traffic = (source_stake × destination_stake / total_stake) × multiplier
   ```
3. **Priority Assignment**: Assign priority levels based on combined stake percentage
4. **Type Classification**: Cities with stake ≥ threshold use Type 2 (high-stake) routing

### City Aggregation Process

1. **Geolocation Parsing**: Extract latitude/longitude from IP info
2. **City Code Generation**: Create standardized 3-letter city codes
3. **Stake Aggregation**: Sum stakes for all validators in each city
4. **Metadata Collection**: Track validator count and average location

### IP Enrichment Workflow

1. **Concurrent Processing**: Use semaphore-controlled concurrent requests
2. **Retry Logic**: Implement exponential backoff with jitter
3. **Error Handling**: Continue processing on individual failures
4. **Rate Monitoring**: Track success rate and processing speed
