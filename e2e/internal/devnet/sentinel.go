//go:build e2e

package devnet

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	dockercontainer "github.com/docker/docker/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	tcwait "github.com/testcontainers/testcontainers-go/wait"
)

func stringReader(s string) *strings.Reader {
	return strings.NewReader(s)
}

// SentinelSpec configures the sentinel container.
type SentinelSpec struct {
	ContainerImage              string
	KeypairPath                 string // Host path to sentinel keypair JSON.
	MulticastGroupPubkeys       string // Comma-separated multicast group pubkeys.
	MulticastPublisherPollSecs  int    // Poll interval in seconds (default: 5 for e2e).
	ConfigOverrides             map[string]string
}

// Sentinel manages the sentinel container.
type Sentinel struct {
	dn  *Devnet
	log *slog.Logger

	ContainerID string
	Container   testcontainers.Container
}

// Start launches the sentinel container.
func (s *Sentinel) Start(ctx context.Context) error {
	s.log.Debug("==> Starting sentinel", "image", s.dn.Spec.Sentinel.ContainerImage)

	networkName := s.dn.DefaultNetworkName

	pollSecs := s.dn.Spec.Sentinel.MulticastPublisherPollSecs
	if pollSecs == 0 {
		pollSecs = 5
	}

	// Build sentinel config as TOML.
	// Use "devnet" env — must match the program ID loaded in the ledger.
	// The sentinel's settings.serviceability_program_id() maps "devnet" to a
	// hardcoded program ID, so the ledger must load the program at that same address.
	config := fmt.Sprintf(`env = "devnet"
dz_rpc = "%s"
sol_rpc = "%s"
keypair = "%s"
log = "doublezero_ledger_sentinel=debug"
metrics_addr = "0.0.0.0:%d"
multicast_group_pubkeys = "%s"
data_api_url = "http://data-api-mock:%d/api/sql/query"
`,
		s.dn.Ledger.InternalRPCURL,
		s.dn.Ledger.InternalRPCURL,
		containerSentinelKeypairPath,
		internalSentinelMetrics,
		s.dn.Spec.Sentinel.MulticastGroupPubkeys,
		internalDataAPIMockPort,
	)

	// Apply any config overrides.
	for k, v := range s.dn.Spec.Sentinel.ConfigOverrides {
		config += fmt.Sprintf("%s = \"%s\"\n", k, v)
	}

	dataAPIURL := fmt.Sprintf("http://data-api-mock:%d/api/sql/query", internalDataAPIMockPort)

	// Use a wrapper script that writes the config file and then execs the sentinel.
	// This avoids issues with testcontainers file mounting and config crate env var parsing.
	entrypoint := fmt.Sprintf(`#!/bin/bash
set -e
cat > %s << 'SENTINEL_CONFIG'
%sSENTINEL_CONFIG
echo "==> Config written to %s"
cat %s
echo "==> Starting sentinel..."
exec doublezero-sentinel \
  --config %s \
  --poll-interval 30 \
  --enable-multicast-publisher \
  --multicast-publisher-poll-interval %d
`, containerSentinelConfigPath, config, containerSentinelConfigPath, containerSentinelConfigPath, containerSentinelConfigPath, pollSecs)

	// Also set env vars as fallback.
	env := map[string]string{
		"SENTINEL__DATA_API_URL": dataAPIURL,
	}

	req := testcontainers.ContainerRequest{
		Image: s.dn.Spec.Sentinel.ContainerImage,
		Name:  s.dn.Spec.DeployID + "-sentinel",
		ConfigModifier: func(cfg *dockercontainer.Config) {
			cfg.Hostname = "sentinel"
		},
		Env:        env,
		Entrypoint: []string{"bash", "-c"},
		Cmd:        []string{entrypoint},
		Files: []testcontainers.ContainerFile{
			{
				HostFilePath:      s.dn.Spec.Sentinel.KeypairPath,
				ContainerFilePath: containerSentinelKeypairPath,
			},
		},
		WaitingFor: tcwait.ForLog("multicast publisher sentinel starting").
			WithStartupTimeout(60 * time.Second).
			WithPollInterval(500 * time.Millisecond),
		Networks: []string{networkName},
		NetworkAliases: map[string][]string{
			networkName: {"sentinel"},
		},
		Resources: dockercontainer.Resources{
			NanoCPUs: defaultContainerNanoCPUs,
			Memory:   defaultContainerMemory,
		},
		Labels: s.dn.labels,
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return fmt.Errorf("failed to start sentinel: %w", err)
	}

	s.Container = container
	s.ContainerID = shortContainerID(container.GetContainerID())

	s.log.Debug("--> Sentinel started", "container", s.ContainerID)
	return nil
}
