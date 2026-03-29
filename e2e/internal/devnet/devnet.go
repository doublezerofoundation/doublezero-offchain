//go:build e2e

package devnet

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	dockerfilters "github.com/docker/docker/api/types/filters"
	dockernetwork "github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/testcontainers/testcontainers-go"
)

// DevnetSpec configures a devnet instance.
type DevnetSpec struct {
	DeployID  string
	DeployDir string

	Ledger      LedgerSpec
	Sentinel    SentinelSpec
	DataAPIMock DataAPIMockSpec
}

// Devnet manages a local devnet for e2e testing.
type Devnet struct {
	Spec DevnetSpec

	log               *slog.Logger
	dockerClient      *client.Client
	labels            map[string]string
	onchainWriteMutex sync.Mutex

	ExternalHost       string
	DefaultNetworkName string
	Ledger             *Ledger
	Sentinel           *Sentinel
	DataAPIMock        *DataAPIMock
}

// New creates a new Devnet instance.
func New(spec DevnetSpec, log *slog.Logger, dockerClient *client.Client) (*Devnet, error) {
	log = log.With("deployID", spec.DeployID)

	if spec.DeployID == "" {
		return nil, fmt.Errorf("deployID is required")
	}
	if spec.DeployDir == "" {
		return nil, fmt.Errorf("deployDir is required")
	}

	if err := os.MkdirAll(spec.DeployDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create deploy directory: %w", err)
	}

	labels := map[string]string{
		"offchain.doublezero":           "true",
		"offchain.doublezero/type":      "devnet",
		"offchain.doublezero/deploy-id": spec.DeployID,
	}

	externalHost := os.Getenv("DIND_LOCALHOST")
	if externalHost == "" {
		externalHost = "localhost"
	}

	dn := &Devnet{
		Spec:         spec,
		log:          log,
		dockerClient: dockerClient,
		labels:       labels,
		ExternalHost: externalHost,
	}

	dn.Ledger = &Ledger{dn: dn, log: log.With("component", "ledger")}
	dn.Sentinel = &Sentinel{dn: dn, log: log.With("component", "sentinel")}
	dn.DataAPIMock = &DataAPIMock{dn: dn, log: log.With("component", "data-api-mock")}

	return dn, nil
}

// Start launches all devnet components in order.
func (d *Devnet) Start(ctx context.Context) error {
	d.log.Info("==> Starting devnet")
	start := time.Now()

	// Create the Docker network.
	networkName := d.Spec.DeployID + "-default"
	d.log.Debug("==> Creating default network", "name", networkName)
	//nolint:staticcheck // SA1019
	_, err := testcontainers.GenericNetwork(ctx, testcontainers.GenericNetworkRequest{
		NetworkRequest: testcontainers.NetworkRequest{
			Name:   networkName,
			Labels: d.labels,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to create default network: %w", err)
	}
	d.DefaultNetworkName = networkName

	// 1. Start the ledger.
	if err := d.Ledger.Start(ctx); err != nil {
		return fmt.Errorf("failed to start ledger: %w", err)
	}

	// 2. Initialize serviceability on-chain state.
	if err := d.InitServiceability(ctx); err != nil {
		return fmt.Errorf("failed to initialize serviceability: %w", err)
	}

	// 3. Start data API mock.
	if err := d.DataAPIMock.Start(ctx); err != nil {
		return fmt.Errorf("failed to start data API mock: %w", err)
	}

	d.log.Info("--> Devnet started", "duration", time.Since(start))
	return nil
}

// StartSentinel starts the sentinel container. Called separately so tests can
// configure state before the sentinel starts polling.
func (d *Devnet) StartSentinel(ctx context.Context) error {
	return d.Sentinel.Start(ctx)
}

// Stop stops all running containers.
func (d *Devnet) Stop(ctx context.Context) error {
	d.log.Info("==> Stopping devnet")

	var errs []error
	if d.Sentinel.Container != nil {
		if err := d.Sentinel.Container.Stop(ctx, nil); err != nil {
			errs = append(errs, fmt.Errorf("sentinel: %w", err))
		}
	}
	if d.DataAPIMock.Container != nil {
		if err := d.DataAPIMock.Container.Stop(ctx, nil); err != nil {
			errs = append(errs, fmt.Errorf("data-api-mock: %w", err))
		}
	}
	if d.Ledger.Container != nil {
		if err := d.Ledger.Container.Stop(ctx, nil); err != nil {
			errs = append(errs, fmt.Errorf("ledger: %w", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors stopping containers: %v", errs)
	}
	return nil
}

// Destroy removes all containers and the Docker network.
func (d *Devnet) Destroy(ctx context.Context) error {
	d.log.Info("==> Destroying devnet")

	// Remove containers.
	containers := []testcontainers.Container{}
	if d.Sentinel.Container != nil {
		containers = append(containers, d.Sentinel.Container)
	}
	if d.DataAPIMock.Container != nil {
		containers = append(containers, d.DataAPIMock.Container)
	}
	if d.Ledger.Container != nil {
		containers = append(containers, d.Ledger.Container)
	}

	for _, c := range containers {
		if err := c.Terminate(ctx); err != nil {
			d.log.Warn("Failed to terminate container", "id", c.GetContainerID(), "error", err)
		}
	}

	// Remove the network.
	if d.DefaultNetworkName != "" {
		networks, err := d.dockerClient.NetworkList(ctx, dockernetwork.ListOptions{
			Filters: dockerfilters.NewArgs(dockerfilters.Arg("name", d.DefaultNetworkName)),
		})
		if err == nil {
			for _, n := range networks {
				_ = d.dockerClient.NetworkRemove(ctx, n.ID)
			}
		}
	}

	return nil
}

// Step runs a named subtest with container health checking.
func (d *Devnet) Step(t *testing.T, name string, fn func(t *testing.T)) {
	t.Helper()
	d.AssertContainersRunning(t)
	t.Run(name, fn)
	if t.Failed() {
		d.DumpDiagnostics(t)
		t.FailNow()
	}
}

// AssertContainersRunning verifies all expected containers are still alive.
func (d *Devnet) AssertContainersRunning(t *testing.T) {
	t.Helper()
	ctx := t.Context()

	containers := map[string]testcontainers.Container{
		"ledger": d.Ledger.Container,
	}
	if d.DataAPIMock.Container != nil {
		containers["data-api-mock"] = d.DataAPIMock.Container
	}
	if d.Sentinel.Container != nil {
		containers["sentinel"] = d.Sentinel.Container
	}

	for name, c := range containers {
		if c == nil {
			continue
		}
		state, err := c.State(ctx)
		if err != nil {
			t.Fatalf("Failed to get state for %s: %v", name, err)
		}
		if !state.Running {
			t.Fatalf("Container %s is not running (status: %s)", name, state.Status)
		}
	}
}

// DumpDiagnostics collects container logs for debugging test failures.
func (d *Devnet) DumpDiagnostics(t *testing.T) {
	t.Helper()

	containers := map[string]testcontainers.Container{
		"ledger": d.Ledger.Container,
	}
	if d.DataAPIMock.Container != nil {
		containers["data-api-mock"] = d.DataAPIMock.Container
	}
	if d.Sentinel.Container != nil {
		containers["sentinel"] = d.Sentinel.Container
	}

	for name, c := range containers {
		if c == nil {
			continue
		}
		logs, err := c.Logs(t.Context())
		if err != nil {
			t.Logf("Failed to get logs for %s: %v", name, err)
			continue
		}
		all, _ := io.ReadAll(logs)
		// Strip Docker multiplexed log headers (8-byte frames).
		clean := stripDockerLogHeaders(all)
		t.Logf("=== %s logs (%d bytes) ===\n%s", name, len(clean), string(clean))
	}
}

// stripDockerLogHeaders removes the 8-byte Docker multiplexed log frame headers.
// Each frame has: [stream_type(1) | padding(3) | size(4, big-endian)] followed by payload.
func stripDockerLogHeaders(data []byte) []byte {
	var result []byte
	for len(data) >= 8 {
		frameSize := binary.BigEndian.Uint32(data[4:8])
		data = data[8:]
		if int(frameSize) > len(data) {
			result = append(result, data...)
			break
		}
		result = append(result, data[:frameSize]...)
		data = data[frameSize:]
	}
	return result
}
