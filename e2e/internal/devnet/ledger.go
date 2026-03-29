//go:build e2e

package devnet

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	dockercontainer "github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	tcwait "github.com/testcontainers/testcontainers-go/wait"
)

// LedgerSpec configures the Solana test validator container.
type LedgerSpec struct {
	ContainerImage          string
	ServiceabilityProgramID string // Program ID to load the serviceability .so at genesis.
}

// Ledger manages a Solana test validator container.
type Ledger struct {
	dn  *Devnet
	log *slog.Logger

	ContainerID      string
	Container        testcontainers.Container
	InternalRPCURL   string
	ExternalRPCPort  int
}

// ExternalRPCURL returns the RPC URL accessible from the host.
func (l *Ledger) ExternalRPCURL() string {
	return fmt.Sprintf("http://%s:%d", l.dn.ExternalHost, l.ExternalRPCPort)
}

// Start launches the Solana test validator container.
func (l *Ledger) Start(ctx context.Context) error {
	l.log.Debug("==> Starting ledger", "image", l.dn.Spec.Ledger.ContainerImage)

	networkName := l.dn.DefaultNetworkName

	env := map[string]string{}
	if l.dn.Spec.Ledger.ServiceabilityProgramID != "" {
		env["SERVICEABILITY_PROGRAM_ID"] = l.dn.Spec.Ledger.ServiceabilityProgramID
	}

	req := testcontainers.ContainerRequest{
		Image: l.dn.Spec.Ledger.ContainerImage,
		Name:  l.dn.Spec.DeployID + "-ledger",
		ConfigModifier: func(cfg *dockercontainer.Config) {
			cfg.Hostname = "ledger"
		},
		ExposedPorts: []string{fmt.Sprintf("%d/tcp", internalLedgerRPCPort)},
		Env:          env,
		WaitingFor: tcwait.ForHTTP("/").
			WithPort(nat.Port(fmt.Sprintf("%d/tcp", internalLedgerRPCPort))).
			WithMethod(http.MethodPost).
			WithHeaders(map[string]string{"Content-Type": "application/json"}).
			WithBody(strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"getHealth"}`)).
			WithResponseMatcher(func(body io.Reader) bool {
				content, err := io.ReadAll(body)
				if err != nil {
					return false
				}
				return strings.Contains(string(content), `"result":"ok"`)
			}).
			WithStartupTimeout(3 * time.Minute).
			WithPollInterval(500 * time.Millisecond),
		Networks: []string{networkName},
		NetworkAliases: map[string][]string{
			networkName: {"ledger"},
		},
		Resources: dockercontainer.Resources{
			NanoCPUs: defaultContainerNanoCPUs,
			Memory:   ledgerContainerMemory,
		},
		Labels: l.dn.labels,
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return fmt.Errorf("failed to start ledger: %w", err)
	}

	l.Container = container
	l.ContainerID = shortContainerID(container.GetContainerID())
	l.InternalRPCURL = "http://ledger:8899"

	mappedPort, err := container.MappedPort(ctx, nat.Port(fmt.Sprintf("%d/tcp", internalLedgerRPCPort)))
	if err != nil {
		return fmt.Errorf("failed to get mapped port: %w", err)
	}
	port, err := strconv.Atoi(mappedPort.Port())
	if err != nil {
		return fmt.Errorf("failed to parse mapped port: %w", err)
	}
	l.ExternalRPCPort = port

	l.log.Debug("--> Ledger started",
		"container", l.ContainerID,
		"internalRPCURL", l.InternalRPCURL,
		"externalRPCPort", l.ExternalRPCPort,
	)
	return nil
}
