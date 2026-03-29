//go:build e2e

package devnet

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	dockercontainer "github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	tcwait "github.com/testcontainers/testcontainers-go/wait"
)

// DataAPIMockSpec configures the data API mock container.
type DataAPIMockSpec struct {
	ContainerImage string
}

// DataAPIMock manages the data API mock container.
type DataAPIMock struct {
	dn  *Devnet
	log *slog.Logger

	ContainerID     string
	Container       testcontainers.Container
	ExternalPort    int
}

// ExternalURL returns the data API mock URL accessible from the host.
func (d *DataAPIMock) ExternalURL() string {
	return fmt.Sprintf("http://%s:%d", d.dn.ExternalHost, d.ExternalPort)
}

// Start launches the data API mock container.
func (d *DataAPIMock) Start(ctx context.Context) error {
	d.log.Debug("==> Starting data API mock", "image", d.dn.Spec.DataAPIMock.ContainerImage)

	networkName := d.dn.DefaultNetworkName

	req := testcontainers.ContainerRequest{
		Image: d.dn.Spec.DataAPIMock.ContainerImage,
		Name:  d.dn.Spec.DeployID + "-data-api-mock",
		ConfigModifier: func(cfg *dockercontainer.Config) {
			cfg.Hostname = "data-api-mock"
		},
		ExposedPorts: []string{fmt.Sprintf("%d/tcp", internalDataAPIMockPort)},
		WaitingFor: tcwait.ForHTTP("/health").
			WithPort(nat.Port(fmt.Sprintf("%d/tcp", internalDataAPIMockPort))).
			WithStartupTimeout(30 * time.Second).
			WithPollInterval(500 * time.Millisecond),
		Networks: []string{networkName},
		NetworkAliases: map[string][]string{
			networkName: {"data-api-mock"},
		},
		Resources: dockercontainer.Resources{
			NanoCPUs: defaultContainerNanoCPUs,
			Memory:   defaultContainerMemory,
		},
		Labels: d.dn.labels,
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return fmt.Errorf("failed to start data API mock: %w", err)
	}

	d.Container = container
	d.ContainerID = shortContainerID(container.GetContainerID())

	mappedPort, err := container.MappedPort(ctx, nat.Port(fmt.Sprintf("%d/tcp", internalDataAPIMockPort)))
	if err != nil {
		return fmt.Errorf("failed to get mapped port: %w", err)
	}
	port, err := strconv.Atoi(mappedPort.Port())
	if err != nil {
		return fmt.Errorf("failed to parse mapped port: %w", err)
	}
	d.ExternalPort = port

	d.log.Debug("--> Data API mock started", "container", d.ContainerID, "externalPort", d.ExternalPort)
	return nil
}

// SqlResponse mirrors the data API response format.
type SqlResponse struct {
	Rows [][]any `json:"rows"`
}

// SetValidators updates the data API mock with new validator data via its PUT /config endpoint.
func (d *DataAPIMock) SetValidators(ctx context.Context, response SqlResponse) error {
	data, err := json.Marshal(response)
	if err != nil {
		return fmt.Errorf("failed to marshal response: %w", err)
	}

	url := fmt.Sprintf("%s/config", d.ExternalURL())
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to update mock config: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("mock config update returned %d", resp.StatusCode)
	}

	d.log.Debug("Updated data API mock validators", "rows", len(response.Rows))
	return nil
}
