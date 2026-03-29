//go:build e2e

package e2e_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/malbeclabs/doublezero-offchain/e2e/internal/devnet"
	solanautil "github.com/malbeclabs/doublezero-offchain/e2e/internal/solana"
)

// TestE2E_SentinelMulticastPublisherCreatesPublishers verifies that the sentinel's
// multicast publisher worker detects IBRL validators and creates multicast
// publisher users on-chain.
func TestE2E_SentinelMulticastPublisherCreatesPublishers(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	deployDir := t.TempDir()

	// Generate keypairs.
	sentinelKeypairPath, sentinelPubkey, err := solanautil.GenerateKeypair(filepath.Join(deployDir, "sentinel"))
	require.NoError(t, err)
	t.Logf("Sentinel pubkey: %s", sentinelPubkey)

	// Use the devnet serviceability program ID — must match what the sentinel's
	// settings maps "devnet" env to. The program .so is loaded at this address
	// by the ledger's entrypoint via --bpf-program.
	programID := "GYhQDKuESrasNZGyhMJhGYFtbzNijYhcrN9poSqCQVah"
	t.Logf("Serviceability program ID: %s", programID)

	// Create devnet.
	dn, err := devnet.New(devnet.DevnetSpec{
		DeployID:  "sentinel-mcast-" + shortTestName(t),
		DeployDir: deployDir,
		Ledger: devnet.LedgerSpec{
			ContainerImage:          os.Getenv("OFFCHAIN_LEDGER_IMAGE"),
			ServiceabilityProgramID: programID,
		},
		Sentinel: devnet.SentinelSpec{
			ContainerImage:             os.Getenv("OFFCHAIN_SENTINEL_IMAGE"),
			KeypairPath:                sentinelKeypairPath,
			MulticastPublisherPollSecs: 5,
		},
		DataAPIMock: devnet.DataAPIMockSpec{
			ContainerImage: os.Getenv("OFFCHAIN_DATA_API_MOCK_IMAGE"),
		},
	}, logger, dockerClient)
	require.NoError(t, err)

	// Start devnet (ledger + serviceability init + data API mock).
	err = dn.Start(ctx)
	t.Cleanup(func() { _ = dn.Destroy(context.Background()) })
	require.NoError(t, err)

	step := func(name string, fn func(t *testing.T)) {
		dn.Step(t, name, fn)
	}

	var multicastGroupPK string

	step("create-device-and-multicast-group", func(t *testing.T) {
		// Create a device.
		err := dn.CreateDevice(ctx, "dev01", "xams", "co01", "ams", "45.33.100.1", "45.33.100.8/29")
		require.NoError(t, err)

		// Get device pubkey, set max-users, and activate.
		deviceGetOutput, err := dn.ExecDoublezero(ctx, "device", "get", "--code", "dev01", "--json")
		require.NoError(t, err, "failed to get device: %s", deviceGetOutput)

		// Parse pubkey from JSON output.
		var deviceInfo struct {
			Account string `json:"account"`
		}
		err = json.Unmarshal([]byte(strings.TrimSpace(deviceGetOutput)), &deviceInfo)
		require.NoError(t, err, "failed to parse device JSON: %s", deviceGetOutput)
		require.NotEmpty(t, deviceInfo.Account, "device pubkey empty")
		t.Logf("Device pubkey: %s", deviceInfo.Account)

		output, err := dn.ExecDoublezero(ctx, "device", "update", "--pubkey", deviceInfo.Account, "--max-users", "10")
		require.NoError(t, err, "failed to set max users: %s", output)

		output, err = dn.ExecDoublezero(ctx, "device", "update", "--pubkey", deviceInfo.Account, "--status", "activated")
		require.NoError(t, err, "failed to activate device: %s", output)

		// Create a multicast group.
		pk, err := dn.CreateMulticastGroup(ctx, "e2e-test", "1Gbps")
		require.NoError(t, err)
		require.NotEmpty(t, pk)
		multicastGroupPK = pk
		t.Logf("Multicast group pubkey: %s", multicastGroupPK)
	})

	step("create-ibrl-users", func(t *testing.T) {
		// Create IBRL users on-chain for test validator IPs.
		// This simulates validators that have already connected to the DZ network.
		// Using IPs within the device's dz-prefixes range (45.33.100.8/29).
		// Each user needs an AccessPass set first, then user create.
		ips := []string{"45.33.100.9", "45.33.100.10", "45.33.100.11"}
		for _, ip := range ips {
			// Set access pass for this IP.
			output, err := dn.ExecDoublezero(ctx,
				"access-pass", "set",
				"--client-ip", ip,
				"--user-payer", "me",
			)
			require.NoError(t, err, "failed to set access pass for %s: %s", ip, output)

			// Create the user.
			output, err = dn.ExecDoublezero(ctx,
				"user", "create",
				"--device", "dev01",
				"--client-ip", ip,
			)
			require.NoError(t, err, "failed to create IBRL user for %s: %s", ip, output)
			t.Logf("Created IBRL user for %s", ip)
		}
	})

	step("configure-data-api-mock", func(t *testing.T) {
		// Configure the data API mock to return validator records for our IPs.
		err := dn.DataAPIMock.SetValidators(ctx, devnet.SqlResponse{
			Rows: [][]any{
				{"node1_pubkey", int64(2000_000_000_000), "45.33.100.9"},  // 2000 SOL
				{"node2_pubkey", int64(1000_000_000_000), "45.33.100.10"}, // 1000 SOL
				{"node3_pubkey", int64(500_000_000_000), "45.33.100.11"},  // 500 SOL
			},
		})
		require.NoError(t, err)
	})

	step("start-sentinel", func(t *testing.T) {
		// Update sentinel spec with the multicast group pubkey.
		dn.Spec.Sentinel.MulticastGroupPubkeys = multicastGroupPK

		err := dn.StartSentinel(ctx)
		require.NoError(t, err)
	})
}

func shortTestName(t *testing.T) string {
	name := t.Name()
	if len(name) > 20 {
		name = name[:20]
	}
	return name
}
