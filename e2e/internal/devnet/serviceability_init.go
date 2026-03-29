//go:build e2e

package devnet

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	solanautil "github.com/malbeclabs/doublezero-offchain/e2e/internal/solana"
	"github.com/testcontainers/testcontainers-go/exec"
)

// InitServiceability deploys and initializes the serviceability program on the
// ledger. This runs the doublezero CLI inside the ledger container.
func (d *Devnet) InitServiceability(ctx context.Context) error {
	d.log.Info("==> Initializing serviceability")

	sentinelPubkey := "me"
	if d.Spec.Sentinel.KeypairPath != "" {
		var err error
		sentinelPubkey, err = solanautil.PubkeyFromKeypairJSON(d.Spec.Sentinel.KeypairPath)
		if err != nil {
			return fmt.Errorf("failed to read sentinel pubkey: %w", err)
		}
	}

	programID := d.Spec.Ledger.ServiceabilityProgramID

	// Build the initialization script.
	script := fmt.Sprintf(`#!/bin/bash
set -euo pipefail

echo "==> Generating keypair..."
solana-keygen new --no-bip39-passphrase --silent --outfile /root/.config/solana/id.json 2>/dev/null || true

echo "==> Configuring Solana CLI..."
solana config set --url http://localhost:8899

echo "==> Configuring doublezero CLI..."
doublezero config set \
  --keypair /root/.config/solana/id.json \
  --url http://localhost:8899 \
  --ws ws://localhost:8900 \
  --program-id %s

echo "==> Funding manager..."
solana airdrop 100
sleep 1
solana airdrop 100

echo "==> Initializing serviceability..."
doublezero init

echo "==> Setting authorities..."
doublezero global-config authority set \
  --activator-authority me \
  --sentinel-authority %s

echo "==> Setting network configuration..."
doublezero global-config set \
  --local-asn 65000 \
  --remote-asn 65342 \
  --device-tunnel-block 172.16.0.0/16 \
  --user-tunnel-block 169.254.0.0/16 \
  --multicastgroup-block 233.84.178.0/24

echo "==> Creating location..."
doublezero location create --code ams --name "Amsterdam" --country NL \
  --lat 52.3080392 --lng 4.9440734

echo "==> Creating exchange..."
doublezero exchange create --code xams --name "Amsterdam AMS-IX" \
  --lat 52.3080392 --lng 4.9440734

echo "==> Creating contributor..."
doublezero contributor create --code co01 --owner me

echo "==> Done initializing serviceability"
`, programID, sentinelPubkey)

	if err := d.execInLedger(ctx, script); err != nil {
		return fmt.Errorf("failed to initialize serviceability: %w", err)
	}

	d.log.Info("--> Serviceability initialized")
	return nil
}

// CreateDevice creates and activates a device on the ledger.
func (d *Devnet) CreateDevice(ctx context.Context, code, exchange, contributor, location, publicIP, dzPrefixes string) error {
	d.onchainWriteMutex.Lock()
	defer d.onchainWriteMutex.Unlock()

	script := fmt.Sprintf(`#!/bin/bash
set -euo pipefail
doublezero device create --code %s --exchange %s --contributor %s --location %s --public-ip %s --dz-prefixes %s
`, code, exchange, contributor, location, publicIP, dzPrefixes)

	return d.execInLedgerUnlocked(ctx, script)
}

// CreateMulticastGroup creates a multicast group on the ledger and returns its pubkey.
func (d *Devnet) CreateMulticastGroup(ctx context.Context, code, maxBandwidth string) (string, error) {
	d.onchainWriteMutex.Lock()
	defer d.onchainWriteMutex.Unlock()

	// Create the group.
	script := fmt.Sprintf(`#!/bin/bash
set -euo pipefail
doublezero multicast group create --code %s --max-bandwidth %s --owner me
`, code, maxBandwidth)

	if err := d.execInLedgerUnlocked(ctx, script); err != nil {
		return "", fmt.Errorf("failed to create multicast group: %w", err)
	}

	// Query for the pubkey using show/list.
	showScript := fmt.Sprintf(`#!/bin/bash
set -euo pipefail
doublezero multicast group show --code %s 2>&1
`, code)

	output, err := d.execInLedgerWithOutputUnlocked(ctx, showScript)
	if err != nil {
		// Try listing all groups and grep for the code.
		listScript := `#!/bin/bash
set -euo pipefail
doublezero multicast group list 2>&1
`
		output, err = d.execInLedgerWithOutputUnlocked(ctx, listScript)
		if err != nil {
			return "", fmt.Errorf("failed to list multicast groups: %w\noutput: %s", err, output)
		}
	}

	// Parse pubkey from output — look for a base58 string (32+ chars, alphanumeric).
	for _, line := range strings.Split(output, "\n") {
		for _, word := range strings.Fields(line) {
			if looksLikePubkey(word) {
				return word, nil
			}
		}
	}

	return "", fmt.Errorf("could not parse multicast group pubkey from output: %s", output)
}

// looksLikePubkey checks if a string looks like a Solana base58 pubkey.
func looksLikePubkey(s string) bool {
	if len(s) < 32 || len(s) > 44 {
		return false
	}
	for _, c := range s {
		if !((c >= '1' && c <= '9') || (c >= 'A' && c <= 'H') || (c >= 'J' && c <= 'N') ||
			(c >= 'P' && c <= 'Z') || (c >= 'a' && c <= 'k') || (c >= 'm' && c <= 'z')) {
			return false
		}
	}
	return true
}

// ExecDoublezero runs an arbitrary doublezero CLI command in the ledger container.
func (d *Devnet) ExecDoublezero(ctx context.Context, args ...string) (string, error) {
	d.onchainWriteMutex.Lock()
	defer d.onchainWriteMutex.Unlock()

	cmd := "doublezero " + strings.Join(args, " ")
	return d.execInLedgerWithOutputUnlocked(ctx, cmd)
}

func (d *Devnet) execInLedger(ctx context.Context, script string) error {
	_, err := d.execInLedgerWithOutput(ctx, script)
	return err
}

func (d *Devnet) execInLedgerUnlocked(ctx context.Context, script string) error {
	_, err := d.execInLedgerWithOutputUnlocked(ctx, script)
	return err
}

func (d *Devnet) execInLedgerWithOutputUnlocked(ctx context.Context, script string) (string, error) {
	return d.execInLedgerImpl(ctx, script)
}

func (d *Devnet) execInLedgerWithOutput(ctx context.Context, script string) (string, error) {
	return d.execInLedgerImpl(ctx, script)
}

func (d *Devnet) execInLedgerImpl(ctx context.Context, script string) (string, error) {
	exitCode, reader, err := d.Ledger.Container.Exec(ctx,
		[]string{"bash", "-c", script},
		exec.Multiplexed(),
	)
	if err != nil {
		return "", fmt.Errorf("exec failed: %w", err)
	}

	var buf bytes.Buffer
	if reader != nil {
		buf.ReadFrom(reader)
	}

	if exitCode != 0 {
		return buf.String(), fmt.Errorf("command exited with code %d: %s", exitCode, buf.String())
	}

	return buf.String(), nil
}
