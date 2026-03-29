//go:build e2e

package devnet

const (
	defaultContainerNanoCPUs int64 = 1_000_000_000          // 1 core
	defaultContainerMemory   int64 = 1024 * 1024 * 1024     // 1GB
	ledgerContainerMemory    int64 = 4 * 1024 * 1024 * 1024 // 4GB

	containerSolanaKeypairPath     = "/root/.config/solana/id.json"
	containerDoublezeroKeypairPath = "/root/.config/doublezero/id.json"
	containerSentinelKeypairPath   = "/etc/sentinel/keypair.json"
	containerSentinelConfigPath    = "/etc/sentinel/config.toml"

	internalLedgerRPCPort    = 8899
	internalDataAPIMockPort  = 8080
	internalSentinelMetrics  = 2112
)

func shortContainerID(id string) string {
	if len(id) > 12 {
		return id[:12]
	}
	return id
}
