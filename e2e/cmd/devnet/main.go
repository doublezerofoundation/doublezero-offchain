//go:build e2e

package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"

	dockerclient "github.com/docker/docker/client"
	"github.com/joho/godotenv"

	"github.com/malbeclabs/doublezero-offchain/e2e/internal/devnet"
	solanautil "github.com/malbeclabs/doublezero-offchain/e2e/internal/solana"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: devnet <build|start|stop|destroy> [flags]\n")
		os.Exit(1)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	cmd := os.Args[1]

	switch cmd {
	case "build":
		verbose := len(os.Args) > 2 && os.Args[2] == "-v"
		workspaceDir := findWorkspaceDir()
		loadEnv(workspaceDir)
		if err := devnet.BuildContainerImages(ctx, logger, workspaceDir, verbose); err != nil {
			logger.Error("Build failed", "error", err)
			os.Exit(1)
		}

	case "start":
		workspaceDir := findWorkspaceDir()
		loadEnv(workspaceDir)

		deployDir := filepath.Join(workspaceDir, ".devnet")
		os.MkdirAll(deployDir, 0755)

		sentinelKeypairPath, sentinelPubkey, err := solanautil.GenerateKeypair(filepath.Join(deployDir, "sentinel"))
		if err != nil {
			logger.Error("Failed to generate sentinel keypair", "error", err)
			os.Exit(1)
		}
		logger.Info("Generated sentinel keypair", "pubkey", sentinelPubkey)

		// Use devnet serviceability program ID (matches sentinel env = "devnet").
		programID := "GYhQDKuESrasNZGyhMJhGYFtbzNijYhcrN9poSqCQVah"

		dockerClient, err := dockerclient.NewClientWithOpts(
			dockerclient.FromEnv, dockerclient.WithAPIVersionNegotiation(),
		)
		if err != nil {
			logger.Error("Failed to create Docker client", "error", err)
			os.Exit(1)
		}

		dn, err := devnet.New(devnet.DevnetSpec{
			DeployID:  "offchain-local",
			DeployDir: deployDir,
			Ledger: devnet.LedgerSpec{
				ContainerImage:          os.Getenv("OFFCHAIN_LEDGER_IMAGE"),
				ServiceabilityProgramID: programID,
			},
			Sentinel: devnet.SentinelSpec{
				ContainerImage: os.Getenv("OFFCHAIN_SENTINEL_IMAGE"),
				KeypairPath:    sentinelKeypairPath,
			},
			DataAPIMock: devnet.DataAPIMockSpec{
				ContainerImage: os.Getenv("OFFCHAIN_DATA_API_MOCK_IMAGE"),
			},
		}, logger, dockerClient)
		if err != nil {
			logger.Error("Failed to create devnet", "error", err)
			os.Exit(1)
		}

		if err := dn.Start(ctx); err != nil {
			logger.Error("Failed to start devnet", "error", err)
			os.Exit(1)
		}

		logger.Info("Devnet started",
			"rpcURL", dn.Ledger.ExternalRPCURL(),
			"dataAPIMockURL", dn.DataAPIMock.ExternalURL(),
		)

		// Wait for interrupt.
		<-ctx.Done()
		logger.Info("Shutting down...")
		dn.Destroy(context.Background())

	case "stop":
		logger.Info("Stop not implemented yet — use 'destroy' to clean up all containers")

	case "destroy":
		logger.Info("Destroying devnet containers...")
		fmt.Println("Run: docker rm -f $(docker ps -aq --filter label=offchain.doublezero) 2>/dev/null; docker network rm $(docker network ls -q --filter label=offchain.doublezero) 2>/dev/null")

	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", cmd)
		os.Exit(1)
	}
}

func findWorkspaceDir() string {
	dir, _ := os.Getwd()
	for {
		if _, err := os.Stat(filepath.Join(dir, "Cargo.toml")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			wd, _ := os.Getwd()
			return filepath.Dir(wd)
		}
		dir = parent
	}
}

func loadEnv(workspaceDir string) {
	envPath := filepath.Join(workspaceDir, "e2e", ".env.local")
	if err := godotenv.Load(envPath); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: could not load %s: %v\n", envPath, err)
	}
}
