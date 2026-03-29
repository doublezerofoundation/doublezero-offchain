//go:build e2e

package e2e_test

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"testing"

	dockerclient "github.com/docker/docker/client"
	"github.com/joho/godotenv"

	"github.com/malbeclabs/doublezero-offchain/e2e/internal/devnet"
)

var (
	logger       *slog.Logger
	dockerClient *dockerclient.Client
	verbose      bool
	debug        bool
)

func TestMain(m *testing.M) {
	flag.Parse()

	verbose = testing.Verbose()
	debug = os.Getenv("DEBUG") != ""

	// Initialize logger.
	logLevel := slog.LevelInfo
	if debug {
		logLevel = slog.LevelDebug
	}
	logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel}))

	// Load .env.local for container image names.
	workspaceDir := findWorkspaceDir()
	envPath := filepath.Join(workspaceDir, "e2e", ".env.local")
	if err := godotenv.Load(envPath); err != nil {
		logger.Error("Failed to load .env.local", "error", err, "path", envPath)
		os.Exit(1)
	}

	// Create Docker client.
	var err error
	dockerClient, err = dockerclient.NewClientWithOpts(
		dockerclient.FromEnv,
		dockerclient.WithAPIVersionNegotiation(),
	)
	if err != nil {
		logger.Error("Failed to create Docker client", "error", err)
		os.Exit(1)
	}

	// Build container images (unless OFFCHAIN_E2E_NO_BUILD is set).
	if os.Getenv("OFFCHAIN_E2E_NO_BUILD") == "" {
		buildCtx, buildCancel := signal.NotifyContext(context.Background(), os.Interrupt)
		logger.Info("Building container images")
		err = devnet.BuildContainerImages(buildCtx, logger, workspaceDir, debug)
		buildCancel()
		if err != nil {
			logger.Error("Failed to build container images", "error", err)
			os.Exit(1)
		}
		logger.Info("Container images built successfully")
	}

	os.Exit(m.Run())
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
