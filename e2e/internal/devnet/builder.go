//go:build e2e

package devnet

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"
)

type depSHAs struct {
	Doublezero string `json:"doublezero"`
}

const shaCacheFile = ".dep-shas.json"

func loadDepSHAs(workspaceDir string) (*depSHAs, error) {
	data, err := os.ReadFile(filepath.Join(workspaceDir, "e2e", shaCacheFile))
	if err != nil {
		return nil, fmt.Errorf("failed to read %s: %w", shaCacheFile, err)
	}
	var shas depSHAs
	if err := json.Unmarshal(data, &shas); err != nil {
		return nil, fmt.Errorf("failed to parse %s: %w", shaCacheFile, err)
	}
	if shas.Doublezero == "" {
		return nil, fmt.Errorf("incomplete SHAs in %s", shaCacheFile)
	}
	return &shas, nil
}

const dockerfilesDirRelativeToWorkspace = "e2e/docker"

// BuildContainerImages builds all Docker images needed for e2e tests.
func BuildContainerImages(ctx context.Context, log *slog.Logger, workspaceDir string, verbose bool) error {
	log.Info("==> Building docker images", "verbose", verbose)
	start := time.Now()

	dockerfilesDir := filepath.Join(workspaceDir, dockerfilesDirRelativeToWorkspace)

	shas, err := loadDepSHAs(workspaceDir)
	if err != nil {
		return err
	}
	log.Info("--> Using pinned dependency SHAs", "doublezero", shas.Doublezero[:12])

	cacheBuster := shas.Doublezero[:12]
	baseExtraArgs := []string{
		"--build-arg", fmt.Sprintf("CACHE_BUSTER=%s", cacheBuster),
		"--build-arg", fmt.Sprintf("DOUBLEZERO_SHA=%s", shas.Doublezero),
		"--platform", "linux/amd64",
	}

	// Build base image first (other images depend on it).
	if err := dockerBuild(ctx, log, os.Getenv("OFFCHAIN_BASE_IMAGE"),
		filepath.Join(dockerfilesDir, "base.dockerfile"), workspaceDir, verbose, baseExtraArgs...); err != nil {
		return fmt.Errorf("failed to build base image: %w", err)
	}

	baseImageArg := fmt.Sprintf("BASE_IMAGE=%s", os.Getenv("OFFCHAIN_BASE_IMAGE"))

	// Build component images (can be parallel).
	type buildTask struct {
		name       string
		envVar     string
		dockerfile string
		args       []string
	}
	tasks := []buildTask{
		{
			name:       "ledger",
			envVar:     "OFFCHAIN_LEDGER_IMAGE",
			dockerfile: filepath.Join(dockerfilesDir, "ledger", "Dockerfile"),
			args: append([]string{
				"--build-arg", baseImageArg,
				"--build-arg", "DOCKERFILE_DIR=" + filepath.Join(dockerfilesDirRelativeToWorkspace, "ledger"),
			}, baseExtraArgs...),
		},
		{
			name:       "sentinel",
			envVar:     "OFFCHAIN_SENTINEL_IMAGE",
			dockerfile: filepath.Join(dockerfilesDir, "sentinel", "Dockerfile"),
			args:       append([]string{"--build-arg", baseImageArg}, baseExtraArgs...),
		},
		{
			name:       "data-api-mock",
			envVar:     "OFFCHAIN_DATA_API_MOCK_IMAGE",
			dockerfile: filepath.Join(dockerfilesDir, "data-api-mock", "Dockerfile"),
			args: append([]string{
				"--build-arg", "DOCKERFILE_DIR=" + filepath.Join(dockerfilesDirRelativeToWorkspace, "data-api-mock"),
			}, baseExtraArgs...),
		},
	}

	if verbose {
		for _, task := range tasks {
			if err := dockerBuild(ctx, log, os.Getenv(task.envVar), task.dockerfile, workspaceDir, verbose, task.args...); err != nil {
				return fmt.Errorf("failed to build %s image: %w", task.name, err)
			}
		}
	} else {
		var wg sync.WaitGroup
		errChan := make(chan error, len(tasks))
		for _, task := range tasks {
			wg.Add(1)
			go func(t buildTask) {
				defer wg.Done()
				if err := dockerBuild(ctx, log, os.Getenv(t.envVar), t.dockerfile, workspaceDir, verbose, t.args...); err != nil {
					errChan <- fmt.Errorf("failed to build %s image: %w", t.name, err)
				}
			}(task)
		}
		wg.Wait()
		close(errChan)
		for err := range errChan {
			if err != nil {
				return err
			}
		}
	}

	log.Info("--> Docker images built", "duration", time.Since(start))
	return nil
}

func dockerBuild(ctx context.Context, log *slog.Logger, tag, dockerfile, contextDir string, verbose bool, extraArgs ...string) error {
	args := []string{"build", "-t", tag, "-f", dockerfile}
	args = append(args, extraArgs...)
	args = append(args, contextDir)

	log.Info("Building image", "tag", tag)

	cmd := exec.CommandContext(ctx, "docker", args...)
	if verbose {
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
	}

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("docker build failed for %s: %w", tag, err)
	}
	return nil
}
