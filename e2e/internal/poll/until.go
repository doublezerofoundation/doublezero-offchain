//go:build e2e

package poll

import (
	"context"
	"fmt"
	"time"
)

// Until polls the condition function at the given interval until it returns true
// or the context deadline is reached. Returns an error if the context expires
// before the condition is met.
func Until(ctx context.Context, condition func() (bool, error), timeout, interval time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out after %s waiting for condition", timeout)
		case <-ticker.C:
			ok, err := condition()
			if err != nil {
				return err
			}
			if ok {
				return nil
			}
		}
	}
}
