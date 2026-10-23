package scheduler

import (
	"os"
	"path/filepath"
	"time"

	"github.com/wavever/CCLimitPing/internal/config"
)

// A nudge tells running watch loops that a provider's limits changed under
// them — `limitping redeem` spent a reset card — so a loop waiting out a window
// re-reads usage within a minute instead of at its next recheck. It is a file
// rather than a signal so it reaches a foreground watch as well as `bg`.
//
//	<config dir>/nudge/<provider>   touched on each nudge; its mtime is the signal

// Nudge wakes the watch loops waiting on provider.
func Nudge(provider string) error {
	path, err := nudgePath(provider)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(time.Now().Format(time.RFC3339)+"\n"), 0o644)
}

// nudgedSince reports whether provider was nudged after t.
func nudgedSince(provider string, t time.Time) bool {
	path, err := nudgePath(provider)
	if err != nil {
		return false
	}
	info, err := os.Stat(path)
	return err == nil && info.ModTime().After(t)
}

func nudgePath(provider string) (string, error) {
	dir, err := config.Dir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "nudge", provider), nil
}
