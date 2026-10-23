package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/wavever/CCLimitPing/internal/config"
	"github.com/wavever/CCLimitPing/internal/provider"
	"github.com/wavever/CCLimitPing/internal/scheduler"
)

type scheduleSpec struct {
	Every time.Duration
	At    []dailyTime
}

type dailyTime struct {
	Hour   int
	Minute int
	Second int
}

func newScheduleCmd() *cobra.Command {
	var every time.Duration
	var at []string
	var dryRun bool
	text := localizedText()
	cmd := &cobra.Command{
		Use:       "schedule [provider]",
		Aliases:   []string{"sched"},
		Short:     text.scheduleShort,
		Long:      text.scheduleLong,
		Args:      cobra.MatchAll(cobra.MaximumNArgs(1), cobra.OnlyValidArgs),
		ValidArgs: []string{"claude", "codex", "all"},
		RunE: func(cmd *cobra.Command, args []string) error {
			name := "all"
			if len(args) > 0 {
				name = args[0]
			}
			cfg, err := config.Load()
			if err != nil {
				return err
			}
			providers, err := selectProviders(cfg, name)
			if err != nil {
				return err
			}
			spec, err := parseScheduleSpec(every, at)
			if err != nil {
				return err
			}
			ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer stop()
			return runSchedule(ctx, cmd.OutOrStdout(), text, providers, spec, dryRun)
		},
	}
	cmd.Flags().DurationVar(&every, "every", 0, text.scheduleEveryFlag)
	cmd.Flags().StringArrayVar(&at, "at", nil, text.scheduleAtFlag)
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, text.pingDryRunFlag)
	return cmd
}

func parseScheduleSpec(every time.Duration, rawAt []string) (scheduleSpec, error) {
	if every < 0 {
		return scheduleSpec{}, fmt.Errorf("--every must not be negative")
	}
	spec := scheduleSpec{Every: every}
	for _, raw := range splitScheduleAt(rawAt) {
		t, err := parseDailyTime(raw)
		if err != nil {
			return scheduleSpec{}, err
		}
		spec.At = append(spec.At, t)
	}
	sort.Slice(spec.At, func(i, j int) bool { return spec.At[i].seconds() < spec.At[j].seconds() })
	if spec.Every == 0 && len(spec.At) == 0 {
		return scheduleSpec{}, fmt.Errorf("set --every duration or at least one --at HH:MM time")
	}
	return spec, nil
}

func splitScheduleAt(values []string) []string {
	var out []string
	for _, value := range values {
		for _, part := range strings.Split(value, ",") {
			part = strings.TrimSpace(part)
			if part != "" {
				out = append(out, part)
			}
		}
	}
	return out
}

func parseDailyTime(raw string) (dailyTime, error) {
	layouts := []string{"15:04", "15:04:05"}
	for _, layout := range layouts {
		t, err := time.ParseInLocation(layout, raw, time.Local)
		if err == nil {
			return dailyTime{Hour: t.Hour(), Minute: t.Minute(), Second: t.Second()}, nil
		}
	}
	return dailyTime{}, fmt.Errorf("invalid --at %q (want HH:MM or HH:MM:SS)", raw)
}

func (t dailyTime) seconds() int {
	return t.Hour*3600 + t.Minute*60 + t.Second
}

func (t dailyTime) on(day time.Time) time.Time {
	return time.Date(day.Year(), day.Month(), day.Day(), t.Hour, t.Minute, t.Second, 0, day.Location())
}

func (s scheduleSpec) nextRun(now time.Time) time.Time {
	var next time.Time
	if s.Every > 0 {
		next = now.Add(s.Every)
	}
	for _, at := range s.At {
		candidate := at.on(now)
		if !candidate.After(now) {
			candidate = candidate.Add(24 * time.Hour)
		}
		if next.IsZero() || candidate.Before(next) {
			next = candidate
		}
	}
	return next
}

func runSchedule(ctx context.Context, out io.Writer, text cliText, providers []provider.Provider, spec scheduleSpec, dryRun bool) error {
	names := make([]string, len(providers))
	for i, p := range providers {
		names[i] = p.Name()
	}
	fmt.Fprintf(out, text.scheduleStartedFmt, strings.Join(names, ", "), scheduleDescription(spec), dryRunNote(dryRun))

	for {
		next := spec.nextRun(time.Now())
		if next.IsZero() {
			return nil
		}
		wait := time.Until(next)
		if wait < 0 {
			wait = 0
		}
		fmt.Fprintf(out, text.scheduleNextFmt, next.Local().Format("2006-01-02 15:04:05"), fmtDur(text, wait))
		if !scheduler.Sleep(ctx, wait) {
			return nil
		}
		fmt.Fprintf(out, text.scheduleRunFmt, time.Now().Local().Format("2006-01-02 15:04:05"))
		var firstErr error
		for _, p := range providers {
			if err := runPing(ctx, out, text, p, dryRun, false); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		if firstErr != nil {
			fmt.Fprintf(out, text.scheduleErrorFmt, firstErr)
		}
	}
}

func scheduleDescription(spec scheduleSpec) string {
	var parts []string
	if spec.Every > 0 {
		parts = append(parts, "every "+spec.Every.String())
	}
	if len(spec.At) > 0 {
		values := make([]string, len(spec.At))
		for i, at := range spec.At {
			values[i] = at.String()
		}
		parts = append(parts, "at "+strings.Join(values, ", "))
	}
	return strings.Join(parts, "; ")
}

func (t dailyTime) String() string {
	if t.Second > 0 {
		return fmt.Sprintf("%02d:%02d:%02d", t.Hour, t.Minute, t.Second)
	}
	return fmt.Sprintf("%02d:%02d", t.Hour, t.Minute)
}
