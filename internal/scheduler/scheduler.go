// Package scheduler runs the watch loop: for each provider it sleeps until the
// 5h window resets, then triggers a minimal ping to start the next window,
// keeping windows back-to-back. It respects the weekly limit and never lets a
// transient error kill the loop. When requested on an interactive terminal, it
// also draws a live status line (heartbeat + per-provider countdowns) beneath
// the scrolling log.
package scheduler

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/wavever/CCLimitPing/internal/config"
	"github.com/wavever/CCLimitPing/internal/notify"
	"github.com/wavever/CCLimitPing/internal/provider"
	"github.com/wavever/CCLimitPing/internal/usage"
)

const (
	postPingGrace  = 15 * time.Second // wait after a ping before re-reading usage
	pingConfirm    = 20 * time.Second // wait between re-reads while confirming a ping's window
	pingConfirmMax = 4                // re-reads before falling back to an estimated window
	minBackoff     = 30 * time.Second
	maxBackoff     = 10 * time.Minute
	rateLimitPause = 5 * time.Minute
	defaultWindow  = 5 * time.Hour // fallback when the API omits the window length
	readTimeout    = 30 * time.Second
	triggerTimeout = 3 * time.Minute
	activeTaskPoll = time.Minute
	resetTolerance = time.Minute // reset times this close are the same reset, re-read
)

// Vars so tests can shorten them.
var (
	// stateRecheck caps how long the loop goes without reading usage while it
	// waits out a window. A window can end before its published reset — a reset
	// card redeemed outside limitping, a provider resetting everyone's limits —
	// and only a fresh read sees that. `limitping redeem` does not wait for it:
	// it nudges the loop (see Nudge).
	stateRecheck = 30 * time.Minute
	// wallClockNap is the longest stretch sleepCtx trusts the monotonic clock
	// for, and so how quickly a waiting loop notices a nudge.
	wallClockNap = time.Minute
)

// Target pairs a provider with its scheduling options.
type Target struct {
	Provider   provider.Provider
	AlignStart time.Time // zero = ping as soon as the window is free
	AutoRedeem bool      // spend a reset credit that is about to lapse
}

// Scheduler drives the watch loops.
type Scheduler struct {
	cfg     config.Config
	targets []Target
	dryRun  bool
	log     *log.Logger
	live    *liveStatus
}

// New builds a scheduler that logs to out. When live is true and out is an
// interactive terminal, a live status line is drawn beneath the scrolling log;
// otherwise log output passes straight through.
func New(cfg config.Config, targets []Target, dryRun, live bool, out io.Writer) *Scheduler {
	names := make([]string, len(targets))
	for i, t := range targets {
		names[i] = t.Provider.Name()
	}
	status := newLiveStatus(out, names, live)
	return &Scheduler{
		cfg:     cfg,
		targets: targets,
		dryRun:  dryRun,
		log:     log.New(status, "", log.LstdFlags),
		live:    status,
	}
}

// Run starts one loop per target and blocks until ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context) {
	names := make([]string, len(s.targets))
	for i, t := range s.targets {
		names[i] = t.Provider.Name()
	}
	s.log.Printf("watching %v (weekly_threshold=%.2f, reset_buffer=%s, notify=%t, dry_run=%t)",
		names, s.cfg.WeeklyThreshold, s.cfg.ResetBuffer.Duration, s.cfg.Notify, s.dryRun)

	var liveWG sync.WaitGroup
	if s.live.enabled {
		liveWG.Add(1)
		go func() {
			defer liveWG.Done()
			s.live.run(ctx)
		}()
	}

	done := make(chan struct{}, len(s.targets))
	for _, t := range s.targets {
		go func(t Target) {
			s.runTarget(ctx, t)
			done <- struct{}{}
		}(t)
	}
	for range s.targets {
		<-done
	}
	liveWG.Wait() // let the render loop clear its line before the final log
	s.log.Printf("shutting down")
}

func (s *Scheduler) runTarget(ctx context.Context, t Target) {
	name := t.Provider.Name()
	backoff := minBackoff
	var retry pingRetry
	aligned := t.AlignStart.IsZero() // whether the align gate has been passed
	var lastPingAt time.Time
	pingConfirms := 0 // re-reads spent waiting for the last ping's window to show up
	// The reset the loop last announced waiting for, so the re-reads during
	// one long wait log (and notify) it once; zero when not waiting on one.
	var waitKind string
	var waitReset time.Time
	announce := func(kind string, resetsAt time.Time) bool {
		if kind == waitKind && sameReset(resetsAt, waitReset) {
			return false
		}
		waitKind, waitReset = kind, resetsAt
		return true
	}

	for {
		if ctx.Err() != nil {
			return
		}

		s.live.set(name, "checking usage…", time.Time{})
		rctx, cancel := context.WithTimeout(ctx, readTimeout)
		readAt := time.Now()
		u, err := provider.ReadUsageForLoop(rctx, t.Provider, s.autoRedeems(t))
		cancel()
		if err != nil {
			var httpErr *provider.UsageHTTPError
			if errors.As(err, &httpErr) && httpErr.StatusCode == http.StatusTooManyRequests {
				wait := usageRateLimitWait(httpErr.RetryAfter, time.Now())
				s.log.Printf("[%s] usage endpoint rate limited; pausing reads for %s", name, wait.Round(time.Second))
				s.live.set(name, "usage rate limited", time.Now().Add(wait))
				if !sleepCtx(ctx, wait) {
					return
				}
				backoff = minBackoff
				continue
			}
			s.log.Printf("[%s] read usage failed: %v (retry in %s)", name, err, backoff)
			s.live.set(name, "read failed — retrying", time.Now().Add(backoff))
			if !sleepCtx(ctx, backoff) {
				return
			}
			backoff = nextBackoff(backoff)
			continue
		}
		backoff = minBackoff

		// A spent credit resets the windows, so this snapshot is stale; re-read
		// before deciding anything from it.
		if s.redeemExpiringCredit(ctx, t, u) {
			continue
		}

		// Respect the weekly limit: if exhausted (and no usable credits), wait
		// for the weekly window to reset instead of pinging.
		exhausted := s.weeklyExhausted(u)
		if waitKind == waitWeeklyExhausted && !exhausted && time.Now().Before(waitReset.Add(-resetTolerance)) {
			s.log.Printf("[%s] weekly limit no longer exhausted (%.0f%%) before its reset; resuming pings",
				name, u.Weekly.UsedPercent)
		}
		if exhausted {
			wait := u.Weekly.Remaining() + s.cfg.ResetBuffer.Duration
			if wait <= 0 {
				wait = time.Minute
			}
			if announce(waitWeeklyExhausted, u.Weekly.ResetsAt) {
				s.log.Printf("[%s] weekly limit exhausted (%.0f%%); sleeping %s until weekly reset",
					name, u.Weekly.UsedPercent, wait.Round(time.Second))
				s.notify(name+": weekly limit reached", "Skipping pings until weekly reset")
			}
			s.live.set(name, fmt.Sprintf("weekly limit reached (%.0f%%)", u.Weekly.UsedPercent), time.Now().Add(wait))
			if !waitWindow(ctx, name, readAt, min(wait, stateRecheck)) {
				return
			}
			continue
		}

		// A provider can stop enforcing the 5h window entirely (OpenAI
		// temporarily removed Codex's on 2026-07-12, leaving only the weekly
		// cap). The running weekly window is already anchored, so the next
		// useful ping is the one that anchors the weekly window right after
		// its reset — pinging every 5h would just burn weekly quota.
		if u.FiveHour.Missing() && u.Weekly.Active() {
			wait := u.Weekly.Remaining() + s.cfg.ResetBuffer.Duration
			if announce(waitWeeklyOnly, u.Weekly.ResetsAt) {
				s.log.Printf("[%s] no 5h window (weekly-only limits, %.0f%%); next ping at weekly reset %s (in %s)",
					name, u.Weekly.UsedPercent,
					u.Weekly.ResetsAt.Local().Format("15:04:05"), wait.Round(time.Second))
			}
			s.live.set(name, fmt.Sprintf("weekly-only %.0f%% — ping at weekly reset", u.Weekly.UsedPercent), time.Now().Add(wait))
			if !waitWindow(ctx, name, readAt, min(wait, stateRecheck)) {
				return
			}
			continue
		}

		// If the 5h window is still running, wait until it resets, then ping.
		if u.FiveHour.Active() {
			// Started, whoever started it: the next free window starts afresh.
			retry = pingRetry{}
			wait := u.FiveHour.Remaining() + s.cfg.ResetBuffer.Duration
			if announce(waitFiveHour, u.FiveHour.ResetsAt) {
				s.log.Printf("[%s] 5h window active (%.0f%%), next ping at %s (in %s)",
					name, u.FiveHour.UsedPercent,
					u.FiveHour.ResetsAt.Local().Format("15:04:05"), wait.Round(time.Second))
			}
			s.live.set(name, fmt.Sprintf("5h window %.0f%% — next ping", u.FiveHour.UsedPercent), time.Now().Add(wait))
			if !waitWindow(ctx, name, readAt, min(wait, stateRecheck)) {
				return
			}
			continue
		}
		waitKind, waitReset = "", time.Time{}

		// Window is free. Guard against double-pinging if our last ping isn't
		// reflected by the endpoint yet. Estimating the cycle from our own ping
		// time is the last resort, not the first: it anchors the next ping to
		// when we pinged rather than to when the window actually resets, which
		// walks the whole schedule forward by a ping's latency every cycle. So
		// give the endpoint a few more reads to publish the real reset time
		// first.
		if !lastPingAt.IsZero() {
			est := lastPingAt.Add(windowLen(u.FiveHour))
			if time.Now().Before(est) {
				wait, confirming := pingVisibilityWait(est, s.cfg.ResetBuffer.Duration, pingConfirms, time.Now())
				if confirming {
					pingConfirms++
					s.log.Printf("[%s] ping not visible in usage yet; re-checking in %s (%d/%d)",
						name, wait.Round(time.Second), pingConfirms, pingConfirmMax)
					s.live.set(name, "confirming window…", time.Now().Add(wait))
				} else {
					s.log.Printf("[%s] ping still not visible; estimating the window, waiting %s",
						name, wait.Round(time.Second))
					s.live.set(name, "awaiting window (estimated)", time.Now().Add(wait))
				}
				if !sleepCtx(ctx, wait) {
					return
				}
				continue
			}
		}

		// Honor the first-window alignment anchor, once.
		if !aligned {
			if d := time.Until(t.AlignStart); d > 0 {
				s.log.Printf("[%s] waiting for align_start %s (in %s)",
					name, t.AlignStart.Local().Format("15:04:05"), d.Round(time.Second))
				s.live.set(name, "waiting for align_start", t.AlignStart)
				if !sleepCtx(ctx, d) {
					return
				}
			}
			aligned = true
		}

		if desc, active, err := activeProviderTask(ctx, t.Provider); err != nil {
			s.log.Printf("[%s] active task check failed: %v; pinging anyway", name, err)
		} else if active {
			s.log.Printf("[%s] window reset but %s is running; waiting %s for it to start the next window",
				name, desc, activeTaskPoll.Round(time.Second))
			s.live.set(name, desc+" active — deferring ping", time.Now().Add(activeTaskPoll))
			if !sleepCtx(ctx, activeTaskPoll) {
				return
			}
			continue
		}

		// Trigger the window.
		if !s.dryRun {
			s.log.Printf("[%s] window reset — triggering ping now…", name)
		}
		s.live.set(name, "window reset — triggering ping…", time.Time{})
		tctx, tcancel := context.WithTimeout(ctx, triggerTimeout)
		res, err := t.Provider.Trigger(tctx, s.dryRun)
		tcancel()
		if s.dryRun {
			if err != nil {
				wait, _ := retry.fail()
				s.log.Printf("[%s] dry-run ping failed: %v (retry in %s)", name, err, wait)
				if !sleepCtx(ctx, wait) {
					return
				}
				continue
			}
			s.log.Printf("[%s] DRY-RUN would ping now: %s%s", name, res.Command, triggerModel(res))
			// In dry-run we can't actually start a window, so estimate the next
			// cycle from the configured window length to keep the loop sane.
			// Sleep immediately instead of doing an extra usage read that cannot
			// observe a real newly-started window.
			lastPingAt, pingConfirms = time.Now(), 0
			wait := windowLen(u.FiveHour) + s.cfg.ResetBuffer.Duration
			s.live.set(name, "dry-run — next estimated ping", lastPingAt.Add(wait))
			if !sleepCtx(ctx, wait) {
				return
			}
			continue
		}
		if err != nil {
			wait, first := retry.fail()
			s.log.Printf("[%s] ping failed: %v (retry in %s)", name, err, wait)
			s.live.set(name, "ping failed — retrying", time.Now().Add(wait))
			if first {
				s.notify(name+": ping failed", err.Error())
			}
			if !sleepCtx(ctx, wait) {
				return
			}
			continue
		}
		retry = pingRetry{}
		lastPingAt, pingConfirms = time.Now(), 0
		s.log.Printf("[%s] ping sent, new window started%s%s", name, triggerModel(res), triggerCost(res))
		s.live.set(name, "ping sent — checking window soon", lastPingAt.Add(postPingGrace))
		s.notify(name+": window started", "New 5h window"+triggerCost(res))

		if !sleepCtx(ctx, postPingGrace) {
			return
		}
	}
}

func (s *Scheduler) weeklyExhausted(u *usage.Usage) bool {
	return u.WeeklyExhausted(s.cfg.WeeklyThreshold)
}

// redeemExpiringCredit spends a banked reset credit that is about to lapse,
// when the target opted in. It reports whether the windows were actually reset;
// a failure is logged and never breaks the loop.
func (s *Scheduler) redeemExpiringCredit(ctx context.Context, t Target, u *usage.Usage) bool {
	redeemer, ok := t.Provider.(provider.ResetCreditRedeemer)
	if !ok || !s.autoRedeems(t) {
		return false
	}
	name := t.Provider.Name()
	rctx, cancel := context.WithTimeout(ctx, readTimeout)
	res, err := redeemer.AutoRedeemResetCredit(rctx, u)
	cancel()
	switch {
	case err != nil:
		s.log.Printf("[%s] reset credit redeem failed: %v", name, err)
	case res.Outcome == provider.RedeemReset:
		s.log.Printf("[%s] redeemed an expiring reset credit; rate-limit windows reset", name)
		s.notify(name+": reset credit redeemed", "An expiring reset credit was spent; the windows are reset")
		return true
	case res.Outcome != "":
		s.log.Printf("[%s] reset credit not spent: %s", name, res)
	}
	return false
}

// autoRedeems reports whether t's loop spends reset credits on its own, which
// is what lets its usage reads carry the credits (provider.ReadUsageForLoop).
func (s *Scheduler) autoRedeems(t Target) bool {
	_, ok := t.Provider.(provider.ResetCreditRedeemer)
	return t.AutoRedeem && ok && !s.dryRun
}

func activeProviderTask(ctx context.Context, p provider.Provider) (string, bool, error) {
	detector, ok := p.(provider.ActiveTaskDetector)
	if !ok {
		return "", false, nil
	}
	return detector.ActiveTask(ctx)
}

func (s *Scheduler) notify(title, msg string) {
	if s.cfg.Notify {
		notify.Notify(title, msg)
	}
}

// triggerCost renders the token/cost tail for logs, e.g.
// " — 32934 tokens (in 32792 / out 142), $0.0110".
// triggerModel names the model a ping used. The watch log is the only record an
// unattended run leaves behind, and the command line names the model only when
// limitping passed one explicitly.
func triggerModel(res *provider.TriggerResult) string {
	if res == nil || res.Model == "" {
		return ""
	}
	return fmt.Sprintf(" (model: %s)", res.Model)
}

func triggerCost(res *provider.TriggerResult) string {
	if res == nil || !res.HasUsage {
		return ""
	}
	s := fmt.Sprintf(" — %d tokens (in %d / out %d)", res.TotalTokens, res.InputTokens, res.OutputTokens)
	if res.CostUSD > 0 {
		s += fmt.Sprintf(", $%.4f", res.CostUSD)
	}
	return s
}

// pingVisibilityWait decides how to wait when the window our own ping started
// has not shown up in usage yet. est is when a window estimated from the ping
// time would end. While confirming, it re-reads soon so that the real reset
// time — once published — is what drives the cycle; only after pingConfirmMax
// re-reads does it give up and wait out the estimate, which is a whole window
// spent on a guess anchored to our ping rather than to the provider's reset.
func pingVisibilityWait(est time.Time, buffer time.Duration, attempts int, now time.Time) (wait time.Duration, confirming bool) {
	wait = est.Sub(now) + buffer
	if attempts >= pingConfirmMax {
		return wait, false
	}
	if wait > pingConfirm {
		wait = pingConfirm
	}
	return wait, true
}

func windowLen(w usage.Window) time.Duration {
	if w.WindowSeconds > 0 {
		return time.Duration(w.WindowSeconds) * time.Second
	}
	return defaultWindow
}

// pingRetry paces the retries of a ping that keeps failing. It is kept apart
// from the usage-read backoff because every retry begins with a usage read
// that succeeds and resets that one, which held a ping that could not succeed
// — one waiting on the user, say — to a retry and a notification every 30
// seconds. The zero value is a ping that has not failed.
type pingRetry struct {
	next     time.Duration
	failures int
}

// fail records a failed ping and returns how long to wait before the next
// attempt, and whether this failure opens a run of them — the one worth a
// notification; the retries after it are in the log.
func (r *pingRetry) fail() (wait time.Duration, first bool) {
	if r.next == 0 {
		r.next = minBackoff
	}
	wait, first = r.next, r.failures == 0
	r.failures++
	r.next = nextBackoff(r.next)
	return wait, first
}

func nextBackoff(d time.Duration) time.Duration {
	d *= 2
	if d > maxBackoff {
		return maxBackoff
	}
	return d
}

func usageRateLimitWait(retryAfter time.Time, now time.Time) time.Duration {
	if !retryAfter.IsZero() {
		wait := retryAfter.Sub(now)
		if wait > 0 {
			return wait
		}
	}
	return rateLimitPause
}

// The long waits runTarget announces, for logging each one once.
const (
	waitWeeklyExhausted = "weekly-exhausted"
	waitWeeklyOnly      = "weekly-only"
	waitFiveHour        = "5h"
)

// sameReset reports whether two published reset times name the same reset;
// re-reads of one window can differ by a second or so.
func sameReset(a, b time.Time) bool {
	d := a.Sub(b)
	return d > -resetTolerance && d < resetTolerance
}

// Sleep is sleepCtx for the other long-waiting loops (`limitping schedule`).
func Sleep(ctx context.Context, d time.Duration) bool {
	return sleepCtx(ctx, d)
}

// sleepCtx sleeps for d of wall-clock time and reports false if the context was
// cancelled. Go timers run on the monotonic clock, which on macOS stops while
// the machine sleeps, so one long timer fires late by however long the lid was
// shut. Napping in short stretches against a wall-clock deadline (Round(0)
// drops the monotonic reading) wakes on time instead.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	return sleepOrWake(ctx, d, nil)
}

// waitWindow is sleepCtx for waiting out one of name's windows, cut short when
// `limitping redeem` nudges name after readAt, the read the wait is based on:
// the card it spent may have ended the window.
func waitWindow(ctx context.Context, name string, readAt time.Time, d time.Duration) bool {
	return sleepOrWake(ctx, d, func() bool { return nudgedSince(name, readAt) })
}

// sleepOrWake is sleepCtx that also ends early, between naps, once woken
// reports true.
func sleepOrWake(ctx context.Context, d time.Duration, woken func() bool) bool {
	if d <= 0 {
		return true
	}
	deadline := time.Now().Round(0).Add(d)
	for {
		left := deadline.Sub(time.Now().Round(0))
		if left <= 0 || (woken != nil && woken()) {
			return true
		}
		t := time.NewTimer(min(left, wallClockNap))
		select {
		case <-ctx.Done():
			t.Stop()
			return false
		case <-t.C:
		}
	}
}
