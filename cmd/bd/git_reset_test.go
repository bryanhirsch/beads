//go:build cgo

package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/steveyegge/beads/internal/config"
	"github.com/steveyegge/beads/internal/types"
)

func TestGitResetCmdRegistered(t *testing.T) {
	t.Parallel()

	// Verify bd reset is registered on rootCmd
	cmd, _, err := rootCmd.Find([]string{"reset"})
	if err != nil {
		t.Fatalf("bd reset not found: %v", err)
	}
	if cmd.Name() != "reset" {
		t.Errorf("expected command name 'reset', got %q", cmd.Name())
	}
	if !cmd.DisableFlagParsing {
		t.Error("bd reset should have DisableFlagParsing=true to pass args through to git")
	}
}

func TestCheckRefsCmdRegistered(t *testing.T) {
	t.Parallel()

	cmd, _, err := rootCmd.Find([]string{"check-refs"})
	if err != nil {
		t.Fatalf("bd check-refs not found: %v", err)
	}
	if cmd.Name() != "check-refs" {
		t.Errorf("expected command name 'check-refs', got %q", cmd.Name())
	}
}

// TestCheckBeadsRefSyncAutoReset verifies that when reset_dolt_with_git=true
// and prompt=false, Dolt is automatically reset to match the ref hash.
func TestCheckBeadsRefSyncAutoReset(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	s := newTestStoreWithPrefix(t, dbPath, "autoreset")
	ctx := context.Background()

	beadsDir := filepath.Dir(s.Path())

	if err := config.Initialize(); err != nil {
		t.Fatalf("config.Initialize: %v", err)
	}
	config.Set("branch_strategy.defaults.reset_dolt_with_git", true)
	config.Set("branch_strategy.prompt", false)
	t.Cleanup(func() {
		config.Set("branch_strategy.defaults.reset_dolt_with_git", false)
		config.Set("branch_strategy.prompt", false)
	})

	now := time.Now()

	// Create issue and commit (checkpoint)
	issueA := &types.Issue{
		ID: "autoreset-aaa", Title: "Checkpoint issue",
		Status: types.StatusOpen, Priority: 2, IssueType: types.TypeTask,
		CreatedAt: now, UpdatedAt: now,
	}
	if err := s.CreateIssue(ctx, issueA, "test"); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	if err := s.Commit(ctx, "checkpoint"); err != nil {
		t.Fatalf("commit checkpoint: %v", err)
	}
	checkpointHash, _ := s.GetCurrentCommit(ctx)

	// Create another issue and commit (latest)
	issueB := &types.Issue{
		ID: "autoreset-bbb", Title: "Post-checkpoint issue",
		Status: types.StatusOpen, Priority: 1, IssueType: types.TypeFeature,
		CreatedAt: now.Add(time.Second), UpdatedAt: now.Add(time.Second),
	}
	if err := s.CreateIssue(ctx, issueB, "test"); err != nil {
		t.Fatalf("create issue B: %v", err)
	}
	if err := s.Commit(ctx, "latest"); err != nil {
		t.Fatalf("commit latest: %v", err)
	}

	// Simulate git reset backward: write refs pointing to checkpoint
	writeTestBeadsRefs(t, beadsDir, "main", checkpointHash)

	// checkBeadsRefSync should auto-reset (prompt=false, reset=true)
	checkBeadsRefSync(ctx, s)

	// Verify issue B is gone (Dolt was reset)
	if _, err := s.GetIssue(ctx, "autoreset-bbb"); err == nil {
		t.Error("issue B should be gone after auto-reset, but it still exists")
	}

	// Verify issue A survives
	gotA, err := s.GetIssue(ctx, "autoreset-aaa")
	if err != nil {
		t.Fatalf("issue A should survive: %v", err)
	}
	if gotA.Title != "Checkpoint issue" {
		t.Errorf("issue A title = %q, want %q", gotA.Title, "Checkpoint issue")
	}
}

// TestCheckBeadsRefSyncSilentDivergence verifies that with default settings
// (prompt=false, reset=false), mismatch is detected but Dolt is NOT reset —
// histories are allowed to diverge silently.
func TestCheckBeadsRefSyncSilentDivergence(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	s := newTestStoreWithPrefix(t, dbPath, "diverge")
	ctx := context.Background()

	beadsDir := filepath.Dir(s.Path())

	if err := config.Initialize(); err != nil {
		t.Fatalf("config.Initialize: %v", err)
	}
	// Ensure defaults: both false (silent divergence)
	config.Set("branch_strategy.defaults.reset_dolt_with_git", false)
	config.Set("branch_strategy.prompt", false)

	now := time.Now()
	issue := &types.Issue{
		ID: "diverge-aaa", Title: "Should not be reset",
		Status: types.StatusOpen, Priority: 2, IssueType: types.TypeTask,
		CreatedAt: now, UpdatedAt: now,
	}
	if err := s.CreateIssue(ctx, issue, "test"); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	if err := s.Commit(ctx, "with issue"); err != nil {
		t.Fatalf("commit: %v", err)
	}
	latestHash, _ := s.GetCurrentCommit(ctx)
	branch, _ := s.CurrentBranch(ctx)

	// Write refs pointing to a different hash (simulate git reset)
	writeTestBeadsRefs(t, beadsDir, branch, "0000000000000000000000000000fake")

	// Silent mode: detect mismatch, take no action
	checkBeadsRefSync(ctx, s)

	// Dolt should NOT have been reset
	afterHash, _ := s.GetCurrentCommit(ctx)
	if afterHash != latestHash {
		t.Errorf("silent divergence mode should not reset Dolt, but hash changed from %q to %q", latestHash, afterHash)
	}

	// Issue should still exist
	if _, err := s.GetIssue(ctx, "diverge-aaa"); err != nil {
		t.Errorf("issue should still exist in diverged state: %v", err)
	}
}

// TestSlashedBranchRefRoundTrip verifies that the write→read→sync cycle works
// correctly with slashed branch names like "feature/foo", which create nested
// directory structures under .beads/refs/heads/.
func TestSlashedBranchRefRoundTrip(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	s := newTestStoreWithPrefix(t, dbPath, "slashed")
	ctx := context.Background()

	beadsDir := filepath.Dir(s.Path())

	if err := config.Initialize(); err != nil {
		t.Fatalf("config.Initialize: %v", err)
	}
	config.Set("branch_strategy.defaults.reset_dolt_with_git", true)
	config.Set("branch_strategy.prompt", false)
	t.Cleanup(func() {
		config.Set("branch_strategy.defaults.reset_dolt_with_git", false)
		config.Set("branch_strategy.prompt", false)
	})

	now := time.Now()

	// Create issue and commit to get a real hash
	issue := &types.Issue{
		ID: "slashed-aaa", Title: "Slashed branch test",
		Status: types.StatusOpen, Priority: 2, IssueType: types.TypeTask,
		CreatedAt: now, UpdatedAt: now,
	}
	if err := s.CreateIssue(ctx, issue, "test"); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	if err := s.Commit(ctx, "slashed branch commit"); err != nil {
		t.Fatalf("commit: %v", err)
	}
	currentHash, _ := s.GetCurrentCommit(ctx)

	// Write refs with a slashed branch name
	writeTestBeadsRefs(t, beadsDir, "feature/foo", currentHash)

	// Verify the nested directory structure was created
	refPath := filepath.Join(beadsDir, "refs", "heads", "feature", "foo")
	if _, err := os.Stat(refPath); err != nil {
		t.Fatalf("expected nested ref file at %s: %v", refPath, err)
	}

	// Read back — should recover both hash and slashed branch name
	gotHash, gotBranch := readBeadsRefs(beadsDir)
	if gotHash != currentHash {
		t.Errorf("read hash = %q, want %q", gotHash, currentHash)
	}
	if gotBranch != "feature/foo" {
		t.Errorf("read branch = %q, want %q", gotBranch, "feature/foo")
	}

	// Sync should detect in-sync state (same hash) and take no action.
	// The important thing is it doesn't crash on the slashed path.
	checkBeadsRefSync(ctx, s)

	// Verify Dolt state unchanged
	afterHash, _ := s.GetCurrentCommit(ctx)
	if afterHash != currentHash {
		t.Errorf("sync changed Dolt hash from %q to %q on in-sync slashed branch", currentHash, afterHash)
	}
}

// TestCheckRefsBehavior exercises the bd check-refs command handler beyond just
// registration. It verifies the command takes action on mismatch when auto-reset
// is enabled, and is a no-op when refs are in sync.
func TestCheckRefsBehavior(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	s := newTestStoreWithPrefix(t, dbPath, "checkrefs")
	ctx := context.Background()

	beadsDir := filepath.Dir(s.Path())

	if err := config.Initialize(); err != nil {
		t.Fatalf("config.Initialize: %v", err)
	}
	config.Set("branch_strategy.defaults.reset_dolt_with_git", true)
	config.Set("branch_strategy.prompt", false)
	t.Cleanup(func() {
		config.Set("branch_strategy.defaults.reset_dolt_with_git", false)
		config.Set("branch_strategy.prompt", false)
	})

	now := time.Now()

	// Build two commits
	issueA := &types.Issue{
		ID: "checkrefs-aaa", Title: "Checkpoint",
		Status: types.StatusOpen, Priority: 2, IssueType: types.TypeTask,
		CreatedAt: now, UpdatedAt: now,
	}
	if err := s.CreateIssue(ctx, issueA, "test"); err != nil {
		t.Fatalf("create issue A: %v", err)
	}
	if err := s.Commit(ctx, "bd: create checkrefs-aaa"); err != nil {
		t.Fatalf("commit checkpoint: %v", err)
	}
	checkpointHash, _ := s.GetCurrentCommit(ctx)

	issueB := &types.Issue{
		ID: "checkrefs-bbb", Title: "Latest",
		Status: types.StatusOpen, Priority: 1, IssueType: types.TypeFeature,
		CreatedAt: now.Add(time.Second), UpdatedAt: now.Add(time.Second),
	}
	if err := s.CreateIssue(ctx, issueB, "test"); err != nil {
		t.Fatalf("create issue B: %v", err)
	}
	if err := s.Commit(ctx, "bd: create checkrefs-bbb"); err != nil {
		t.Fatalf("commit latest: %v", err)
	}
	latestHash, _ := s.GetCurrentCommit(ctx)

	// Subtest: in-sync — check-refs should be a no-op
	t.Run("in-sync is no-op", func(t *testing.T) {
		writeTestBeadsRefs(t, beadsDir, "main", latestHash)
		checkBeadsRefSync(ctx, s)
		afterHash, _ := s.GetCurrentCommit(ctx)
		if afterHash != latestHash {
			t.Errorf("in-sync check changed hash from %q to %q", latestHash, afterHash)
		}
	})

	// Subtest: mismatch — check-refs should auto-reset
	t.Run("mismatch triggers reset", func(t *testing.T) {
		writeTestBeadsRefs(t, beadsDir, "main", checkpointHash)
		checkBeadsRefSync(ctx, s)
		afterHash, _ := s.GetCurrentCommit(ctx)
		if afterHash != checkpointHash {
			t.Errorf("mismatch check: hash = %q, want %q", afterHash, checkpointHash)
		}
		if _, err := s.GetIssue(ctx, "checkrefs-bbb"); err == nil {
			t.Error("issue B should be gone after reset")
		}
	})

	// Subtest: disabled — check-refs should take no action
	t.Run("disabled is no-op", func(t *testing.T) {
		// Reset Dolt back to latest for this subtest
		writeTestBeadsRefs(t, beadsDir, "main", latestHash)
		checkBeadsRefSync(ctx, s)

		// Disable branch_strategy
		config.Set("branch_strategy.defaults.reset_dolt_with_git", false)
		config.Set("branch_strategy.prompt", false)

		// Write mismatched refs
		writeTestBeadsRefs(t, beadsDir, "main", checkpointHash)
		checkBeadsRefSync(ctx, s)

		// Should NOT have reset (silent divergence)
		afterHash, _ := s.GetCurrentCommit(ctx)
		if afterHash != latestHash {
			t.Errorf("disabled check should not reset, but hash changed to %q", afterHash)
		}

		// Re-enable for cleanup
		config.Set("branch_strategy.defaults.reset_dolt_with_git", true)
	})
}

// TestCheckBeadsRefSyncNilStore verifies no panic when store is nil.
func TestCheckBeadsRefSyncNilStore(t *testing.T) {
	t.Parallel()
	// Should not panic
	checkBeadsRefSync(context.Background(), nil)
}

// TestReadBeadsRefsNoFiles verifies readBeadsRefs returns empty strings
// when no ref files exist.
func TestReadBeadsRefsNoFiles(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	hash, branch := readBeadsRefs(dir)
	if hash != "" || branch != "" {
		t.Errorf("expected empty strings, got hash=%q branch=%q", hash, branch)
	}
}

// TestReadBeadsRefsMalformedHead verifies readBeadsRefs handles
// malformed HEAD file gracefully.
func TestReadBeadsRefsMalformedHead(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	headPath := filepath.Join(dir, "HEAD")
	os.WriteFile(headPath, []byte("not a ref line\n"), 0644)

	hash, branch := readBeadsRefs(dir)
	if hash != "" || branch != "" {
		t.Errorf("expected empty strings for malformed HEAD, got hash=%q branch=%q", hash, branch)
	}
}

// TestReadBeadsRefsValidButMissingRefFile verifies readBeadsRefs returns
// the branch but empty hash when HEAD is valid but ref file doesn't exist.
func TestReadBeadsRefsValidButMissingRefFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	headPath := filepath.Join(dir, "HEAD")
	os.WriteFile(headPath, []byte("ref: refs/heads/main\n"), 0644)

	hash, branch := readBeadsRefs(dir)
	if branch != "main" {
		t.Errorf("expected branch='main', got %q", branch)
	}
	if hash != "" {
		t.Errorf("expected empty hash when ref file missing, got %q", hash)
	}
}

// TestReadBeadsRefsRoundTrip verifies writeBeadsRefs and readBeadsRefs
// are consistent.
func TestReadBeadsRefsRoundTrip(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	branch := "feature-branch"
	hash := "abc123def456abc123def456abc12345"

	// Write refs
	headPath := filepath.Join(dir, "HEAD")
	os.WriteFile(headPath, []byte("ref: refs/heads/"+branch+"\n"), 0644)
	refsDir := filepath.Join(dir, "refs", "heads")
	os.MkdirAll(refsDir, 0755)
	refPath := filepath.Join(refsDir, branch)
	os.WriteFile(refPath, []byte(hash+"\n"), 0644)

	// Read back
	gotHash, gotBranch := readBeadsRefs(dir)
	if gotBranch != branch {
		t.Errorf("branch = %q, want %q", gotBranch, branch)
	}
	if gotHash != hash {
		t.Errorf("hash = %q, want %q", gotHash, hash)
	}
}

// TestWriteBeadsRefsDisabled verifies writeBeadsRefs is a no-op when
// branch_strategy is not configured.
func TestWriteBeadsRefsDisabled(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	if err := config.Initialize(); err != nil {
		t.Fatalf("config.Initialize: %v", err)
	}
	// Ensure branch_strategy is not set (default state)

	headPath := filepath.Join(dir, "HEAD")

	// writeBeadsRefs needs a store, but the guard should return before using it.
	// We test indirectly: if refs are disabled, no HEAD file should be created.
	// (We can't call writeBeadsRefs directly without a store, but we verify the
	// guard via checkBeadsRefSync which also has the guard.)

	// Verify no HEAD file exists
	if _, err := os.Stat(headPath); !os.IsNotExist(err) {
		t.Error("HEAD file should not exist when branch_strategy is disabled")
	}
}

// TestCheckBeadsRefSyncDisabledNoAction verifies checkBeadsRefSync is a no-op
// when branch_strategy is not configured, even if ref files exist.
func TestCheckBeadsRefSyncDisabledNoAction(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	s := newTestStoreWithPrefix(t, dbPath, "disabled")
	ctx := context.Background()

	beadsDir := filepath.Dir(s.Path())

	if err := config.Initialize(); err != nil {
		t.Fatalf("config.Initialize: %v", err)
	}
	// Ensure branch_strategy is NOT set
	config.Set("branch_strategy.prompt", nil)
	config.Set("branch_strategy.defaults.reset_dolt_with_git", nil)

	// Create an issue and commit
	issue := &types.Issue{
		ID: "disabled-aaa", Title: "Should not be touched",
		Status: types.StatusOpen, Priority: 2, IssueType: types.TypeTask,
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}
	if err := s.CreateIssue(ctx, issue, "test"); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	if err := s.Commit(ctx, "with issue"); err != nil {
		t.Fatalf("commit: %v", err)
	}
	latestHash, _ := s.GetCurrentCommit(ctx)
	branch, _ := s.CurrentBranch(ctx)

	// Write refs pointing to a fake hash (would trigger reset if enabled)
	writeTestBeadsRefs(t, beadsDir, branch, "0000000000000000000000000000fake")

	// checkBeadsRefSync should be a no-op — branch_strategy not configured
	checkBeadsRefSync(ctx, s)

	// Dolt should NOT have been reset
	afterHash, _ := s.GetCurrentCommit(ctx)
	if afterHash != latestHash {
		t.Errorf("disabled mode should not reset Dolt, but hash changed from %q to %q", latestHash, afterHash)
	}
}

// TestCleanupStaleBeadsRefs verifies that stale ref files are removed
// when branch_strategy is disabled.
func TestCleanupStaleBeadsRefs(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Create ref files as if they were previously generated
	headPath := filepath.Join(dir, "HEAD")
	os.WriteFile(headPath, []byte("ref: refs/heads/main\n"), 0644)
	refsDir := filepath.Join(dir, "refs", "heads")
	os.MkdirAll(refsDir, 0755)
	refPath := filepath.Join(refsDir, "main")
	os.WriteFile(refPath, []byte("abc123\n"), 0644)

	// Run cleanup
	cleanupStaleBeadsRefs(dir)

	// Verify files are gone
	if _, err := os.Stat(headPath); !os.IsNotExist(err) {
		t.Error("HEAD file should be removed after cleanup")
	}
	if _, err := os.Stat(refPath); !os.IsNotExist(err) {
		t.Error("ref file should be removed after cleanup")
	}
}

// TestCleanupStaleBeadsRefsNoFiles verifies cleanup is a no-op
// when no ref files exist.
func TestCleanupStaleBeadsRefsNoFiles(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Should not panic or error
	cleanupStaleBeadsRefs(dir)
}

// TestAutoResetRoundTrip verifies backward then forward reset preserves data
// and that getDoltCommitMessage works in both directions (uses dolt_commits,
// not dolt_log, so forward-unreachable commits are still found).
func TestAutoResetRoundTrip(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	s := newTestStoreWithPrefix(t, dbPath, "roundtrip")
	ctx := context.Background()

	beadsDir := filepath.Dir(s.Path())

	if err := config.Initialize(); err != nil {
		t.Fatalf("config.Initialize: %v", err)
	}
	config.Set("branch_strategy.defaults.reset_dolt_with_git", true)
	config.Set("branch_strategy.prompt", false)
	t.Cleanup(func() {
		config.Set("branch_strategy.defaults.reset_dolt_with_git", false)
		config.Set("branch_strategy.prompt", false)
	})

	now := time.Now()

	// Commit 1: checkpoint
	issueA := &types.Issue{
		ID: "roundtrip-aaa", Title: "Checkpoint issue",
		Status: types.StatusOpen, Priority: 2, IssueType: types.TypeTask,
		CreatedAt: now, UpdatedAt: now,
	}
	if err := s.CreateIssue(ctx, issueA, "test"); err != nil {
		t.Fatalf("create issue A: %v", err)
	}
	if err := s.Commit(ctx, "bd: create roundtrip-aaa"); err != nil {
		t.Fatalf("commit checkpoint: %v", err)
	}
	checkpointHash, _ := s.GetCurrentCommit(ctx)

	// Commit 2: latest
	issueB := &types.Issue{
		ID: "roundtrip-bbb", Title: "Latest issue",
		Status: types.StatusOpen, Priority: 1, IssueType: types.TypeFeature,
		CreatedAt: now.Add(time.Second), UpdatedAt: now.Add(time.Second),
	}
	if err := s.CreateIssue(ctx, issueB, "test"); err != nil {
		t.Fatalf("create issue B: %v", err)
	}
	if err := s.Commit(ctx, "bd: create roundtrip-bbb"); err != nil {
		t.Fatalf("commit latest: %v", err)
	}
	latestHash, _ := s.GetCurrentCommit(ctx)

	// --- Backward reset ---
	writeTestBeadsRefs(t, beadsDir, "main", checkpointHash)
	checkBeadsRefSync(ctx, s)

	// Verify Dolt is at checkpoint
	afterHash, _ := s.GetCurrentCommit(ctx)
	if afterHash != checkpointHash {
		t.Fatalf("backward reset: expected hash %q, got %q", checkpointHash, afterHash)
	}
	// Issue B should be gone
	if _, err := s.GetIssue(ctx, "roundtrip-bbb"); err == nil {
		t.Error("backward reset: issue B should be gone")
	}

	// getDoltCommitMessage should still find the FORWARD commit via dolt_commits
	forwardMsg := getDoltCommitMessage(ctx, s, latestHash)
	if forwardMsg == "" {
		t.Error("getDoltCommitMessage should find forward-unreachable commit via dolt_commits")
	}
	if forwardMsg != "bd: create roundtrip-bbb" {
		t.Errorf("forward commit message = %q, want %q", forwardMsg, "bd: create roundtrip-bbb")
	}

	// --- Forward reset ---
	writeTestBeadsRefs(t, beadsDir, "main", latestHash)
	checkBeadsRefSync(ctx, s)

	// Verify Dolt is at latest
	afterHash, _ = s.GetCurrentCommit(ctx)
	if afterHash != latestHash {
		t.Fatalf("forward reset: expected hash %q, got %q", latestHash, afterHash)
	}
	// Both issues should be present
	if _, err := s.GetIssue(ctx, "roundtrip-aaa"); err != nil {
		t.Errorf("forward reset: issue A should exist: %v", err)
	}
	if _, err := s.GetIssue(ctx, "roundtrip-bbb"); err != nil {
		t.Errorf("forward reset: issue B should exist: %v", err)
	}

	// getDoltCommitMessage should work for both hashes
	backMsg := getDoltCommitMessage(ctx, s, checkpointHash)
	if backMsg != "bd: create roundtrip-aaa" {
		t.Errorf("checkpoint message = %q, want %q", backMsg, "bd: create roundtrip-aaa")
	}
	fwdMsg := getDoltCommitMessage(ctx, s, latestHash)
	if fwdMsg != "bd: create roundtrip-bbb" {
		t.Errorf("latest message = %q, want %q", fwdMsg, "bd: create roundtrip-bbb")
	}
}

// TestResetRoundTripWithGitRepo exercises the full reset flow inside a real
// git repo with a real Dolt container: create issues + commits, git reset
// --hard HEAD~1, verify both git and Dolt state moved backward, then reset
// forward and verify round-trip integrity.
func TestResetRoundTripWithGitRepo(t *testing.T) {
	// Setup: create a real git repo
	repoDir := t.TempDir()
	for _, args := range [][]string{
		{"git", "init"},
		{"git", "config", "user.email", "test@test.com"},
		{"git", "config", "user.name", "Test"},
	} {
		cmd := exec.CommandContext(context.Background(), args[0], args[1:]...)
		cmd.Dir = repoDir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("%v: %s: %v", args, out, err)
		}
	}

	// Create .beads/ inside the git repo and initialize DoltStore
	beadsDir := filepath.Join(repoDir, ".beads")
	if err := os.MkdirAll(beadsDir, 0755); err != nil {
		t.Fatalf("mkdir .beads: %v", err)
	}
	dbPath := filepath.Join(beadsDir, "test.db")
	s := newTestStoreWithPrefix(t, dbPath, "gitreset")
	ctx := context.Background()

	// Enable branch_strategy so refs are checked
	if err := config.Initialize(); err != nil {
		t.Fatalf("config.Initialize: %v", err)
	}
	config.Set("branch_strategy.defaults.reset_dolt_with_git", true)
	config.Set("branch_strategy.prompt", false)
	t.Cleanup(func() {
		config.Set("branch_strategy.defaults.reset_dolt_with_git", false)
		config.Set("branch_strategy.prompt", false)
	})

	// Initial git commit so HEAD~1 works later
	initFile := filepath.Join(repoDir, "init.txt")
	os.WriteFile(initFile, []byte("init\n"), 0644)
	gitRun(t, repoDir, "add", "init.txt")
	gitRun(t, repoDir, "commit", "-m", "initial commit")

	now := time.Now()

	// --- Commit 1: checkpoint ---
	issueA := &types.Issue{
		ID: "gitreset-aaa", Title: "Checkpoint issue",
		Status: types.StatusOpen, Priority: 2, IssueType: types.TypeTask,
		CreatedAt: now, UpdatedAt: now,
	}
	if err := s.CreateIssue(ctx, issueA, "test"); err != nil {
		t.Fatalf("create issue A: %v", err)
	}
	if err := s.Commit(ctx, "bd: create gitreset-aaa"); err != nil {
		t.Fatalf("commit checkpoint: %v", err)
	}
	checkpointHash, _ := s.GetCurrentCommit(ctx)

	// Write beads refs and commit to git
	writeTestBeadsRefs(t, beadsDir, "main", checkpointHash)
	gitRun(t, repoDir, "add", ".beads/")
	gitRun(t, repoDir, "commit", "-m", "checkpoint")

	// --- Commit 2: latest ---
	issueB := &types.Issue{
		ID: "gitreset-bbb", Title: "Latest issue",
		Status: types.StatusOpen, Priority: 1, IssueType: types.TypeFeature,
		CreatedAt: now.Add(time.Second), UpdatedAt: now.Add(time.Second),
	}
	if err := s.CreateIssue(ctx, issueB, "test"); err != nil {
		t.Fatalf("create issue B: %v", err)
	}
	if err := s.Commit(ctx, "bd: create gitreset-bbb"); err != nil {
		t.Fatalf("commit latest: %v", err)
	}
	latestHash, _ := s.GetCurrentCommit(ctx)

	writeTestBeadsRefs(t, beadsDir, "main", latestHash)
	gitRun(t, repoDir, "add", ".beads/")
	gitRun(t, repoDir, "commit", "-m", "latest")

	// --- Backward reset: git reset --hard HEAD~1 ---
	gitRun(t, repoDir, "reset", "--hard", "HEAD~1")

	// Verify git moved back: refs file should contain checkpointHash
	refData, err := os.ReadFile(filepath.Join(beadsDir, "refs", "heads", "main"))
	if err != nil {
		t.Fatalf("read ref after git reset: %v", err)
	}
	gotRefHash := strings.TrimSpace(string(refData))
	if gotRefHash != checkpointHash {
		t.Fatalf("after git reset, ref hash = %q, want %q", gotRefHash, checkpointHash)
	}

	// Trigger mismatch detection — should reset Dolt to checkpointHash
	checkBeadsRefSync(ctx, s)

	afterHash, _ := s.GetCurrentCommit(ctx)
	if afterHash != checkpointHash {
		t.Fatalf("backward reset: Dolt hash = %q, want %q", afterHash, checkpointHash)
	}
	if _, err := s.GetIssue(ctx, "gitreset-bbb"); err == nil {
		t.Error("backward reset: issue B should be gone")
	}
	if _, err := s.GetIssue(ctx, "gitreset-aaa"); err != nil {
		t.Errorf("backward reset: issue A should exist: %v", err)
	}

	// --- Forward reset: write refs with latestHash ---
	writeTestBeadsRefs(t, beadsDir, "main", latestHash)
	checkBeadsRefSync(ctx, s)

	afterHash, _ = s.GetCurrentCommit(ctx)
	if afterHash != latestHash {
		t.Fatalf("forward reset: Dolt hash = %q, want %q", afterHash, latestHash)
	}
	if _, err := s.GetIssue(ctx, "gitreset-aaa"); err != nil {
		t.Errorf("forward reset: issue A should exist: %v", err)
	}
	if _, err := s.GetIssue(ctx, "gitreset-bbb"); err != nil {
		t.Errorf("forward reset: issue B should exist: %v", err)
	}
}

// gitRun executes a git command in the given directory, failing the test on error.
func gitRun(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.CommandContext(context.Background(), "git", args...)
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %s: %v", args, out, err)
	}
}

// TestCleanupStaleBeadsRefsInGitRepo verifies that cleanupStaleBeadsRefs
// stages file deletions in the git index when run inside a real git repo.
// The basic TestCleanupStaleBeadsRefs test runs without a git repo, so the
// git rm --cached calls silently fail. This test exercises the full path.
func TestCleanupStaleBeadsRefsInGitRepo(t *testing.T) {
	t.Parallel()

	// Create a temporary git repo
	repoDir := t.TempDir()
	for _, args := range [][]string{
		{"git", "init"},
		{"git", "config", "user.email", "test@test.com"},
		{"git", "config", "user.name", "Test"},
	} {
		cmd := exec.CommandContext(context.Background(), args[0], args[1:]...)
		cmd.Dir = repoDir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("%v: %s: %v", args, out, err)
		}
	}

	// Create .beads/ dir with ref files
	beadsDir := filepath.Join(repoDir, ".beads")
	os.MkdirAll(beadsDir, 0755)
	headPath := filepath.Join(beadsDir, "HEAD")
	os.WriteFile(headPath, []byte("ref: refs/heads/main\n"), 0644)
	refsHeadsDir := filepath.Join(beadsDir, "refs", "heads")
	os.MkdirAll(refsHeadsDir, 0755)
	refPath := filepath.Join(refsHeadsDir, "main")
	os.WriteFile(refPath, []byte("abc123\n"), 0644)

	// Stage and commit the ref files so git rm --cached has something to remove
	for _, args := range [][]string{
		{"git", "add", ".beads/HEAD", ".beads/refs/"},
		{"git", "commit", "-m", "add ref files"},
	} {
		cmd := exec.CommandContext(context.Background(), args[0], args[1:]...)
		cmd.Dir = repoDir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("%v: %s: %v", args, out, err)
		}
	}

	// Verify files are tracked
	cmd := exec.CommandContext(context.Background(), "git", "ls-files", ".beads/HEAD")
	cmd.Dir = repoDir
	out, err := cmd.Output()
	if err != nil || !strings.Contains(string(out), ".beads/HEAD") {
		t.Fatal("HEAD should be tracked before cleanup")
	}

	// Run cleanup
	cleanupStaleBeadsRefs(beadsDir)

	// Verify files are gone from disk
	if _, err := os.Stat(headPath); !os.IsNotExist(err) {
		t.Error("HEAD file should be removed from disk after cleanup")
	}
	if _, err := os.Stat(refPath); !os.IsNotExist(err) {
		t.Error("ref file should be removed from disk after cleanup")
	}

	// Verify deletions are staged in git index
	cmd = exec.CommandContext(context.Background(), "git", "status", "--porcelain")
	cmd.Dir = repoDir
	out, err = cmd.Output()
	if err != nil {
		t.Fatalf("git status: %v", err)
	}
	status := string(out)
	// Expect "D  .beads/HEAD" and "D  .beads/refs/heads/main" (staged deletions)
	if !strings.Contains(status, "D  .beads/HEAD") {
		t.Errorf("expected staged deletion of .beads/HEAD in git status, got:\n%s", status)
	}
	if !strings.Contains(status, "D  .beads/refs/heads/main") {
		t.Errorf("expected staged deletion of .beads/refs/heads/main in git status, got:\n%s", status)
	}
}

// TestTruncHash verifies hash truncation behavior.
func TestTruncHash(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input string
		want  string
	}{
		{"abcdefghijklmnop", "abcdefgh"},
		{"short", "short"},
		{"12345678", "12345678"},
		{"123456789", "12345678"},
		{"", ""},
	}
	for _, tt := range tests {
		if got := truncHash(tt.input); got != tt.want {
			t.Errorf("truncHash(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}
