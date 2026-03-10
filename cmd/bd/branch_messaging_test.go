//go:build cgo

package main

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/steveyegge/beads/internal/types"
)

func TestCherryPickToTarget_ConflictCleanup(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	testDB := filepath.Join(tmpDir, ".beads", "beads.db")
	s := newTestStoreIsolatedDB(t, testDB, "cptest")
	ctx := context.Background()

	// Step 1: Create an issue on main and commit, so we have a base state.
	issue := &types.Issue{
		ID:        "cptest-1",
		Title:     "Conflict test issue",
		Status:    types.StatusOpen,
		Priority:  2,
		IssueType: types.TypeTask,
		CreatedAt: time.Now(),
	}
	if err := s.CreateIssue(ctx, issue, "test"); err != nil {
		t.Fatalf("create issue on main: %v", err)
	}
	if err := s.Commit(ctx, "initial issue on main"); err != nil {
		t.Fatalf("commit on main: %v", err)
	}

	// Step 2: Register a branch with merge-with-branch strategy (creates Dolt branch).
	if err := s.RegisterBranch(ctx, "feature-cp", "merge-with-branch"); err != nil {
		t.Fatalf("register branch: %v", err)
	}

	// Step 3: On the branch, modify the issue to create a conflicting state.
	if err := s.Checkout(ctx, "feature-cp"); err != nil {
		t.Fatalf("checkout feature-cp: %v", err)
	}
	if err := s.UpdateIssue(ctx, "cptest-1", map[string]interface{}{
		"title":    "Modified on feature branch",
		"priority": 0,
	}, "test"); err != nil {
		t.Fatalf("update issue on branch: %v", err)
	}
	if err := s.Commit(ctx, "modify issue on feature-cp"); err != nil {
		t.Fatalf("commit on feature-cp: %v", err)
	}

	// Step 4: On main, modify the same issue differently and get the commit hash.
	if err := s.Checkout(ctx, "main"); err != nil {
		t.Fatalf("checkout main: %v", err)
	}
	if err := s.UpdateIssue(ctx, "cptest-1", map[string]interface{}{
		"title":    "Modified on main",
		"priority": 1,
	}, "test"); err != nil {
		t.Fatalf("update issue on main: %v", err)
	}
	if err := s.Commit(ctx, "modify issue on main"); err != nil {
		t.Fatalf("commit on main: %v", err)
	}

	// Get the commit hash from main's HEAD.
	var commitHash string
	connStr := s.ConnectionString()
	if connStr == "" {
		t.Fatal("no connection string available")
	}
	mainDB, err := sql.Open("mysql", connStr)
	if err != nil {
		t.Fatalf("open main connection: %v", err)
	}
	defer mainDB.Close()
	mainDB.SetMaxOpenConns(1)
	mainDB.SetMaxIdleConns(1)

	if err := mainDB.QueryRowContext(ctx, "SELECT HASHOF('HEAD')").Scan(&commitHash); err != nil {
		t.Fatalf("get HEAD hash: %v", err)
	}

	// Step 5: Open a separate connection and cherry-pick main's commit onto feature-cp.
	deliveryDB, err := openMergeConnection(s)
	if err != nil {
		t.Fatalf("open merge connection: %v", err)
	}
	defer deliveryDB.Close()

	cpErr := cherryPickToTarget(ctx, deliveryDB, s, "feature-cp", commitHash)

	// Step 6: Assert cherry-pick returned a conflict error.
	if cpErr == nil {
		t.Fatal("expected cherry-pick conflict error, got nil")
	}
	if !strings.Contains(cpErr.Error(), "cherry-pick") {
		t.Fatalf("expected error to mention cherry-pick, got: %v", cpErr)
	}

	// Step 7: Verify the branch working set is clean after abort.
	// Query dolt_status on the delivery connection (still checked out to feature-cp).
	var dirtyCount int
	if err := deliveryDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM dolt_status").Scan(&dirtyCount); err != nil {
		t.Fatalf("query dolt_status: %v", err)
	}
	if dirtyCount != 0 {
		t.Errorf("expected clean working set after cherry-pick abort, got %d dirty tables", dirtyCount)
	}
}
