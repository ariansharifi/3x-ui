package database

import (
	"path/filepath"
	"testing"

	"github.com/mhsanaei/3x-ui/v2/database/model"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func initTestDB(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("XUI_DB_FOLDER", dir)
	dbPath := filepath.Join(dir, "x-ui.db")
	if err := InitDB(dbPath); err != nil {
		t.Fatalf("InitDB: %v", err)
	}
	t.Cleanup(func() { CloseDB() })
	return dbPath
}

// Readers must not block the writer: a big inbound list read used to stall the traffic job into "database is locked".
func TestInitDBUsesWAL(t *testing.T) {
	initTestDB(t)
	var mode string
	if err := GetDB().Raw("PRAGMA journal_mode").Scan(&mode).Error; err != nil {
		t.Fatal(err)
	}
	if mode != "wal" {
		t.Errorf("journal_mode = %q, want wal", mode)
	}
}

// In WAL mode recent commits can sit in the -wal file, so a backup must not be a copy of the main file alone.
func TestSnapshotHoldsUncheckpointedCommits(t *testing.T) {
	dbPath := initTestDB(t)
	if err := GetDB().Create(&model.Setting{Key: "snapshot-probe", Value: "1"}).Error; err != nil {
		t.Fatal(err)
	}
	snapshot, cleanup, err := Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	defer cleanup()
	if filepath.Base(snapshot) != filepath.Base(dbPath) {
		t.Errorf("snapshot named %q, want %q", filepath.Base(snapshot), filepath.Base(dbPath))
	}

	copyDB, err := gorm.Open(sqlite.Open(snapshot), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, _ := copyDB.DB()
	defer sqlDB.Close()
	var count int64
	copyDB.Model(&model.Setting{}).Where("key = ?", "snapshot-probe").Count(&count)
	if count != 1 {
		t.Error("snapshot is missing a committed row")
	}
}
