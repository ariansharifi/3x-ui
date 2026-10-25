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

// The subscription index trusts this counter to notice every edit, from any writer, but not the traffic ticks.
func TestInboundsVersionTracksInboundEdits(t *testing.T) {
	initTestDB(t)
	version := func() int64 {
		t.Helper()
		v, err := InboundsVersion()
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	step := func(name string, wantMove bool, change func() error) {
		t.Helper()
		before := version()
		if err := change(); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if moved := version() != before; moved != wantMove {
			t.Errorf("%s: version moved = %v, want %v", name, moved, wantMove)
		}
	}
	inbound := &model.Inbound{UserId: 1, Port: 44001, Protocol: model.VLESS, Tag: "inbound-44001", Enable: true, Settings: `{"clients":[]}`}
	step("insert", true, func() error { return GetDB().Create(inbound).Error })
	step("traffic counters", false, func() error {
		return GetDB().Model(&model.Inbound{}).Where("id = ?", inbound.Id).
			Updates(map[string]any{"up": gorm.Expr("up + 1"), "down": gorm.Expr("down + 1"), "all_time": gorm.Expr("all_time + 2")}).Error
	})
	step("settings", true, func() error {
		return GetDB().Model(&model.Inbound{}).Where("id = ?", inbound.Id).Update("settings", `{"clients":[{"id":"x","email":"a"}]}`).Error
	})
	step("enable flag", true, func() error {
		return GetDB().Model(&model.Inbound{}).Where("id = ?", inbound.Id).Update("enable", false).Error
	})
	step("full save", true, func() error { return GetDB().Save(inbound).Error })
	step("raw SQL from another tool", true, func() error {
		return GetDB().Exec("UPDATE inbounds SET remark = 'edited' WHERE id = ?", inbound.Id).Error
	})
	step("delete", true, func() error { return GetDB().Delete(&model.Inbound{}, inbound.Id).Error })
}

// An imported database restarts the counter, so the epoch must tell the two databases apart.
func TestEpochMovesOnEveryInitDB(t *testing.T) {
	initTestDB(t)
	first := Epoch()
	initTestDB(t)
	if Epoch() == first {
		t.Error("epoch did not move when InitDB opened another database")
	}
}
