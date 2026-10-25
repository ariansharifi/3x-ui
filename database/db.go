// Package database provides database initialization, migration, and management utilities
// for the 3x-ui panel using GORM with SQLite.
package database

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"log"
	"os"
	"path"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/mhsanaei/3x-ui/v2/config"
	"github.com/mhsanaei/3x-ui/v2/database/model"
	"github.com/mhsanaei/3x-ui/v2/util/crypto"
	"github.com/mhsanaei/3x-ui/v2/xray"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var db *gorm.DB

// epoch moves each time InitDB opens a database. A cache keyed on InboundsVersion pairs it with the
// epoch, because an imported database brings its own counter, which may be lower than the one cached.
var epoch atomic.Int64

const (
	defaultUsername = "admin"
	defaultPassword = "admin"
)

func initModels() error {
	models := []any{
		&model.User{},
		&model.Inbound{},
		&model.OutboundTraffics{},
		&model.Setting{},
		&model.InboundClientIps{},
		&xray.ClientTraffic{},
		&model.HistoryOfSeeders{},
		&model.CustomGeoResource{},
	}
	for _, model := range models {
		if err := db.AutoMigrate(model); err != nil {
			log.Printf("Error auto migrating model: %v", err)
			return err
		}
	}
	return nil
}

// initUser creates a default admin user if the users table is empty.
func initUser() error {
	empty, err := isTableEmpty("users")
	if err != nil {
		log.Printf("Error checking if users table is empty: %v", err)
		return err
	}
	if empty {
		hashedPassword, err := crypto.HashPasswordAsBcrypt(defaultPassword)

		if err != nil {
			log.Printf("Error hashing default password: %v", err)
			return err
		}

		user := &model.User{
			Username: defaultUsername,
			Password: hashedPassword,
		}
		return db.Create(user).Error
	}
	return nil
}

// runSeeders migrates user passwords to bcrypt and records seeder execution to prevent re-running.
func runSeeders(isUsersEmpty bool) error {
	empty, err := isTableEmpty("history_of_seeders")
	if err != nil {
		log.Printf("Error checking if users table is empty: %v", err)
		return err
	}

	if empty && isUsersEmpty {
		hashSeeder := &model.HistoryOfSeeders{
			SeederName: "UserPasswordHash",
		}
		return db.Create(hashSeeder).Error
	} else {
		var seedersHistory []string
		db.Model(&model.HistoryOfSeeders{}).Pluck("seeder_name", &seedersHistory)

		if !slices.Contains(seedersHistory, "UserPasswordHash") && !isUsersEmpty {
			var users []model.User
			db.Find(&users)

			for _, user := range users {
				hashedPassword, err := crypto.HashPasswordAsBcrypt(user.Password)
				if err != nil {
					log.Printf("Error hashing password for user '%s': %v", user.Username, err)
					return err
				}
				db.Model(&user).Update("password", hashedPassword)
			}

			hashSeeder := &model.HistoryOfSeeders{
				SeederName: "UserPasswordHash",
			}
			return db.Create(hashSeeder).Error
		}
	}

	return nil
}

// isTableEmpty returns true if the named table contains zero rows.
func isTableEmpty(tableName string) (bool, error) {
	var count int64
	err := db.Table(tableName).Count(&count).Error
	return count == 0, err
}

// InitDB sets up the database connection, migrates models, and runs seeders.
func InitDB(dbPath string) error {
	dir := path.Dir(dbPath)
	err := os.MkdirAll(dir, fs.ModePerm)
	if err != nil {
		return err
	}

	var gormLogger logger.Interface

	if config.IsDebug() {
		gormLogger = logger.Default
	} else {
		gormLogger = logger.Discard
	}

	c := &gorm.Config{
		Logger: gormLogger,
	}
	db, err = gorm.Open(sqlite.Open(sqliteDSN(dbPath)), c)
	if err != nil {
		return err
	}
	epoch.Add(1)

	if err := initModels(); err != nil {
		return err
	}
	if err := initInboundsVersion(); err != nil {
		return err
	}

	isUsersEmpty, err := isTableEmpty("users")
	if err != nil {
		return err
	}

	if err := initUser(); err != nil {
		return err
	}
	return runSeeders(isUsersEmpty)
}

// inboundsUnwatchedColumns never move the version: the traffic counters change on every tick and say
// nothing about how to reach an inbound, and id never changes.
var inboundsUnwatchedColumns = []string{"id", "up", "down", "all_time"}

// initInboundsVersion keeps a counter that triggers move on any change to an inbound except its traffic
// counters, so a cache built from inbounds can tell it is stale whoever wrote: this process, the x-ui
// CLI or a hand edit. The update trigger is recreated on every start so it covers columns that later
// migrations add.
func initInboundsVersion() error {
	var columns []string
	if err := db.Raw("SELECT name FROM pragma_table_info('inbounds')").Scan(&columns).Error; err != nil {
		return err
	}
	var watched []string
	for _, c := range columns {
		if !slices.Contains(inboundsUnwatchedColumns, c) {
			watched = append(watched, `"`+c+`"`)
		}
	}
	const bump = "BEGIN UPDATE inbounds_version SET version = version + 1 WHERE id = 1; END"
	return db.Transaction(func(tx *gorm.DB) error {
		for _, stmt := range []string{
			"CREATE TABLE IF NOT EXISTS inbounds_version (id INTEGER PRIMARY KEY CHECK (id = 1), version INTEGER NOT NULL)",
			"INSERT OR IGNORE INTO inbounds_version (id, version) VALUES (1, 0)",
			"DROP TRIGGER IF EXISTS inbounds_version_insert",
			"DROP TRIGGER IF EXISTS inbounds_version_update",
			"DROP TRIGGER IF EXISTS inbounds_version_delete",
			"CREATE TRIGGER inbounds_version_insert AFTER INSERT ON inbounds " + bump,
			"CREATE TRIGGER inbounds_version_update AFTER UPDATE OF " + strings.Join(watched, ", ") + " ON inbounds " + bump,
			"CREATE TRIGGER inbounds_version_delete AFTER DELETE ON inbounds " + bump,
		} {
			if err := tx.Exec(stmt).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

// InboundsVersion returns the counter initInboundsVersion's triggers move.
func InboundsVersion() (int64, error) {
	var version int64
	err := db.Raw("SELECT version FROM inbounds_version WHERE id = 1").Scan(&version).Error
	return version, err
}

// Epoch identifies the database InitDB last opened; see epoch.
func Epoch() int64 {
	return epoch.Load()
}

// sqliteDSN opens every pooled connection in WAL mode, so readers never block the writer, and makes
// every transaction BEGIN IMMEDIATE, so a writer queues for the lock up front instead of failing with
// "database is locked" when a read-then-write transaction tries to upgrade. An inbound with tens of
// thousands of clients is one multi-megabyte settings row, rewritten on each client change.
func sqliteDSN(dbPath string) string {
	return dbPath + "?_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=10000&_txlock=immediate&_cache_size=-32768"
}

// CloseDB closes the database connection if it exists.
func CloseDB() error {
	if db != nil {
		sqlDB, err := db.DB()
		if err != nil {
			return err
		}
		return sqlDB.Close()
	}
	return nil
}

// GetDB returns the global GORM database instance.
func GetDB() *gorm.DB {
	return db
}

func IsNotFound(err error) bool {
	return errors.Is(err, gorm.ErrRecordNotFound)
}

// IsSQLiteDB checks if the given file is a valid SQLite database by reading its signature.
func IsSQLiteDB(file io.ReaderAt) (bool, error) {
	signature := []byte("SQLite format 3\x00")
	buf := make([]byte, len(signature))
	_, err := file.ReadAt(buf, 0)
	if err != nil {
		return false, err
	}
	return bytes.Equal(buf, signature), nil
}

// Checkpoint performs a WAL checkpoint on the SQLite database to ensure data consistency.
func Checkpoint() error {
	// Update WAL
	err := db.Exec("PRAGMA wal_checkpoint;").Error
	if err != nil {
		return err
	}
	return nil
}

// Snapshot writes a consistent copy of the live database, under the database's own file name, into a
// fresh temporary directory; cleanup removes it. In WAL mode the main file alone can lack recent commits
// or be caught mid-checkpoint, so it must not be read directly as a backup.
func Snapshot() (snapshotPath string, cleanup func(), err error) {
	dir, err := os.MkdirTemp("", "x-ui-db-")
	if err != nil {
		return "", nil, err
	}
	cleanup = func() { os.RemoveAll(dir) }
	snapshotPath = path.Join(dir, path.Base(config.GetDBPath()))
	if err = db.Exec("VACUUM INTO ?", snapshotPath).Error; err != nil {
		cleanup()
		return "", nil, err
	}
	return snapshotPath, cleanup, nil
}

// sqliteSidecars are the files SQLite keeps next to a WAL-mode database. They belong to that one file:
// left behind when it is replaced, a stale WAL would be replayed onto the new database.
var sqliteSidecars = []string{"-wal", "-shm"}

// MoveDBFiles renames a database file together with its WAL sidecars.
func MoveDBFiles(from, to string) error {
	if err := os.Rename(from, to); err != nil {
		return err
	}
	for _, suffix := range sqliteSidecars {
		if err := os.Rename(from+suffix, to+suffix); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	return nil
}

// RemoveDBFiles deletes a database file and its WAL sidecars; missing files are not an error.
func RemoveDBFiles(dbPath string) error {
	for _, suffix := range append([]string{""}, sqliteSidecars...) {
		if err := os.Remove(dbPath + suffix); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	return nil
}

// ValidateSQLiteDB opens the provided sqlite DB path with a throw-away connection
// and runs a PRAGMA integrity_check to ensure the file is structurally sound.
// It does not mutate global state or run migrations.
func ValidateSQLiteDB(dbPath string) error {
	if _, err := os.Stat(dbPath); err != nil { // file must exist
		return err
	}
	gdb, err := gorm.Open(sqlite.Open(dbPath), &gorm.Config{Logger: logger.Discard})
	if err != nil {
		return err
	}
	sqlDB, err := gdb.DB()
	if err != nil {
		return err
	}
	defer sqlDB.Close()
	var res string
	if err := gdb.Raw("PRAGMA integrity_check;").Scan(&res).Error; err != nil {
		return err
	}
	if res != "ok" {
		return errors.New("sqlite integrity check failed: " + res)
	}
	return nil
}
