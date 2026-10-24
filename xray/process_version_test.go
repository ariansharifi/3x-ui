//go:build !windows

package xray

import (
	"os"
	"path/filepath"
	"testing"
)

// Fork and upstream builds share version numbers, so the panel can only tell them apart by the build field.
func TestRefreshVersionReadsTheBuildField(t *testing.T) {
	binDir := t.TempDir()
	t.Setenv("XUI_BIN_FOLDER", binDir)
	script := "#!/bin/sh\n" +
		"echo 'Xray 26.10.21 (Xray, Penetrates Everything.) 4f65bc97-ariansharifi (go1.27.1 linux/amd64)'\n" +
		"echo 'A unified platform for anti-censorship.'\n"
	if err := os.WriteFile(filepath.Join(binDir, GetBinaryName()), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	p := newProcess(&Config{})
	p.refreshVersion()

	if p.GetVersion() != "26.10.21" {
		t.Errorf("version = %q, want %q", p.GetVersion(), "26.10.21")
	}
	if p.GetBuild() != "4f65bc97-ariansharifi" {
		t.Errorf("build = %q, want %q", p.GetBuild(), "4f65bc97-ariansharifi")
	}
}
