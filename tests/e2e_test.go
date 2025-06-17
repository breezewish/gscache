package e2e

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/shirou/gopsutil/v4/process"
)

var GSCACHE_BINARY_PATH string

var afterEachTest = func(t *testing.T, workDir string) {
	// Cleanup detached daemon if any
	allProc, err := process.Processes()
	if err != nil {
		return
	}
	for _, proc := range allProc {
		name, _ := proc.Cmdline()
		if strings.HasPrefix(name, GSCACHE_BINARY_PATH) || strings.HasPrefix(name, "gscache_bin") {
			_ = proc.Terminate()
		}
	}
	// Additionally print out server logs for debugging
	if t.Failed() && workDir != "" {
		logData, err := os.ReadFile(filepath.Join(workDir, ".gscache/gscache.log"))
		if err == nil {
			t.Logf("Server logs: %s", string(logData))
		}
	}
}

func TestAll(t *testing.T) {
	// Note: This test must run with cache disabled if gscache source is changed.
	makeGsCacheBinary(t)
	runScriptTests(t, []string{
		"GSCACHE_BIN=" + GSCACHE_BINARY_PATH,
	}, "./testdata/scripts/*.txt")
}

func makeGsCacheBinary(t *testing.T) {
	err := exec.Command("go", "build", "-o", "./gscache_bin", "../cmd/gscache").Run()
	if err != nil {
		t.Fatal("failed to build gscache", err)
	}
	wd, _ := os.Getwd()
	GSCACHE_BINARY_PATH = filepath.Join(wd, "gscache_bin")

	t.Cleanup(func() {
		os.Remove(GSCACHE_BINARY_PATH)
		afterEachTest(t, "")
	})
}
