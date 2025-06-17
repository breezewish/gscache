// Copyright 2022 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package e2e

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/tools/txtar"
	"rsc.io/script"
	"rsc.io/script/scripttest"
)

// This is a modified version of scripttest.Test that does not run tests in parallel,
// and calls afterEachTest after each test case to do some cleanup.
func runScriptTests(t *testing.T, env []string, pattern string) {
	ctx := context.Background()
	engine := &script.Engine{
		Conds: scripttest.DefaultConds(),
		Cmds:  Commands(),
		Quiet: false,
	}
	files, _ := filepath.Glob(pattern)
	if len(files) == 0 {
		t.Fatal("no testdata")
	}
	for _, file := range files {
		file := file
		name := strings.TrimSuffix(filepath.Base(file), ".txt")
		t.Run(name, func(t *testing.T) {
			workdir := t.TempDir()
			s, err := script.NewState(ctx, workdir, env)
			if err != nil {
				t.Fatal(err)
			}

			// Unpack archive.
			a, err := txtar.ParseFile(file)
			if err != nil {
				t.Fatal(err)
			}
			initScriptDirs(t, s)
			if err := s.ExtractFiles(a); err != nil {
				t.Fatal(err)
			}

			t.Log(time.Now().UTC().Format(time.RFC3339))
			work, _ := s.LookupEnv("WORK")
			t.Logf("$WORK=%s", work)

			t.Cleanup(func() {
				afterEachTest(t, workdir)
			})

			scripttest.Run(t, engine, s, file, bytes.NewReader(a.Comment))
		})
	}
}

func initScriptDirs(t testing.TB, s *script.State) {
	must := func(err error) {
		if err != nil {
			t.Helper()
			t.Fatal(err)
		}
	}

	work := s.Getwd()
	must(s.Setenv("WORK", work))
	must(os.MkdirAll(filepath.Join(work, "tmp"), 0777))
	must(s.Setenv("TMPDIR", filepath.Join(work, "tmp")))
}
