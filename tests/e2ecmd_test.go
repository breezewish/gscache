package e2e

import (
	"fmt"
	"time"

	minioSDK "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/minio"
	"rsc.io/script"
	"rsc.io/script/scripttest"
)

func CmdRunMinio() script.Cmd {
	return script.Command(
		script.CmdUsage{
			Summary: "start a MinIO server in background, and connection URL will be available as environment variable MINIO_URL",
			Args:    "",
		},
		func(s *script.State, args ...string) (script.WaitFunc, error) {
			if len(args) > 0 {
				return nil, script.ErrUsage
			}

			fmt.Println("Starting MinIO server...")

			minioContainer, err := minio.Run(s.Context(), "minio/minio:RELEASE.2024-01-16T16-07-38Z")
			if err != nil {
				return nil, err
			}
			url, err := minioContainer.ConnectionString(s.Context())
			if err != nil {
				return nil, err
			}
			s.Setenv("MINIO_URL", url)

			// Create default buckets
			minioClient, err := minioSDK.New(url, &minioSDK.Options{
				Creds:  credentials.NewStaticV4("minioadmin", "minioadmin", ""),
				Secure: false,
			})
			if err != nil {
				return nil, err
			}
			err = minioClient.MakeBucket(s.Context(), "my-bucket", minioSDK.MakeBucketOptions{})
			if err != nil {
				return nil, fmt.Errorf("failed to create bucket: %w", err)
			}
			s.Setenv("MINIO_BUCKET", "my-bucket")

			wait := func(*script.State) (stdout, stderr string, err error) {
				// Keep minio running in the background, kill until script ends
				go func() {
					<-s.Context().Done()
					_ = testcontainers.TerminateContainer(minioContainer)
				}()
				return "", "", nil
			}
			return wait, nil
		})
}

func CmdSetEnvGoCacheProg() script.Cmd {
	return script.Command(
		script.CmdUsage{
			Summary: "set environment variable GOCACHEPROG to the path of gscache binary",
			Args:    "",
			Async:   false,
		},
		func(s *script.State, args ...string) (script.WaitFunc, error) {
			if len(args) > 0 {
				return nil, script.ErrUsage
			}
			s.Setenv("GOCACHEPROG", fmt.Sprintf("%s prog", GSCACHE_BINARY_PATH))
			return nil, nil
		})
}

func Commands() map[string]script.Cmd {
	commands := scripttest.DefaultCmds()
	commands["start_minio"] = CmdRunMinio()
	commands["gscache"] = script.Program(GSCACHE_BINARY_PATH, nil, 100*time.Millisecond) // Shortcut of exec $GSCACHE_BIN
	commands["go"] = script.Program("go", nil, 100*time.Millisecond)
	commands["env:set_gocacheprog"] = CmdSetEnvGoCacheProg() // For some reason env command does not with space
	return commands
}
