# gscache

gscache (*G*o *S*hared _Cache_) is a shared cache for Go build systems that implements the [Golang's cacheprog protocol](https://pkg.go.dev/cmd/go/internal/cacheprog) (available in Go 1.24 or later).

Supported cache backends:

- Local filesystem
- Amazon S3 and compatible (like MinIO)
- Google Cloud Storage
- Azure Blob Storage

## Configuration

Use S3 as backend:

```bash
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key
export GOCACHEPROG="<abs_path>/gscache prog --remote.url=s3://bucket-name?region=us-west-2"
```

Use MinIO as backend:

```bash
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export GOCACHEPROG="<abs_path>/gscache prog --remote.url=s3://bucket-name?endpoint=http://localhost:9000&use_path_style=true&disable_https=true"
```
