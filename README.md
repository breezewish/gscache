# gscache

gscache (*G*o *S*hared _Cache_) is a shared cache for Go build systems that implements the
[Golang's cacheprog protocol](https://pkg.go.dev/cmd/go/internal/cacheprog), which is available in
Go 1.24 or later.

- S3, GCS, Azure, Minio as the backend storage.
- Hydrate from local cache when local cache is still valid.
- Various statistics metrics
- (Experimental) Compact and pre-warm small objects.

## Requirements

- Go 1.24 or later

## Usage

**Use in Github Action:**

See [gscache-action](https://github.com/breezewish/gscache-action).

**S3 backend:**

```shell
# Remember to configure AWS credentials, like:
#   export AWS_ACCESS_KEY_ID=your-access-key
#   export AWS_SECRET_ACCESS_KEY=your-secret-key
# Other ways like ~/.aws/credentials or Instance Profile are also supported.
export GSCACHE_BLOB_URL="s3://bucket-name?prefix=gscache/&region=us-west-2"
export GOCACHEPROG="<abs_path>/gscache prog"
```

**GCS backend:**

```shell
# Remember to configure GCP credentials.
export GSCACHE_BLOB_URL="gs://bucket-name?prefix=gscache/"
export GOCACHEPROG="<abs_path>/gscache prog"
```

**MinIO backend:**

```shell
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export GSCACHE_BLOB_URL="s3://bucket-name?prefix=gscache/&endpoint=http://localhost:9000&use_path_style=true&disable_https=true"
export GOCACHEPROG="<abs_path>/gscache prog"
```

**View statistics:**

```shell
gscache stats

# To clear statistics counters:
# gscache stats clear
```

**View logs:**

Log is by default written to `~/.gscache/gscache.log`.

You may also execute the following command to tail logs in a colorized way:

```shell
gscache logs
```

**Use config file:**

By default `~/.config/gscache/config.toml` will be used as the config file. To use a different
config file, set `GSCACHE_CONFIG=<config_file>` or use `--config <config_file>` flag.

The default configuration is as below:

```toml
port = 8511
dir = "~/.gscache"
shutdown_after_inactivity = "10m"

[log]
level = "info"
file = "~/.gscache/gscache.log"

[blob]
url = ""  # If not set, a local-only cache will be used.
upload_concurrency = 50
```

## Development

**Run unit tests and e2e tests:**

```shell
# Note: gscache uses https://github.com/casey/just for task automation.
just test
```

## Performance

When cache is cold:

| Project | Go Local Cache + @actions/cache | gscache (GCS) |
| ------- | ------------------------------- | ------------- |
| TiDB    | 20s + Download/Upload (25s)     | 60s           |

As you can see, gscache is always slower than @actions/cache **for cold cache**, because
@actions/cache downloads all cache files at once as an archive, while gscache downloads files
on-demand (as requested by go/cmd), suffering more from object storage latency.

So when to use gscache? You may find it useful for large projects, e.g. you must parallelize your
builds and unit tests into multiple shards. In such case, you cannot keep a good-enough local cache
for each shard, because most of the cache content is similar. On the other hand, uploading cache for
each shard will consume too many storage resources. In such case, gscache can be a good choice to
save both storage and keep cache content valid across shards.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
