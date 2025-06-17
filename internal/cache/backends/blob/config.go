package blob

type Config struct {
	URL               string `json:"url"`
	UploadConcurrency int    `json:"upload_concurrency"`
	WorkDir           string `json:"-"` // Should be set from parent config instead of config file
}

func DefaultConfig() Config {
	return Config{
		URL:               "",
		UploadConcurrency: 50,
		WorkDir:           "",
	}
}
