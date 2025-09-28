package rscthrottler

type WorkspaceRSCConfig struct {
	TinyTableThreshold int64 `toml:"tiny-table-threshold"`
	MaxAccumulatedRows int64 `toml:"max-accumulated-rows"`
	MaxSingleAcquire   int64 `toml:"max-single-acquire"`
	MaxAccumulatedSize int64 `toml:"max-accumulated-size"`
}
