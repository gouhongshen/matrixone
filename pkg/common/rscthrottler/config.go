package rscthrottler

type WorkspaceRSCConfig struct {
	MaxSingleAcquire   int64 `toml:"max_single_acquire"`
	MaxAccumulatedSize int64 `toml:"max_accumulated_size"`
}
