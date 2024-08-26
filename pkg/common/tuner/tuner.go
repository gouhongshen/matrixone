package tuner

type EngineTestingTuner struct {
	// DeletionFlushThreshold overrides flushThreshold in deletion/types.go
	// keep not change: 0
	// always flush: < 0
	// another value: > 0
	DeletionFlushThreshold int32
}
