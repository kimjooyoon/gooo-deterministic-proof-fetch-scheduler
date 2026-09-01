package scheduler

import (
	"runtime"
	"syscall"
)

func peakRSSKib() int64 {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err == nil {
		value := int64(usage.Maxrss)
		if runtime.GOOS == "darwin" {
			return value / 1024
		}
		return value
	}
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return int64(stats.Sys / 1024)
}

func CurrentPeakRSSKib() int64 {
	return peakRSSKib()
}
