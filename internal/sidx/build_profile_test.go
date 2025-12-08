package sidx_test

import (
	"os"
	"runtime/pprof"
	"testing"

	"github.com/sieswi/sieswi/internal/sidx"
)

// TestBuildProfileLong profiles SIDX index building on a 10GB CSV file.
// This test is skipped by default. Set TEST_10GB=1 environment variable to enable it.
// Example: TEST_10GB=1 go test -v -run TestBuildProfileLong ./internal/sidx
func TestBuildProfileLong(t *testing.T) {
	if os.Getenv("TEST_10GB") != "1" {
		t.Skip("Skipping 10GB profile test (set TEST_10GB=1 to enable)")
	}

	csvPath := "../../fixtures/ecommerce_10gb.csv"

	// CPU profiling
	cpuFile, err := os.Create("build_profile.prof")
	if err != nil {
		t.Fatalf("Could not create CPU profile: %v", err)
	}
	defer cpuFile.Close()

	if err := pprof.StartCPUProfile(cpuFile); err != nil {
		t.Fatalf("Could not start CPU profile: %v", err)
	}
	defer pprof.StopCPUProfile()

	// Build index
	builder := sidx.NewBuilder(sidx.BlockSize)
	builder.SetSkipTypeInference(true)
	index, err := builder.BuildFromFile(csvPath)
	if err != nil {
		t.Fatalf("BuildFromFile failed: %v", err)
	}

	t.Logf("Built index with %d blocks", len(index.Blocks))
}
