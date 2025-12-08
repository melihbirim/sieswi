package main

import (
	"bufio"
	"fmt"
	"os"
	"runtime/pprof"

	"github.com/melihbirim/sieswi/internal/engine"
	"github.com/melihbirim/sieswi/internal/sqlparser"
)

func main() {
	// Start CPU profiling
	f, err := os.Create("/tmp/sieswi_cpu.prof")
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "close profile: %v\n", err)
		}
	}()

	if err := pprof.StartCPUProfile(f); err != nil {
		panic(err)
	}
	defer pprof.StopCPUProfile()

	// Run query
	query, err := sqlparser.Parse("SELECT * FROM 'fixtures/ecommerce_10m.csv' WHERE country = 'UK'")
	if err != nil {
		panic(err)
	}

	writer := bufio.NewWriter(os.Stdout)
	defer func() {
		if err := writer.Flush(); err != nil {
			fmt.Fprintf(os.Stderr, "flush output: %v\n", err)
		}
	}()

	if err := engine.Execute(query, writer); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
