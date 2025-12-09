package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/melihbirim/sieswi/internal/engine"
	"github.com/melihbirim/sieswi/internal/sidx"
	"github.com/melihbirim/sieswi/internal/sqlparser"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	// Check for version flag
	if len(os.Args) >= 2 && (os.Args[1] == "--version" || os.Args[1] == "-v") {
		fmt.Printf("sieswi %s (commit: %s, built: %s)\n", version, commit, date)
		os.Exit(0)
	}

	// Check for index command
	if len(os.Args) >= 2 && os.Args[1] == "index" {
		// Parse flags for index command
		indexFlags := flag.NewFlagSet("index", flag.ExitOnError)
		skipTypeInference := indexFlags.Bool("skip-type-inference", false, "Skip type inference, assume all columns are strings (faster)")
		blockSizeKB := indexFlags.Int("block-size", 32, "Block size in KB (default: 32)")
		if err := indexFlags.Parse(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "parse flags: %v\n", err)
			os.Exit(1)
		}

		if indexFlags.NArg() < 1 {
			fmt.Fprintln(os.Stderr, "usage: sieswi index [--skip-type-inference] [--block-size KB] <csvfile>")
			os.Exit(1)
		}

		csvPath := indexFlags.Arg(0)
		blockSize := uint32(*blockSizeKB * 1024)
		if err := buildIndex(csvPath, *skipTypeInference, blockSize); err != nil {
			fmt.Fprintln(os.Stderr, "index error:", err)
			os.Exit(1)
		}
		return
	}

	queryText, err := getQueryFromArgsOrStdin(os.Args[1:], os.Stdin)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	query, err := sqlparser.Parse(queryText)
	if err != nil {
		fmt.Fprintln(os.Stderr, "parse error:", err)
		os.Exit(1)
	}

	writer := bufio.NewWriter(os.Stdout)
	defer func() {
		if err := writer.Flush(); err != nil {
			fmt.Fprintf(os.Stderr, "flush output: %v\n", err)
		}
	}()

	if err := engine.Execute(query, writer); err != nil {
		fmt.Fprintln(os.Stderr, "execution error:", err)
		os.Exit(1)
	}
}

func buildIndex(csvPath string, skipTypeInference bool, blockSize uint32) error {
	fmt.Fprintf(os.Stderr, "Building index for %s (block size: %d KB)...\n", csvPath, blockSize/1024)

	builder := sidx.NewBuilder(blockSize)
	builder.SetSkipTypeInference(skipTypeInference)
	index, err := builder.BuildFromFile(csvPath)
	if err != nil {
		return fmt.Errorf("build index: %w", err)
	}

	indexPath := csvPath + ".sidx"
	f, err := os.Create(indexPath)
	if err != nil {
		return fmt.Errorf("create index file: %w", err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "close index file: %v\n", err)
		}
	}()

	if err := sidx.WriteIndex(f, index); err != nil {
		return fmt.Errorf("write index: %w", err)
	}

	fmt.Fprintf(os.Stderr, "Index written to %s (%d blocks)\n", indexPath, index.Header.NumBlocks)
	return nil
}

func getQueryFromArgsOrStdin(args []string, stdin io.Reader) (string, error) {
	if len(args) > 0 {
		return strings.TrimSpace(strings.Join(args, " ")), nil
	}

	data, err := io.ReadAll(stdin)
	if err != nil {
		return "", fmt.Errorf("read query from stdin: %w", err)
	}

	query := strings.TrimSpace(string(data))
	if query == "" {
		return "", errors.New("usage: sieswi \"SELECT ...\" (or pipe query via stdin)")
	}

	return query, nil
}
