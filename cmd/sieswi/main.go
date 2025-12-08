package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/sieswi/sieswi/internal/engine"
	"github.com/sieswi/sieswi/internal/sidx"
	"github.com/sieswi/sieswi/internal/sqlparser"
)

func main() {
	// Check for index command
	if len(os.Args) >= 2 && os.Args[1] == "index" {
		// Parse flags for index command
		indexFlags := flag.NewFlagSet("index", flag.ExitOnError)
		skipTypeInference := indexFlags.Bool("skip-type-inference", false, "Skip type inference, assume all columns are strings (faster)")
		indexFlags.Parse(os.Args[2:])

		if indexFlags.NArg() < 1 {
			fmt.Fprintln(os.Stderr, "usage: sieswi index [--skip-type-inference] <csvfile>")
			os.Exit(1)
		}

		csvPath := indexFlags.Arg(0)
		if err := buildIndex(csvPath, *skipTypeInference); err != nil {
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
	defer writer.Flush()

	if err := engine.Execute(query, writer); err != nil {
		fmt.Fprintln(os.Stderr, "execution error:", err)
		os.Exit(1)
	}
}

func buildIndex(csvPath string, skipTypeInference bool) error {
	fmt.Fprintf(os.Stderr, "Building index for %s...\n", csvPath)

	builder := sidx.NewBuilder(sidx.BlockSize)
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
	defer f.Close()

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
