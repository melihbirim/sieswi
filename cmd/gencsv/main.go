package main

import (
	"bufio"
	"encoding/csv"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"
)

var (
	rows    = flag.Int("rows", 1_000_000, "number of rows to generate")
	outPath = flag.String("out", "fixtures/ecommerce_1m.csv", "output CSV path")
	seed    = flag.Int64("seed", 42, "random seed")
	sorted  = flag.Bool("sorted", false, "generate with sorted timestamps (for testing .sidx)")
)

func main() {
	flag.Parse()

	if err := os.MkdirAll(filepath.Dir(*outPath), 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "create output dir: %v\n", err)
		os.Exit(1)
	}

	file, err := os.Create(*outPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create file: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	writer := csv.NewWriter(bufio.NewWriterSize(file, 1<<20))
	defer writer.Flush()

	header := []string{
		"order_id",
		"user_id",
		"product_id",
		"quantity",
		"price_minor", // 4-digit minor units (1000 => £10.00)
		"discount_minor",
		"total_minor",
		"status",
		"country",
		"created_at",
	}
	if err := writer.Write(header); err != nil {
		fmt.Fprintf(os.Stderr, "write header: %v\n", err)
		os.Exit(1)
	}

	rng := rand.New(rand.NewSource(*seed))

	countries := []string{"UK", "US", "DE", "FR", "ES", "IT", "NL", "CA", "AU", "SE"}
	statuses := []string{"pending", "processing", "completed", "cancelled", "refunded"}

	baseTime := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)

	buf := make([]string, len(header))

	for i := 0; i < *rows; i++ {
		orderID := fmt.Sprintf("ORD%09d", i+1)
		userID := fmt.Sprintf("USR%06d", rng.Intn(200_000)+1)
		productID := fmt.Sprintf("PRD%05d", rng.Intn(20_000)+1)
		quantity := rng.Intn(5) + 1
		priceMinor := rng.Intn(9000) + 1000 // 4-digit price (1000 => £10.00)
		discountMinor := 0
		if rng.Float64() < 0.15 {
			discountMinor = rng.Intn(priceMinor/5 + 1)
		}
		subtotal := priceMinor * quantity
		totalMinor := subtotal - discountMinor
		if totalMinor < 0 {
			totalMinor = 0
		}
		status := statuses[rng.Intn(len(statuses))]
		country := countries[rng.Intn(len(countries))]

		// Generate timestamp
		var createdAt string
		if *sorted {
			// Sequential timestamps: ~1 second per row for 1M rows = ~11 days
			createdAt = baseTime.Add(time.Duration(i) * time.Second).Format(time.RFC3339)
		} else {
			// Random timestamps across full year
			createdAt = baseTime.Add(time.Duration(rand.Intn(365*24)) * time.Hour).Format(time.RFC3339)
		}

		buf[0] = orderID
		buf[1] = userID
		buf[2] = productID
		buf[3] = fmt.Sprintf("%d", quantity)
		buf[4] = fmt.Sprintf("%04d", priceMinor)
		buf[5] = fmt.Sprintf("%d", discountMinor)
		buf[6] = fmt.Sprintf("%d", totalMinor)
		buf[7] = status
		buf[8] = country
		buf[9] = createdAt

		if err := writer.Write(buf); err != nil {
			fmt.Fprintf(os.Stderr, "write row %d: %v\n", i, err)
			os.Exit(1)
		}

		if (i+1)%100_000 == 0 {
			writer.Flush()
			if err := writer.Error(); err != nil {
				fmt.Fprintf(os.Stderr, "flush rows: %v\n", err)
				os.Exit(1)
			}
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		fmt.Fprintf(os.Stderr, "final flush: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stdout, "wrote %d rows to %s\n", *rows, *outPath)
}
