# Examples

## Basic Queries

### Select All Columns

```bash
sieswi "SELECT * FROM 'data.csv'"
```

### Select Specific Columns

```bash
sieswi "SELECT name, email, age FROM 'users.csv'"
```

### Filter with WHERE

```bash
sieswi "SELECT * FROM 'orders.csv' WHERE country = 'US'"
```

### Limit Results

```bash
sieswi "SELECT * FROM 'logs.csv' WHERE level = 'ERROR' LIMIT 100"
```

## Advanced Queries

### Numeric Comparisons

```bash
# Greater than
sieswi "SELECT * FROM 'sales.csv' WHERE amount > 1000"

# Range query
sieswi "SELECT * FROM 'products.csv' WHERE price >= 10 AND price <= 50"

# Not equal
sieswi "SELECT * FROM 'users.csv' WHERE status != 'inactive'"
```

### Case-Insensitive Columns

```bash
# These are all equivalent
sieswi "SELECT UserName FROM 'users.csv'"
sieswi "SELECT username FROM 'users.csv'"
sieswi "SELECT USERNAME FROM 'users.csv'"
```

## Piping and Chaining

### Read from stdin

```bash
cat data.csv | sieswi "SELECT * FROM '-' WHERE active = 'true'"
```

### Chain with other tools

```bash
# Tail logs and filter
tail -f app.log.csv | sieswi "SELECT * FROM '-' WHERE level = 'ERROR'"

# Filter and count
sieswi "SELECT * FROM 'orders.csv' WHERE status = 'pending'" | wc -l

# Filter and grep
sieswi "SELECT * FROM 'users.csv' WHERE country = 'US'" | grep '@gmail.com'

# Multiple filters
sieswi "SELECT * FROM 'logs.csv' WHERE level = 'ERROR'" | \
sieswi "SELECT * FROM '-' WHERE service = 'api'" | \
head -10
```

### Write to file

```bash
# Save results
sieswi "SELECT * FROM 'data.csv' WHERE active = 'true'" > active_users.csv

# Append to file
sieswi "SELECT * FROM 'new_data.csv'" >> all_data.csv
```

## Working with Indexes

### Create Index

```bash
# Create index on 'country' column
sieswi index data.csv country

# This creates data.csv.sidx
```

### Query with Index (85x faster!)

```bash
# Automatically uses index if available
sieswi "SELECT * FROM 'data.csv' WHERE country = 'UK'"

# On 10M rows: 12ms with index vs 1050ms without
```

### Skip Type Inference (faster indexing)

```bash
# If all columns are strings
sieswi index --skip-type-inference large_file.csv indexed_column
```

## Real-World Use Cases

### Log Analysis

```bash
# Find errors in last hour
tail -1000 app.log.csv | \
sieswi "SELECT timestamp, message FROM '-' WHERE level = 'ERROR'"

# Count errors by service
sieswi "SELECT service FROM 'logs.csv' WHERE level = 'ERROR'" | \
sort | uniq -c
```

### Data Exploration

```bash
# Quick peek at data
sieswi "SELECT * FROM 'unknown.csv' LIMIT 10"

# Check for missing values
sieswi "SELECT * FROM 'data.csv' WHERE email = ''" | wc -l

# Sample random rows (with seed)
shuf -n 1000 large.csv | sieswi "SELECT * FROM '-'"
```

### ETL Pipelines

```bash
# Extract subset
sieswi "SELECT user_id, event_type FROM 'events.csv' WHERE date = '2025-01-01'" > daily_events.csv

# Transform and load
sieswi "SELECT name, email FROM 'users.csv' WHERE country = 'US'" | \
your_etl_tool --import

# Batch processing
for file in data/*.csv; do
  sieswi "SELECT * FROM '$file' WHERE active = 'true'" >> active_users.csv
done
```

### Data Quality Checks

```bash
# Check for duplicates
sieswi "SELECT email FROM 'users.csv'" | sort | uniq -d

# Validate email format
sieswi "SELECT email FROM 'users.csv'" | grep -v '@' | wc -l

# Find outliers
sieswi "SELECT price FROM 'products.csv' WHERE price > 10000"
```

## Performance Tips

### Use Indexes for Selective Queries

```bash
# Create index on filtered column
sieswi index orders.csv country

# Query is now 85x faster
sieswi "SELECT * FROM 'orders.csv' WHERE country = 'US'"
```

### Parallel Processing (automatic for large files)

```bash
# Files >10MB automatically use parallel processing
# No configuration needed!

sieswi "SELECT * FROM 'huge_file.csv' WHERE status = 'active'"
# Uses all CPU cores, 7x faster than sequential
```

### Limit Results Early

```bash
# Good: Only processes until limit
sieswi "SELECT * FROM 'huge.csv' WHERE active = 'true' LIMIT 100"

# Avoid: Processes entire file
sieswi "SELECT * FROM 'huge.csv' WHERE active = 'true'" | head -100
```

### Debug Performance

```bash
# Enable debug logging
SIDX_DEBUG=1 sieswi "SELECT * FROM 'file.csv' WHERE col = 'val'"

# Shows:
# - Whether parallel processing is used
# - Number of chunks and workers
# - Row counts

# Disable parallel (for comparison)
SIDX_NO_PARALLEL=1 sieswi "SELECT * FROM 'file.csv' WHERE col = 'val'"
```

## Edge Cases

### Quoted Fields

```bash
# Handles CSV with commas in fields
echo 'name,address
"Smith, John","123 Main St, NYC"' | sieswi "SELECT * FROM '-'"

# Output:
# name,address
# Smith, John,123 Main St, NYC
```

### Escaped Quotes

```bash
# Handles doubled quotes per RFC 4180
echo 'product,description
Phone,"5\"" screen"' | sieswi "SELECT * FROM '-'"
```

### Large Lines

```bash
# Handles rows up to 4MB
# (larger rows logged with SIDX_DEBUG=1)
SIDX_DEBUG=1 sieswi "SELECT * FROM 'wide_rows.csv'"
```

## Integration Examples

### With jq (JSON conversion)

```bash
# CSV to JSON
sieswi "SELECT name, age FROM 'users.csv'" | \
awk 'NR>1 {print "{\"name\":\""$1"\",\"age\":"$2"}"}' | jq -s '.'
```

### With awk (custom formatting)

```bash
# Custom output format
sieswi "SELECT name, price FROM 'products.csv'" | \
awk -F, 'NR>1 {printf "%-20s $%.2f\n", $1, $2}'
```

### With watch (monitoring)

```bash
# Real-time monitoring
watch -n 5 "sieswi 'SELECT * FROM \"live.csv\" WHERE status = \"error\"' | wc -l"
```

## Troubleshooting

### Query Syntax Errors

```bash
# Error: Missing quotes around file path
sieswi "SELECT * FROM data.csv"
# Fix: Add quotes
sieswi "SELECT * FROM 'data.csv'"

# Error: Case-sensitive column name (not in sieswi!)
sieswi "SELECT UserName FROM 'users.csv'"  # Works!
sieswi "SELECT username FROM 'users.csv'"  # Also works!
```

### Performance Issues

```bash
# Slow on small selective query?
# → Create an index!
sieswi index data.csv filtered_column

# Slow on large file without WHERE?
# → Parallel processing should auto-activate
SIDX_DEBUG=1 sieswi "SELECT * FROM 'large.csv'"
# Check output: should show "Processed X chunks with Y workers"
```

## More Help

- Run `sieswi --version` to check your version
- See [README.md](../README.md) for SQL support
- See [PARALLEL_PROCESSING.md](../PARALLEL_PROCESSING.md) for performance details
- Report issues at https://github.com/sieswi/sieswi/issues
