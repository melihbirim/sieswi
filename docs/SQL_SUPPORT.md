# SQL Feature Support

Quick reference for what sieswi supports now and what's coming.

---

## ✅ Currently Supported (Phase 1)

```sql
-- Basic SELECT
SELECT col1, col2 FROM file.csv
SELECT * FROM file.csv

-- Single WHERE condition
WHERE country = 'UK'
WHERE price > 100
WHERE status != 'cancelled'

-- Operators: =, !=, >, >=, <, <=

-- LIMIT
LIMIT 10

-- Complete example
SELECT order_id, price FROM orders.csv 
WHERE country = 'UK' 
LIMIT 5
```

---

## 🔜 Coming Soon

### Phase 4a (Week 3) - AND
```sql
WHERE country = 'UK' AND price > 100
WHERE a = 1 AND b > 2 AND c != 'foo'
```

### Phase 4b (Week 4) - IN Clause
```sql
WHERE country IN ('UK', 'US', 'CA')
WHERE status IN ('PENDING', 'APPROVED')
```

### Phase 4c (Weeks 5-6) - Full Boolean
```sql
WHERE (country = 'UK' AND price > 100) OR (country = 'US' AND price > 200)
WHERE NOT (status = 'CANCELLED' OR status = 'REFUNDED')
WHERE a = 1 AND (b = 2 OR c = 3)
```

### Phase 5 (Weeks 7-8) - UX Features
```sql
SELECT col1 AS id, col2 AS name FROM file.csv
SELECT DISTINCT country FROM orders.csv
LIMIT 100 OFFSET 50

SELECT 
  CASE 
    WHEN price < 10 THEN 'cheap'
    WHEN price < 100 THEN 'medium'
    ELSE 'expensive'
  END as tier
FROM orders.csv
```

---

## ❌ Not Supported (Use DuckDB Instead)

```sql
-- Sorting (breaks streaming)
ORDER BY price DESC

-- Aggregations (needs memory buffering)
SELECT country, COUNT(*), AVG(price)
FROM orders.csv
GROUP BY country

-- JOINs (multi-file operations)
SELECT * FROM a.csv 
JOIN b.csv ON a.id = b.id

-- Subqueries
WHERE id IN (SELECT id FROM other.csv)
```

---

## 🎯 Target Coverage: 70-80% of Real-World Queries

**What you CAN do:**
- Filter rows with complex boolean logic
- Project specific columns
- Sample data (LIMIT/OFFSET)
- Find unique values (DISTINCT)
- Transform values (CASE)

**What you CANNOT do:**
- Sort results (use `| sort` in shell)
- Aggregate data (use `| awk` or DuckDB)
- Join tables (use DuckDB)

---

## 💡 Workarounds

### Sorting
```bash
# Instead of ORDER BY
sieswi "SELECT * FROM data.csv WHERE price > 100" | sort -t, -k2 -n
```

### Simple Aggregations
```bash
# Count rows
sieswi "SELECT * FROM data.csv WHERE status = 'ERROR'" | wc -l

# Sum column
sieswi "SELECT price FROM data.csv WHERE country = 'UK'" | awk -F, '{sum+=$1} END {print sum}'
```

### Multiple Filters (Before AND Support)
```bash
# Chain filters
sieswi "SELECT * FROM data.csv WHERE country = 'UK'" | \
  sieswi "SELECT * FROM - WHERE price > 100"
```

---

## 🧪 Testing Your Queries

```bash
# Verify syntax
sieswi "SELECT * FROM test.csv LIMIT 1"

# Compare with DuckDB
sieswi "SELECT * FROM data.csv WHERE x = 1" > sieswi_out.csv
duckdb -c "COPY (SELECT * FROM 'data.csv' WHERE x = 1) TO 'duck_out.csv' (HEADER)"
diff sieswi_out.csv duck_out.csv  # Should be identical
```

---

## 📚 See Also

- [SQL_ROADMAP.md](./SQL_ROADMAP.md) - Detailed implementation plan
- [PLAN.md](../PLAN.md) - Overall project vision
- [NEXT_STEPS.md](../NEXT_STEPS.md) - Immediate priorities
