# SQL Feature Roadmap

This document outlines the SQL features sieswi will implement, prioritized by user value and aligned with the streaming architecture.

---

## ✅ Phase 1 - IMPLEMENTED (Current)

### Basic SELECT
```sql
SELECT col1, col2, col3 FROM file.csv
SELECT * FROM file.csv
```

### Single WHERE Predicate
```sql
WHERE column = 'value'
WHERE column != 'value'
WHERE column > 100
WHERE column >= 100
WHERE column < 100
WHERE column <= 100
```

### LIMIT
```sql
SELECT * FROM file.csv LIMIT 10
SELECT * FROM file.csv WHERE price > 100 LIMIT 5
```

### Type Handling
- Auto-detect numeric values (parseable as float64)
- String comparisons (lexicographic, case-sensitive)
- Type coercion: `"123" == 123` → true

---

## 🎯 Phase 4 - Compound Predicates (Weeks 3-6)

**Impact:** Enable 90% of real-world filtering use cases.

### Phase 4a - Simple AND
```sql
-- All predicates must pass
SELECT * FROM orders.csv 
WHERE country = 'UK' AND price > 100

SELECT * FROM logs.csv 
WHERE status = 'ERROR' AND timestamp >= '2024-01-01'

-- Multiple ANDs
WHERE a = 1 AND b > 10 AND c != 'foo'
```

**Implementation:**
- Extend parser to handle multiple predicates
- Evaluate left-to-right with short-circuit
- Index pruning: intersect candidate blocks

**Test cases:**
```sql
-- Both match
WHERE country = 'UK' AND price > 0  

-- First fails (short-circuit)
WHERE country = 'XX' AND price > 0

-- Numeric + string
WHERE price > 100 AND country = 'UK'
```

---

### Phase 4b - Same-Column OR (IN clause)
```sql
-- Syntactic sugar for OR on same column
SELECT * FROM data.csv 
WHERE country IN ('UK', 'US', 'CA')

-- Equivalent to:
WHERE country = 'UK' OR country = 'US' OR country = 'CA'
```

**Why this first:**
- Common pattern (multi-value filter)
- Index-friendly (union of block candidates)
- Easy to optimize

**Implementation:**
- Parse `IN (value1, value2, ...)`
- Rewrite internally as OR chain
- Single column scan, multiple value checks

**Test cases:**
```sql
-- Numeric IN
WHERE id IN (1, 2, 3)

-- String IN
WHERE status IN ('PENDING', 'APPROVED')

-- Single value (edge case)
WHERE country IN ('UK')

-- Empty list (error case)
WHERE country IN ()
```

---

### Phase 4c - Full Boolean Expressions
```sql
-- Complex nested conditions
SELECT * FROM orders.csv 
WHERE (country = 'UK' AND price > 100) 
   OR (country = 'US' AND price > 200)

-- With NOT
WHERE NOT (status = 'CANCELLED' OR status = 'REFUNDED')

-- Precedence: NOT > AND > OR
WHERE a = 1 AND b = 2 OR c = 3  
-- Parsed as: (a=1 AND b=2) OR (c=3)
```

**Implementation:**
- Recursive descent parser for WHERE clause
- AST nodes:
  - `BinaryExpr{Op: AND/OR, Left, Right}`
  - `UnaryExpr{Op: NOT, Expr}`
  - `Comparison{Col, Op, Val}`
- Evaluator: depth-first with short-circuiting

**Test cases:**
```sql
-- Parentheses override precedence
WHERE (a = 1 OR b = 2) AND c = 3

-- Deep nesting
WHERE ((a = 1 AND b = 2) OR (c = 3 AND d = 4)) AND e = 5

-- NOT with AND/OR
WHERE NOT (a = 1 AND b = 2)
-- De Morgan's: NOT a=1 OR NOT b=2
```

---

## 🔜 Phase 5 - UX Features (Weeks 7-8)

### OFFSET (Pagination)
```sql
SELECT * FROM data.csv LIMIT 100 OFFSET 1000
-- Skip first 1000 rows, return next 100
```

**Use case:** Paginated APIs, data sampling

**Implementation:**
- Counter: skip first N rows
- Then apply LIMIT
- Warning: OFFSET requires scanning skipped rows (not free)

---

### DISTINCT
```sql
SELECT DISTINCT country FROM orders.csv
SELECT DISTINCT country, status FROM orders.csv
```

**Use case:** Find unique values, cardinality analysis

**Implementation:**
- Hash set to track seen combinations
- Memory grows with unique count
- Warning: Defeats streaming if too many unique values

**Optimization:**
- For single column + indexed CSV: read from .sidx distinct sets
- For low cardinality (< 1000 unique): fast
- For high cardinality: warn about memory usage

---

### CASE Expressions (Simple)
```sql
SELECT 
  order_id,
  CASE 
    WHEN price < 10 THEN 'cheap'
    WHEN price < 100 THEN 'medium'
    ELSE 'expensive'
  END as price_tier
FROM orders.csv
```

**Use case:** Data transformation, binning

**Implementation:**
- Evaluate CASE per row
- First matching WHEN wins
- Return computed column

---

### Aliases
```sql
SELECT 
  order_id AS id,
  price_minor AS price
FROM orders.csv
WHERE price > 100
```

**Implementation:**
- Parse `column AS alias`
- Output header uses alias
- WHERE/LIMIT still use original names (or support both)

---

## 🚫 Phase 7+ - Deferred / Maybe Never

### ORDER BY ❌ (Architectural Constraint)
```sql
SELECT * FROM data.csv ORDER BY price DESC
```

**Why NOT:**
- Defeats streaming model (requires full scan + sort)
- Massive memory usage (must buffer all rows)
- DuckDB excels here (columnar, vectorized sorting)

**Alternative:**
- Post-process: `sieswi "..." | sort -t, -k2 -n`
- Documentation: explain UNIX philosophy

---

### GROUP BY / Aggregations ❌ (Phase 7+)
```sql
SELECT country, COUNT(*), AVG(price) 
FROM orders.csv 
GROUP BY country
```

**Why NOT in v1:**
- Requires hash table (memory grows)
- Can't stream results until full scan
- DuckDB's strength (columnar aggregation)

**Future possibility:**
- Streaming approximate aggregations (HyperLogLog, t-digest)
- Top-K via heap (stream top 10 countries)
- Time-windowed aggregations (for logs)

---

### JOINs ❌ (Never)
```sql
SELECT * FROM orders.csv 
JOIN customers.csv ON orders.customer_id = customers.id
```

**Why NOT:**
- Single CSV focus is core value prop
- Multi-table = database territory
- Use DuckDB for this

---

### Subqueries ❌ (Phase 8+)
```sql
SELECT * FROM orders.csv 
WHERE customer_id IN (
  SELECT id FROM customers.csv WHERE country = 'UK'
)
```

**Why NOT in v1:**
- Complex implementation
- Requires multiple passes or memory
- Niche use case

**Alternative:**
- Two queries + shell: `ids=$(sieswi "..."); sieswi "WHERE id IN ($ids)"`

---

### String Functions ❌ (Phase 6+)
```sql
WHERE UPPER(country) = 'UK'
WHERE country LIKE 'U%'
WHERE REGEXP(email, '.*@gmail.com')
```

**Tradeoff:**
- Users will want this
- Each function adds complexity
- Start with most common: UPPER, LOWER, LIKE

**Phase 6 priority:**
- `LIKE` for simple patterns
- `UPPER`/`LOWER` for case-insensitive matching
- Hold on regex (use `grep` post-process)

---

### Date/Time ❌ (Phase 6+)
```sql
WHERE timestamp > '2024-01-01'
WHERE DATE(timestamp) = '2024-01-01'
WHERE timestamp BETWEEN '2024-01-01' AND '2024-12-31'
```

**Current:** Treat as strings (lexicographic comparison works for ISO8601)

**Future:**
- Parse common formats (ISO8601, RFC3339)
- Date arithmetic (add days, diff)
- Time zones (extract, convert)

**Complexity:** High. Consider external lib or keep it simple.

---

## 📊 SQL Feature Priority Matrix

| Feature | User Value | Implementation Cost | Streaming Friendly | Phase |
|---------|------------|---------------------|-------------------|-------|
| AND predicates | 🔥🔥🔥 | Low | ✅ Yes | 4a |
| IN clause | 🔥🔥🔥 | Low | ✅ Yes | 4b |
| OR predicates | 🔥🔥 | Medium | ✅ Yes | 4c |
| OFFSET | 🔥 | Low | ✅ Yes | 5 |
| Aliases | 🔥 | Low | ✅ Yes | 5 |
| DISTINCT | 🔥🔥 | Medium | ⚠️ Limited | 5 |
| CASE | 🔥 | Medium | ✅ Yes | 5 |
| LIKE | 🔥🔥 | Medium | ✅ Yes | 6 |
| UPPER/LOWER | 🔥 | Low | ✅ Yes | 6 |
| ORDER BY | 🔥🔥🔥 | High | ❌ No | Never |
| GROUP BY | 🔥🔥🔥 | High | ❌ No | 7+ |
| JOINs | 🔥🔥 | Very High | ❌ No | Never |

---

## 🎯 Target SQL Coverage (End of Phase 5)

**Supported (~70% of real-world queries):**
```sql
SELECT col1, col2, col3, CASE ... END as alias
FROM file.csv
WHERE (a = 1 AND b > 2) OR c IN ('x', 'y', 'z')
LIMIT 100 OFFSET 50
```

**Not Supported (use DuckDB):**
```sql
-- Sorting
SELECT * FROM file.csv ORDER BY price DESC

-- Aggregations
SELECT country, COUNT(*) FROM file.csv GROUP BY country

-- JOINs
SELECT * FROM a.csv JOIN b.csv ON a.id = b.id
```

---

## 🧪 Test Strategy for Each Feature

### Unit Tests
- Parser: valid syntax, invalid syntax, edge cases
- Evaluator: correct results, short-circuit behavior
- Type handling: numeric, string, mixed

### Integration Tests
- Small CSV (100 rows): verify correctness
- Large CSV (1M rows): verify streaming behavior
- Edge cases: empty results, all matches, no matches

### Benchmark Tests
- Compare vs DuckDB (when applicable)
- Measure memory usage
- Validate performance doesn't regress

---

## 📝 SQL Compatibility Philosophy

**Goals:**
1. **Familiar:** Standard SQL syntax where possible
2. **Predictable:** Clear semantics, no surprises
3. **Fast:** Every feature must maintain streaming performance
4. **Honest:** Document what we DON'T support

**Non-Goals:**
1. Full SQL standard compliance
2. PostgreSQL/MySQL quirks
3. Everything DuckDB does

**Message:** "If you need full SQL, use DuckDB. If you need instant CSV queries, use sieswi."

---

## 🚀 Summary: Next 3 Implementations

1. **Phase 4a (Week 3):** AND predicates
   ```sql
   WHERE country = 'UK' AND price > 100
   ```

2. **Phase 4b (Week 4):** IN clause  
   ```sql
   WHERE country IN ('UK', 'US', 'CA')
   ```

3. **Phase 4c (Week 5-6):** Full boolean expressions
   ```sql
   WHERE (a = 1 AND b > 2) OR (c = 3 AND d < 4)
   ```

After these, 80% of real-world filtering needs are covered. Then focus on .sidx index for 100x speedup! 🎯
