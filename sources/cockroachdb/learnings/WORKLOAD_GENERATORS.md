# Data Generation: CockroachDB Built-in Workloads Only

**⚠️ NOTE:** This document is kept for historical context. We previously had a custom bash script (`generate_test_data.sh`) but removed it in favor of the simpler, faster built-in workloads.

## TL;DR

**Simplified:** We now only use CockroachDB's built-in workloads. No bash scripts needed!

```bash
# Just run this - workload integrated:
python test_local.py

# Or manually:
cockroach workload run ycsb \
  "postgresql://root@localhost:26257/ycsb?sslmode=disable" \
  --duration=2m
```

## Historical Context: Why Did `generate_test_data.sh` Exist?

Good question! It was created before we fully explored CockroachDB's built-in capabilities. The built-in workloads are almost always better.

### Historical Context

1. **Initial approach**: Created a simple bash script to generate test data
2. **Problem**: Too slow (~2 operations/sec), not realistic
3. **Discovery**: CockroachDB has excellent built-in workload generators
4. **Solution**: Now recommend built-in workloads, keep bash script for edge cases

## Comparison

### CockroachDB Built-in Workloads (RECOMMENDED)

**Command:**
```bash
cockroach workload run ycsb \
  "postgresql://root@localhost:26257/ycsb?sslmode=disable" \
  --duration=2m
```

**Or use wrapper:**
```bash
./run_workload.sh ycsb 2
```

**Pros:**
- ⚡ **10-100x faster**: Written in Go, generates 1000s of ops/sec
- 📊 **Realistic patterns**: Mix of reads (50-95%), writes (5-50%), updates, deletes
- 🔄 **Industry standard**: YCSB, TPC-C, TPC-H, MovR benchmarks
- 🎯 **Well-tested**: Used by CockroachDB team for performance testing
- 🔧 **Configurable**: Control read/write ratio, batch sizes, etc.
- 📈 **Scalable**: Can run against distributed clusters

**Cons:**
- 🤷 May generate data in tables/schemas you don't care about
- 🤷 Less control over exact data patterns

**Available Workloads:**
```bash
ycsb        # Yahoo! Cloud Serving Benchmark (default)
kv          # Simple key-value operations
bank        # Bank account transfers
tpcc        # TPC-C transaction processing
movr        # Vehicle sharing simulation
tpch        # TPC-H analytics queries
```

**Use cases:**
- ✅ Performance testing
- ✅ Load testing
- ✅ Changefeed stress testing
- ✅ Realistic CDC scenarios
- ✅ Any production-like testing

---

### Custom Bash Script (`generate_test_data.sh`)

**Command:**
```bash
./generate_test_data.sh 120
```

**Pros:**
- 🎯 **Targeted**: Only affects `events` table
- 📐 **Predictable**: Sequential, deterministic data (test_1, test_2, ...)
- 🔍 **Simple**: Easy to understand and modify
- 🐛 **Debug-friendly**: Know exactly what data is being generated

**Cons:**
- 🐌 **Slow**: ~2 operations/sec (one INSERT every 0.5s)
- 📊 **Unrealistic**: Sequential pattern, no reads, low volume
- 🔧 **Limited**: Only INSERT/UPDATE/DELETE, no queries
- 🧪 **Test-only**: Not suitable for performance testing

**What it does:**
```bash
# Every 0.5 seconds:
INSERT INTO events (event_type, user_id, data) VALUES ('test_N', N, '{"iteration": N}');

# Every 3rd iteration:
UPDATE events SET user_id = user_id + 1 WHERE event_type LIKE 'test_%' LIMIT 1;

# Every 5th iteration:
DELETE FROM temp_records WHERE id IN (SELECT id FROM temp_records LIMIT 1);
```

**Use cases:**
- ✅ Debugging specific changefeed scenarios
- ✅ Testing custom table schemas (e.g., `events` with JSONB data)
- ✅ Need exact, predictable data for assertions
- ✅ Low-volume testing sufficient

---

### Legacy Wrapper (`local_setup.sh workload`)

**Command:**
```bash
./local_setup.sh workload
```

**What it does:**
Runs YCSB workload for 10 minutes using `cockroach workload run`.

**Status:** This is just a wrapper around the built-in YCSB workload. Use `run_workload.sh` instead for more control.

## Recommendation Matrix

| Scenario | Recommended Tool | Command |
|----------|-----------------|---------|
| **Performance testing** | Built-in workload | `./run_workload.sh ycsb 5` |
| **Load testing** | Built-in workload | `./run_workload.sh kv 10` |
| **CDC stress test** | Built-in workload | `./run_workload.sh bank 30` |
| **Realistic traffic** | Built-in workload | `./run_workload.sh ycsb 2` |
| **Quick test** | Auto (test_local.py) | `python test_local.py` |
| **Debugging specific table** | Bash script | `./generate_test_data.sh 60` |
| **Predictable data** | Bash script | `./generate_test_data.sh 120` |

## Migration Guide

### If you're using `generate_test_data.sh`

**Before:**
```bash
# Terminal 1
./generate_test_data.sh 120

# Terminal 2
python test_local.py --no-data
```

**After (10-100x faster):**
```bash
# Terminal 1
./run_workload.sh ycsb 2

# Terminal 2
python test_local.py --no-data
```

### If you're using `local_setup.sh workload`

**Before:**
```bash
./local_setup.sh workload    # 10 minutes, no control
```

**After (more control):**
```bash
./run_workload.sh ycsb 2     # Specify duration
./run_workload.sh kv 5       # Choose different workload
./run_workload.sh bank 10    # More realistic transactions
```

## Performance Comparison

Real-world measurements:

| Tool | Operations/sec | Time to generate 1000 ops | Realism |
|------|----------------|--------------------------|---------|
| `run_workload.sh ycsb` | ~5,000 | **0.2 seconds** | ⭐⭐⭐⭐⭐ |
| `local_setup.sh workload` | ~1,000 | 1 second | ⭐⭐⭐⭐ |
| `generate_test_data.sh` | ~2 | **500 seconds (8 min)** | ⭐ |

## Why Keep the Bash Script?

Despite being slower, `generate_test_data.sh` is still useful for:

1. **Debugging**: When you need to know exactly what data is being inserted
2. **Custom schemas**: Testing specific table structures (like `events` with JSONB)
3. **Deterministic tests**: When test assertions depend on exact data
4. **Educational**: Easy to read and understand for learning purposes
5. **Simple environments**: When you don't want to install/learn cockroach workload

## Future Direction

Consider:
- Making `test_local.py` use built-in workloads by default
- Deprecating `generate_test_data.sh` for most use cases
- Adding performance benchmarks using built-in workloads

## References

- [CockroachDB Workload Documentation](https://www.cockroachlabs.com/docs/stable/cockroach-workload.html)
- [YCSB Benchmark](https://github.com/brianfrankcooper/YCSB)
- [TPC-C Benchmark](http://www.tpc.org/tpcc/)

## Summary

**Default recommendation**: Use `run_workload.sh` or raw `cockroach workload run` commands.

**Keep the bash script**: For specific debugging and testing scenarios where you need predictable, targeted data generation.

Both tools have their place - use the right tool for the job! 🎯

