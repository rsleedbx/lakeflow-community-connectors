# Multi-Table Snapshot Consistency

## Problem Statement

When ingesting multiple tables in a single pipeline run with `initial_scan='only'`, each table was capturing its snapshot start timestamp independently:

```
T=0s:   Table 1 captures timestamp T0, starts snapshot
T=10s:  Table 2 captures timestamp T10, starts snapshot
T=20s:  Table 3 captures timestamp T20, starts snapshot
```

**Problem:** Changes occurring between T0 and T20 could create inconsistencies:
- A change at T5 affecting both Table 1 and Table 2 would be captured in Table 1's cursor but missed by Table 2
- Referential integrity issues across tables
- Inconsistent point-in-time snapshot

---

## Solution: Shared Snapshot Timestamp

### Implementation

**1. Class-Level Shared Timestamp**
```python
class LakeflowConnect:
    # Shared across all instances in the same pipeline run
    _shared_snapshot_timestamp = None
```

**2. Multi-Table Detection (ingest.py)**
```python
if len(all_tables) > 1:
    default_table_config["multi_table_pipeline"] = "true"
```

**3. Timestamp Sharing (cockroachdb.py)**
```python
if effective_initial_scan.lower() == "only":
    is_multi_table = table_options.get("multi_table_pipeline") == "true"
    
    if is_multi_table and LakeflowConnect._shared_snapshot_timestamp:
        # Reuse shared timestamp from first table
        self._snapshot_start_timestamp = LakeflowConnect._shared_snapshot_timestamp
    else:
        # Capture new timestamp
        self._snapshot_start_timestamp = current_ts
        
        # Store for other tables if multi-table
        if is_multi_table:
            LakeflowConnect._shared_snapshot_timestamp = current_ts
```

---

## How It Works

### Single-Table Pipeline (No Change)

```
Pipeline with 1 table:
├── Table 1
    ├── Capture timestamp: T0
    └── Use for snapshot: T0
```

**Behavior:** Same as before (no shared timestamp needed)

---

### Multi-Table Pipeline (New Behavior)

```
Pipeline with 3 tables:
├── Table 1 (first)
│   ├── Capture timestamp: T0
│   ├── Store as shared: T0
│   └── Use for snapshot: T0
│
├── Table 2
│   ├── Reuse shared: T0 ✅
│   └── Use for snapshot: T0
│
└── Table 3
    ├── Reuse shared: T0 ✅
    └── Use for snapshot: T0
```

**Benefit:** All tables use the same timestamp T0, ensuring consistency!

---

## Example: Consistent Multi-Table Snapshot

### Scenario: E-commerce Database

**Tables:**
- `customers` (primary key: customer_id)
- `orders` (foreign key: customer_id)
- `order_items` (foreign key: order_id)

**Timeline:**
```
T=0s:   Pipeline starts
        Captures shared timestamp: T0 = 1766105060072019839

T=1s:   Start snapshot of `customers` table (uses T0)
T=5s:   💳 New order created (customer_id=123, order_id=456)
T=10s:  Start snapshot of `orders` table (uses T0)
T=15s:  📦 New order item added (order_id=456, item_id=789)
T=20s:  Start snapshot of `orders_items` table (uses T0)
T=30s:  All snapshots complete

Next run (incremental):
        All tables use cursor = T0
        ✅ Order created at T5 captured by all tables
        ✅ Order item at T15 captured by all tables
        ✅ Consistent point-in-time view!
```

---

## Benefits

### 1. **Referential Integrity** ✅

**Without shared timestamp:**
```
customers table: snapshot at T0
orders table: snapshot at T10

New order created at T5:
- ❌ order table has it (T5 < T10)
- ❌ customers table doesn't (T5 > T0)
- ❌ Orphaned order!
```

**With shared timestamp:**
```
Both tables: snapshot at T0

New order created at T5:
- ✅ Both tables will pick it up in next incremental run
- ✅ cursor = T0 for both tables
- ✅ Consistent!
```

---

### 2. **Consistent Point-in-Time** ✅

All tables represent the database state at the exact same moment (T0), not different moments.

---

### 3. **Simplified Debugging** ✅

When troubleshooting data inconsistencies, you can verify that all tables used the same snapshot timestamp from the logs.

---

## Log Output

### Multi-Table Pipeline

```
🕐 Multi-table pipeline detected (3 tables)
   Connector will capture and share a single snapshot start timestamp
   This ensures consistency across all tables

# Table 1 (customers)
🕐 CockroachDB Timestamps:
   Current cluster timestamp: 1766105060072019839.0000000000
   Current wall clock time: 2025-12-19 01:10:00

💡 Capturing snapshot start timestamp (from CockroachDB)...
   📍 Snapshot start: 1766105060072019839.0000000000
   💾 Stored as shared timestamp for other tables

# Table 2 (orders)
🕐 CockroachDB Timestamps:
   Current cluster timestamp: 1766105070123456789.0000000000 (10s later)
   Current wall clock time: 2025-12-19 01:10:10

💡 Reusing shared snapshot timestamp (multi-table consistency):
   📍 Snapshot start: 1766105060072019839.0000000000
   ✅ Same timestamp as other tables in this pipeline

# Table 3 (order_items)
🕐 CockroachDB Timestamps:
   Current cluster timestamp: 1766105080234567890.0000000000 (20s later)
   Current wall clock time: 2025-12-19 01:10:20

💡 Reusing shared snapshot timestamp (multi-table consistency):
   📍 Snapshot start: 1766105060072019839.0000000000
   ✅ Same timestamp as other tables in this pipeline
```

---

## Edge Cases

### Q: What if tables are in different databases?

**A:** Each connection (database) will have its own shared timestamp. Tables within the same database/connection share a timestamp.

---

### Q: What if pipeline is restarted mid-run?

**A:** The class variable is reset on pipeline restart. First table processed after restart will capture a new shared timestamp. This is correct behavior - a new pipeline run should have a new consistent timestamp.

---

### Q: What if incremental run (with cursor)?

**A:** Shared timestamp only applies to `initial_scan='only'` (snapshot mode). Incremental runs use per-table cursors (which is correct - each table progresses independently based on changes).

---

### Q: Can I override this behavior?

**A:** Yes, you can provide explicit `snapshot_start_timestamp` in table configuration:

```python
table_config = {
    "initial_scan": "only",
    "snapshot_start_timestamp": "1766105060072019839.0000000000"
}
```

This will use your provided timestamp instead of capturing one.

---

## Testing

### Verify Shared Timestamp

**1. Run multi-table pipeline:**
```bash
databricks pipelines start-update <pipeline_id>
```

**2. Check logs for all tables:**
```
# Should see for table 1:
💡 Capturing snapshot start timestamp
   📍 Snapshot start: 1766105060072019839.0000000000
   💾 Stored as shared timestamp

# Should see for tables 2+:
💡 Reusing shared snapshot timestamp
   📍 Snapshot start: 1766105060072019839.0000000000
   ✅ Same timestamp as other tables
```

**3. Verify end cursors match:**
```sql
-- Check cursors in Delta Lake metadata
DESCRIBE HISTORY main.catalog.table1;
DESCRIBE HISTORY main.catalog.table2;
DESCRIBE HISTORY main.catalog.table3;

-- All should have same initial cursor timestamp
```

---

## Configuration

### Automatic (Recommended)

```python
# ingest.py
table_list = "customers,orders,order_items"  # Multiple tables

# Automatically sets:
# - multi_table_pipeline = "true"
# - Shared timestamp enabled
```

### Manual Override

```python
# For explicit control
table_config = {
    "initial_scan": "only",
    "snapshot_start_timestamp": "1766105060072019839.0000000000",
    "multi_table_pipeline": "false"  # Disable sharing
}
```

---

## Summary

| Feature | Benefit |
|---------|---------|
| **Shared timestamp** | All tables use same T0 |
| **Automatic detection** | No configuration needed |
| **Referential integrity** | Consistent across tables |
| **Point-in-time snapshot** | Database state at T0 |
| **Debug logging** | Clear indication of shared timestamp |
| **Per-table cursors (incremental)** | Independent progress after snapshot |

**Result:** Multi-table CDC pipelines now have consistent point-in-time snapshots! 🎯








