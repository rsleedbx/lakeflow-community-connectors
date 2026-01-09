# What to Watch For in Test Output

**Test Running:** `test_cdc_matrix.sh` with new diagnostic code

---

## 🔍 Key Output to Examine

### For Tests 1, 2, 5, 6 (usertable - DELETE doubling issue)

Look for this section in each test:

```
📊 Analyzing changefeed data...
   📍 Before coalescing: XXX,XXX events
      Operations before: snap=XXXXX, ins=XX, upd=XXX, del=???
      Sample DELETE _cdc_keys: [...]
   📍 After coalescing: XX,XXX events
      Operations after: snap=XXXXX, ins=XX, upd=XXX, del=???
```

**Critical Questions:**

1. **Is `del=` changing from before to after?**
   - `del=200` → `del=100` = Coalescing IS working! ✅
   - `del=200` → `del=200` = Coalescing NOT merging DELETEs ❌

2. **What do Sample DELETE _cdc_keys look like?**
   - Same keys repeated = Should be merged
   - Different keys = That's why they're not merging

---

### For Test 2 (json_usertable_no_split - UPDATE doubling issue)

Compare Test 1 vs Test 2:

**Test 1 (with_split):**
```
Operations before: snap=XXXXX, ins=50, upd=400, del=200
Operations after: snap=XXXXX, ins=50, upd=400, del=???
```

**Test 2 (no_split):**
```
Operations before: snap=XXXXX, ins=50, upd=???, del=200
Operations after: snap=XXXXX, ins=50, upd=800, del=???
```

**Critical Questions:**

1. **Is `upd=` different BEFORE coalescing?**
   - Test 1: `upd=400` before
   - Test 2: `upd=???` before (is it 400 or 800?)

2. **Does snapshot count differ between tests?**
   - Test 1: `snap=~19000`
   - Test 2: `snap=~18600` (400 less)
   - This correlation suggests misclassification

---

## 📊 Diagnostic Patterns

### Pattern A: DELETE Events Not Coalescing

```
Before: del=200
After:  del=200  ← NOT changing!
Sample keys: [('ycsb_key', 'user1')], [('ycsb_key', 'user1')]  ← Same key!
```

**Diagnosis:** Coalescing grouping logic broken  
**Fix:** Check line 832-837 in cockroachdb.py

---

### Pattern B: DELETE Operations Overwritten During Merge

```
Before: del=200
After:  del=100  ← Changed!
But final count still shows: Delete rows: 200
```

**Diagnosis:** Coalescing works, but operation field overwritten  
**Fix:** Check line 850-855 merge logic

---

### Pattern C: UPDATE Misclassification Before Coalescing

```
Test 1: Operations before: upd=400
Test 2: Operations before: upd=800  ← Already wrong!
```

**Diagnosis:** Events classified incorrectly from the start  
**Fix:** Check timestamp comparison at lines 3320-3327

---

### Pattern D: UPDATE Changed During Coalescing

```
Test 2:
  Before: upd=400
  After:  upd=800  ← Doubled during coalescing!
```

**Diagnosis:** Coalescing creating duplicates or changing operations  
**Fix:** Check merge logic at lines 850-855

---

## ⏰ Test Timeline

The test takes ~30 minutes total:
- ~5 min per test × 8 tests = 40 min
- Tests run sequentially
- Each test: create table → snapshot → workload → CDC → analyze

**Watch for diagnostic output in the "Analyzing changefeed data" section of each test.**

---

## 📝 Next Steps After Test Completes

1. **Copy the diagnostic output** for tests 1, 2, 5, 6
2. **Identify which pattern** matches the behavior
3. **Apply the appropriate fix** from RUN_DIAGNOSTIC.md
4. **Re-run test** to validate fix
5. **Update documentation** with findings

---

**Current Status:** Test in progress. Waiting for diagnostic output... ⏳

