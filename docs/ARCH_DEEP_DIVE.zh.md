# MiniDB 架构带读笔记素材：执行层 / 存储层 / 事务·WAL 层

> 核对方式：逐文件阅读源码，所有类型名、函数名、枚举值、常量值、行号均取自当前工作区（`\\wsl.localhost\Ubuntu\home\first\minidb`）的源码。
> 标注约定：`路径:行号`。凡代码中不存在、无法确认或文档与代码冲突者，均显式标注「未确认」或「文档/代码不一致」。
> 本文档不含推测；凡属按 ABI 推算而非静态断言保证的结论，均单独说明。
> 后续校验：WAL 记录头尺寸已由编译实测确认为 32 字节；「HOT 更新未加载元数据就写盘」已修复并由 `tests/acid/durability/hot_update_metadata_restart.py` 钉住。

---

## 0. 先看结论：文档与代码的 8 处硬冲突（写笔记时最重要）

| # | 文档说法 | 代码事实 |
|---|---|---|
| 1 | WAL 记录头 30 字节，`data_len` 在偏移 26（`docs/WAL_RECOVERY_PROTOCOL.md:10-23`） | `WalRecord`（`src/recovery/wal.h:59-67`）**没有 `#pragma pack`**，且 `write_record` 注释明说 `type`(u16) 与 `data_len`(u32) 之间有 padding（`src/recovery/wal.cpp:162-167`）。头为 **32 字节**，`data_len` 在偏移 28；`sizeof(WalRecord)` 无静态断言。**已独立编译验证**：g++ 对着 `recovery/wal.h` 实测 `sizeof(WalRecord)=32`、`sizeof(WalType)=2`、`offsetof(magic/crc/lsn/txn_id/type/data_len)=0/4/8/16/24/28` |
| 2 | 「No visibility map — IndexOnlyScan always performs a heap recheck」（`docs/KNOWN_LIMITATIONS.md:57`、`docs/QUERY_EXECUTION.md:112,194`、`docs/CAPABILITY_GAP_CHECKLIST.md:32,129`） | Visibility Map **已实现且在 IndexOnlyScan 热路径生效**：`src/storage/visibility_map.h:23-70`、`src/storage/visibility_map.cpp:18-88`；`IndexOnlyScanExecutor::next` 用 `heap_->vm().is_visible(rid.page_id)` 直接跳过堆页读取（`src/sql/executor/index_scan_executor.cpp:472-474`）。GC/VACUUM 负责置位（`src/recovery/gc.cpp:144`、`:262-264`） |
| 3 | 死锁「youngest-aborts」（`docs/TRANSACTION_MVCC.md:723-725`、`docs/ARCHITECTURE.md:349`、`docs/CONCURRENCY_CONTROL.md:139`、`src/concurrency/README.md:36`） | **代码中不存在 youngest/最年轻受害者选择**。受害者恒为发起请求者自己：`detect_deadlock` 只判断「请求者是否处于环中」（`src/concurrency/lock_manager.cpp:405-457`，判环条件 `holders[i] == txn_id` 在 `:449`），检测到即放弃等待并返回 `kLockConflict`（`:182-211`）。记录锁路径 `lock_record` **根本不调用** `detect_deadlock`，只靠 2s 超时，且注释明确「only abort the waiter (self). Never silently revoke another transaction's granted lock」（`:281-284`） |
| 4 | `Status` 取值 `kGranted/kWaiting/kDeadlock/kTimeout/kError`（`src/concurrency/README.md:25-26`） | `ErrorCode` 是唯一状态枚举（`src/common/status.h:13-29`），与本模块相关的只有 `kDeadlock`(`:26`)、`kLockConflict`(`:27`)、`kTxnConflict`(`:28`)、`kBufferFull`(`:20`)、`kPageFull`(`:21`)。**不存在** `kGranted/kWaiting/kTimeout/kError`；`lock_table/lock_record` 成功返回 `Status::ok_status()`，失败返回 `kLockConflict`（`src/concurrency/lock_manager.cpp:143,211,241,310`） |
| 5 | 「Aggregate 在哈希表超过 work_mem 时溢写」（`docs/QUERY_EXECUTION.md:653-663`） | 真实机制是**入口处就切换成排序聚合**：`if (work_mem_bytes_ != 0 && compute_groups_sort_spill()) return;`（`src/sql/executor/aggregate_executor.cpp:218`），而 `compute_groups_sort_spill()` 只有在临时文件创建/写入**失败**时才 `return false`（`:368,379,427,432,466,472,545`）。工厂恒传非零 `work_mem_bytes`（`src/sql/executor/executor_factory.cpp:459`，默认 16MB 见 `src/common/db_config.h:15`），因此**默认配置下哈希聚合分支（`:222-284`）不可达**。因此哈希分支实际是**溢写设施失败时的兜底**；哈希路径里唯一的「超限」处理是直接报错 `"work_mem exceeded during aggregate"`（`:248-250`） |
| 6 | `docs/ACID_TODO.md:326`「currently youngest-aborts, should be oldest-wins」 | 同上，与代码不符（既不是 youngest 也不是 oldest，而是「请求者自杀」） |
| 7 | `kIndexInsert/kIndexDelete` 载荷顺序「index_id + key_len + key_data + rid」（`docs/WAL_RECOVERY_PROTOCOL.md:44-45`） | 代码是 `index_id(u32) + rid.page_id(u64) + rid.slot_idx(u16) + key_size(u16) + key_data`（`src/recovery/wal.cpp:300-320`） |
| 8 | 配置开关 `recover_indexes_lazy`（`src/common/db_config.h:25`，解析于 `src/common/db_config.cpp:145`） | **无任何读取点**。索引重建路径唯一且无条件：`Database` 构造中 `if (wal_->recover(this)) { 全部 IndexEntry::state = kInvalid; rebuild_all_indexes(); flush(); }`（`src/database/database.cpp:97-114`）。即该开关当前语义未实现 |

---

## 1. 执行器接口

### 1.1 `Executor` 基类的真实虚函数签名

文件：`src/sql/executor/executor.h`

```cpp
class Executor {                                   // :79
public:
    virtual ~Executor() = default;                 // :81
    virtual void init() = 0;                       // :82
    virtual ExecResult next() = 0;                 // :83
    virtual const Schema& output_schema() const = 0;                      // :84
    virtual bool fast_count(u64* count) { (void)count; return false; }    // :85-88
    virtual bool fast_plain_aggregate(const Vector<AggregateColumn>& aggregates,
                                      Vector<Value>* row) { ... return false; }  // :89-94
    virtual bool last_record_id(RecordId* rid) const { ... return false; }      // :95-98
};
```

**关键事实：没有 `close()`。** 在 `src/` 全树 grep `void close()` 无匹配。资源释放全部走析构函数，例如：
- `SeqScanExecutor::~SeqScanExecutor` → `release_pinned_page()`（`src/sql/executor/seq_scan.cpp:38-41`、`:43-51`）
- `IndexScanExecutor::~IndexScanExecutor` → `unpin_page(cached_heap_page_id_)`（`src/sql/executor/index_scan_executor.cpp:24-28`）
- `SortExecutor::~SortExecutor` → `cleanup_spill_files()`（`src/sql/executor/sort_executor.cpp:72-74`）
- `HashJoinExecutor::~HashJoinExecutor` → `cleanup_spill()` + 释放 entry（`src/sql/executor/hash_join_executor.cpp:31-36`）

返回值类型（`src/sql/executor/executor.h:70-77`）：

```cpp
struct ExecResult {
    bool has_tuple;   // :71
    Tuple tuple;      // :72
    bool ok() const { return has_tuple; }        // :74
    static ExecResult empty() { return {false, {}}; }              // :75
    static ExecResult ok(Tuple t) { return {true, ...}; }          // :76
};
```

即 **EOF 与失败共用 `has_tuple == false`**，失败必须靠错误通道区分（`src/sql/executor/README.md:20-23` 也是这样描述的）。

### 1.2 `set_executor_error()` 的位置与错误通道

文件：`src/sql/executor/executor.h`

```cpp
inline thread_local const char* g_executor_error_message = nullptr;   // :18
inline void clear_executor_error() { g_executor_error_message = nullptr; }   // :28-30
inline void set_executor_error(const char* message) { g_executor_error_message = message; }  // :32-34
inline const char* executor_error() { return g_executor_error_message; }     // :36-38
```

- 是 **thread_local 的 `const char*`**，只存指针，因此传入的都是**静态字符串字面量**（全树 60+ 处调用均为字面量，唯一例外是 `Insert/Update` 转发 `Schema::validate_row` 的返回指针，见 `src/sql/executor/insert.cpp:280-281`、`src/sql/executor/update.cpp:378-379`）。
- 超时与取消同通道：`set_executor_deadline_ms(u64)`（`:40-48`）、`request_executor_interrupt()`（`:51-53`，仅一个 relaxed atomic store，见 `src/repl/repl.cpp:141-144` 的 SIGINT 说明）、轮询入口 `executor_cancelled()`（`:59-68`）——中断写入 `"interrupted"`，超时写入 `"statement timeout"`。
- `inline std::atomic<bool> g_executor_interrupted{false}`（`:26`）。

### 1.3 `ExecutorFactory`

```cpp
class ExecutorFactory {                       // src/sql/executor/executor_factory.h:17
public:
    explicit ExecutorFactory(Database& db);   // :19
    UniquePtr<Executor> create(PlanNode* plan);   // :21  ← 唯一入口
private:
    UniquePtr<Expression> materialize_scalar_subqueries(const Expression* expr);  // :24
    Value evaluate_scalar_subquery(const SelectStmt* subquery);                   // :25
    Database& db_;                                                                // :27
};
```

`create()` 实现：`src/sql/executor/executor_factory.cpp:116`，核心是 `switch (plan->type)`（`:123`）。真实分支与关键处理：

| 分支 | 行号 | 要点 |
|---|---|---|
| `kOneRow` | `:124-125` | `OneRowExecutor`（`:40-55`） |
| `kSeqScan` | `:127-162` | 表锁 `LockMode::kAccessShare`（`:131-136`）；并行扫描门槛（`:145-153`，见 §2.1）；构造 `SeqScanExecutor`（`:154-156`）；下推谓词 `set_pushed_predicate`（`:157-160`） |
| `kIndexScan` | `:164-189` | AccessShare（`:166-171`）；从 catalog 取 `key_columns` 传入以做索引项 key 重校验（`:178-183`） |
| `kIndexOnlyScan` | `:191-216` | 取单列 key column，`key_columns.size()==1` 否则用 `static_cast<u32>(-1)` 表示「无重校验」（`:209-211`） |
| `kFilter` | `:218-252` | `IN_SUBQUERY/NOT_IN_SUBQUERY` 特判转 `SubqueryInExecutor`（`:220-244`）；否则 `FilterExecutor`（`:249-251`） |
| `kProject` | `:254-275` | 对投影列逐条 `materialize_scalar_subqueries`（`:263-270`） |
| `kInsert` | `:277-291` | `LockMode::kRowExclusive`（`:279-284`） |
| `kDelete` | `:293-310` | 同上 `:295-300` |
| `kUpdate` | `:312-344` | 同上 `:314-319`；SET 值里的标量子查询在此物化（`:329-335`） |
| `kJoin` | `:346-390` | 先试 `JoinAlgorithm::kIndexLookup`（`:351-371`，要求 inner 表/索引 id 非 0 且 join type ∈ {Inner, Left}）；再试 `kHash`（`:372-384`，要求 ON 是 `ExprType::kBinaryOp` 且 `op == "="`）；否则 `NestedLoopJoinExecutor`（`:385-389`） |
| `kLimit` | `:392-399` | |
| `kSort` | `:401-418` | 传 `db_.config().work_mem_bytes, s_plan->top_n, db_.config().temp_file_limit_bytes, db_.config().temp_dir`（`:413-417`） |
| `kDistinct` | `:420-427` | |
| `kAggregate` | `:429-461` | |
| `kUnion` | `:463-472` | |
| `default` | `:474-475` | 返回空 `UniquePtr<Executor>()` |

两个包装器：
- `OneRowExecutor`（`:40-55`）：无表 SELECT 的单行源。
- `TracedExecutor`（`:57-105`）：计时/计数包装，`init()`/`next()` 用 `TraceNodeScope` + `TraceOperatorStats`（`:63-91`），并**透传** `fast_count/fast_plain_aggregate/last_record_id`（`:94-99`）。是否包装由 `maybe_trace_executor`（`:107-114`）决定：`current_trace()` 且 `plan->trace_id != 0`。

`optimizer_config_from_db`（`:31-38`）把 `enable_hashjoin/enable_indexscan/enable_indexonlyscan/storage_mode == "remote"` 映射进 `OptimizerConfig`。

### 1.4 错误如何冒泡到 REPL / Server

**REPL：`REPL::execute_sql(const String& sql)`（`src/repl/repl.cpp:343`）**
1. 取快照事务：`needs_snapshot && !db_.txn_manager().current()` → `db_.txn_manager().begin()`（`:722-729`）。
2. `ExecutorFactory factory(db_); UniquePtr<Executor> exec = factory.create(plan.get());`（`:761-762`）。
3. **创建期失败**：`if (!exec)` → 有隐式事务则 `rollback` → `if (const char* err = executor_error()) printf("Error: %s\n\n", err);`（`:763-771`）。这正是 `ExecutorFactory` 里 6 处 `set_executor_error("could not acquire table lock")`（`src/sql/executor/executor_factory.cpp:134,169,196,282,298,317`）的出口。
4. 运行期：`clear_executor_error(); set_executor_deadline_ms(...)`（`:776-777`）→ 记录语句级保存点 `savepoint_mark = current()->undo_mark()`（`:783-784`）→ `exec->init()`（`:786`）→ 拉取循环（`:799-817`，其中 `executor_cancelled()` 提前 break）。
5. **运行期失败**：`if (executor_error())`（`:822-833`）→ 隐式事务 `rollback`；显式事务 `rollback_to_savepoint(mark)`，失败再整体 `rollback` → `printf("Error: %s\n\n", executor_error())`。
6. 超时单独分支（`:834-845`）打印 `"Error: statement timeout."`（此处不看 `executor_error()`）。
7. EXPLAIN TRACE 路径：`build_trace_json(sql, plan, trace, timings, err)`（`:674`），`err` 来自 `executor_error()`（`:671`）。

**Server：`Server::execute_plan_result(StmtType, PlanNode*)`（`src/network/server.cpp:298`）**
1. 快照事务：`:308-311`。
2. `factory.create(plan)`（`:313-314`）；创建失败 → `return String("Error: ") + err + "\n";`（`:315-321`）。
3. `clear_executor_error(); set_executor_deadline_ms(effective_timeout_ms);`（`:326-327`）；保存点 `undo_mark()`（`:333-334`）。
4. 拉取循环写结果串（`:351-373`）。
5. 出错 → 回滚（隐式 / 保存点）→ `return String("Error: ") + executor_error() + "\n";`（`:375-385`）；超时 `:387-397`；正常则隐式事务 `commit`（`:399-403`）并 `db_.maybe_gc()`（`:407-409`）。

其它同类调用点（仅由 grep 命中，未逐行核对上下文）：`src/network/server.cpp:272-288, 317, 326-327, 375-384, 529-537, 847-862, 1182-1197, 1291-1374`；`src/repl/repl.cpp:656-671, 689-707, 765-831`。

---

## 2. 各算子内部机制

### 2.1 SeqScan（全表扫描 / MVCC）

类定义 `src/sql/executor/seq_scan.h:22-100`；私有 helper 声明 `:50-55`（`try_tuple` / `follow_version_chain` / `follow_latest_committed` / `is_redirect_target` / `should_skip_rid` / `mark_skip_rid`），页级缓存辅助 `:88-90`。

**MVCC 可见性判断在哪个函数**：不在扫描器内部实现，而是调用 `TransactionManager::is_visible(xmin, xmax, txn)`（定义 `src/transaction/transaction.cpp:513`）。四个调用点：
- `src/sql/executor/seq_scan.cpp:145`（REDIRECT 目标分支）
- `src/sql/executor/seq_scan.cpp:176`（普通槽位分支）
- `src/sql/executor/seq_scan.cpp:220`（版本链分支）
- `src/sql/executor/seq_scan.cpp:425`（`fast_count` 快路径）

**版本链怎么跟随**：
- `SeqScanExecutor::follow_version_chain(Page*, u16)`：`src/sql/executor/seq_scan.cpp:193-236`。先按 `storage_schema_` 全量反序列化（`:196-197`），循环条件 `tuple.has_next_version() && depth < kMaxChainDepth`（`:202`），`kMaxChainDepth = 64`（`:200`，函数内 `static constexpr`）；每步 `pool_->fetch_page(ver_page, true)`（`:207`）后按 `next_version_page/next_version_slot` 取版本（`:204-205`）；命中可见版本则 `mark_skip_rid` + `record_read` 并把该 RID 作为 `last_rid_`（`:222-224`），最后用 `deserialize_projected_from_page` 投影输出（`:225-227`）。
- 无事务（autocommit / 无快照）时走 `follow_latest_committed`：`src/sql/executor/seq_scan.cpp:238-281`，判定退化为 `tuple.xmax() == kInvalidTxnId`（`:266`）。
- 同一 RID 可能既被 REDIRECT 槽位又被目标槽位枚举到，用 `skipped_rids_`（线性查找，`:295-306`）去重。

**late materialization**：
- 页内投影反序列化 `Tuple::deserialize_projected_from_page(page->data() + lp->offset, storage_schema_, output_schema_, projected_columns_, lp->length)`（`src/sql/executor/seq_scan.cpp:134-136` 与 `:160-162`）。
- 实现：`src/record/tuple.cpp:264-354`。`projected_columns` 为空时退化成整行解码（`:269-271`）；先建 `source_to_output` 映射（栈上 `stack_map[128]`，超过则堆 `heap_map`，`:302-316`），不需要的列走 `skip_value_bounded`（`:337`），需要的列才 `read_value_bounded` 填值（`:330-336`）。这正是「投影下推到扫描」的落点。
- 注意：**版本链遍历仍用 `storage_schema_` 全量反序列化**（`:196-197`、`:241-242`、`:264-265`），只有最终输出那一次才投影。late materialization 只省了「输出列解码」，没有省「可见性判定所需的头部读取」。

**页级优化**：
- 当前页跨 `next()` 调用保持 pin：`pinned_page_`（`src/sql/executor/seq_scan.h:75-76`），注释见 `:71-74`；`release_pinned_page()`（`seq_scan.cpp:43-51`）在换页与析构时释放。
- REDIRECT 目标位图预扫：`prepare_page`（`seq_scan.cpp:56-71`）把「某槽位是否是 REDIRECT 目标」编码进 `redirect_target_bits_[16]`（`u64`，`kRedirectBitmapWords = 16`，即最多 1024 槽，`seq_scan.h:81-83`），查询 O(1)（`is_redirect_target_cached`，`:73-77`）。旧的 O(N²) 版 `is_redirect_target` 仍保留但**已不在热路径调用**（`:283-293`）。

**并行扫描的开关与实现**：
- 开关（全在工厂里，`src/sql/executor/executor_factory.cpp:145-153`）：
  `!tm && db_.config().enable_parallel_seqscan && scan_plan->projected_columns.empty() && !scan_plan->pushed_predicate && db_.config().parallel_workers > 1 && heap->meta().num_data_pages >= db_.config().parallel_workers * 4`
  → 语义：**只要存在当前事务就永不并行**（`tm == nullptr` 才并行）；无投影、无下推谓词、页数 ≥ worker×4。
  配置默认：`enable_parallel_seqscan = true`、`parallel_workers = 4`（`src/common/db_config.h:46-47`）。注释（`executor_factory.cpp:142-144`）说明原因：`ParallelSeqScan` 会先把所有行读进缓冲且不做谓词求值。
- 实现 `ParallelSeqScanExecutor`（`src/sql/executor/seq_scan.h:102-123`）：`init()`（`seq_scan.cpp:629-680`）为每个 worker 起一个 `std::thread`（`:648-669`），分片 `for (u32 page_idx = w; page_idx < pages; page_idx += workers)`（`:650`），每页只保留 `tuple.xmax() == kInvalidTxnId` 的行（`:662`），结果按页序汇总进 `rows_`（`:674-679`）；`next()` 只是游标出队（`:682-688`）。**没有 MVCC、没有谓词、没有版本链**——这也是工厂层设门槛的原因。

**快路径**：
- `fast_count(u64*)`（`seq_scan.cpp:390-432`）：有下推谓词时直接 `return false`（`:395`）；只 `memcpy` 读 16 字节（xmin/xmax，`:417-420`）；无事务时 `visible = (xmax == kInvalidTxnId)`（`:423`）；每页 `executor_cancelled()` 检查（`:405`）。
- `fast_plain_aggregate`（`seq_scan.cpp:489-620`）：入口即 `if (... || txn_mgr_) return false;`（`:491`）→ **只要扫描器持有事务管理器就永不启用**（而 REPL/Server 对 SELECT 都会 `begin()`，见 §1.4）；遇到「有旧版本」的页立即放弃（`:551-554`）；SUM 用 `widen_int_for_sum` 加宽到 i64（`:448-452`、`:574-576`），AVG 用 `double_sum / count`（`:483-484`）。

### 2.2 IndexScan / IndexOnlyScan

**IndexScanExecutor**（`src/sql/executor/index_scan_executor.h:20-80`、`.cpp`）：
- 批量迭代器：`kBatchSize = 32`（`.h:74`），`IndexKey batch_keys_[32]; RecordId batch_rids_[32];`（`:75-76`），一次 `index_->range_scan_batch(...)` 拿 32 条（`.cpp:194-196`），`scan_leaf_id_ == kNullPageId` 表示索引区间耗尽（`:208`）。
- 堆页 pin 复用：`cached_heap_page_/cached_heap_page_id_`（`.h:57-58`），RID 同页则复用（`.cpp:220-233`）。
- REDIRECT 处理：`.cpp:237-240`。
- **heap recheck 的具体代码位置**：
  - 版本链回溯 + 投影：`VersionResult vr = follow_version_chain(pool_, txn_mgr_, page, visible_slot, output_schema_);`（`.cpp:245-246`）；
  - **key 重校验**：`.cpp:256-268` —— `entry_key.column_count() == key_columns_.size()`，逐列 `vr.tuple.get_value(key_columns_[k]).compare(entry_key.value(k)) != 0` → `key_ok = false` → `continue`（`:260-267`）。注释（`:248-255`）说明动机：UPDATE 改了索引列后旧索引项不会被立即删除，从陈旧项跟随版本链会拿到不再匹配的行（范围扫描下会重复）。
  - 统计钩子：`trace->record_index_recheck(true/false)`（`:265`、`:269`、`:299`）。
  - 语义提示：这里的 recheck 是「索引项 key ↔ 可见堆元组 key 一致性」，**不是** PostgreSQL 的 index qual recheck；可见性本身由 `follow_version_chain` 内部闭包 `check_visible` 用 `txn_mgr->is_visible(...)` 完成（`.cpp:73-78`）。
- 静态 helper：`follow_version_chain`（`.cpp:63-116`，同样 `kMaxChainDepth = 64`，`:83`）、`visible_header`（`.cpp:118-178`，只读 26 字节头部走链，要求 `length >= 26`，`:121-122`、`:135-136`）。
- `fast_count`（`.cpp:314-412`）：只有「全部数据页 all-visible」时才用 `index_->range_count(...)`（`:324-330`）；`key_columns_` 非空时直接放弃（`:337`，因为逐项检查不校验 key）；否则逐项 `visible_header`（`:403`），VM 命中的 RID 直接计数（`:373-381`）。

**IndexOnlyScanExecutor**（`src/sql/executor/index_scan_executor.h:82-123`、`.cpp:420-529`）：
- 只输出 `key.first_value()` 构造的一列元组（`.cpp:516-518`）。
- **heap recheck 的具体代码位置**：
  - VM 快路径：`bool vm_visible = heap_ && heap_->vm().is_visible(rid.page_id);`（`.cpp:472`）——all-visible 则**完全跳过堆页 fetch**；
  - 未命中 VM 时：fetch 堆页（`:476`）→ REDIRECT 处理（`:480-485`）→ 若配置了单列重校验列则 `follow_version_chain(pool_, txn_mgr_, page, visible_slot, recheck_schema_)`（`:497-498`）+ `actual.compare(key.first_value()) == 0`（`:501`）；否则退回 `visible_header`（`:504`）。
  - 重校验列由工厂给出，仅当索引是单列时（`executor_factory.cpp:209-211`），否则为 `static_cast<u32>(-1)`（`.cpp:488`）。

### 2.3 HashJoin

文件：`src/sql/executor/hash_join_executor.h` / `.cpp`。
- 桶数 `static constexpr u32 BUCKET_COUNT = 8192;`（`.h:60`），链式 `HashJoinEntry* buckets_[8192]`（`.h:61-62`）+ `Vector<HashJoinEntry*> all_entries_` 持有（`.h:62`）。
- Grace 分区数 `static constexpr u32 GRACE_PARTITIONS = 32;`（`.h:77`），`build_partitions_[32]` / `probe_partitions_[32]`（`.h:78-79`）。
- 构造默认 `temp_dir = "/tmp"`（`.h:34`），成员构造时兜底 `"/tmp"`（`.cpp:25`）。

**build/probe 侧选择**：
- 成员初始化 `build_left_(join_type == JoinType::kInner && build_left)`（`.cpp:23`）→ **LEFT JOIN 恒为「build 右侧」**，只有 INNER 才允许 `build_left = true`。
- `build_left` 来自 `JoinPlan::hash_build_left`（`src/sql/planner/plan_node.h:131`），工厂传参 `j_plan->hash_build_left`（`src/sql/executor/executor_factory.cpp:382`）。
- 键归属由 `can_evaluate_on(expr, schema)`（`.cpp:135-165`）判定：`kSubquery` 直接返回 false（`:161-162`）；四种归属组合在 `:176-192`，两侧都判不出时默认左/右直接赋值（`:189-192` 兜底而非报错）。
- 建表：`build_hash_table()`（`.cpp:167-254`），build 侧执行器 `build_left_ ? left_ : right_`（`:207`）。

**Grace hash spill 的触发条件与临时文件命名**：
- 触发：`.cpp:225-236` —— 先 `memory_used += t.serialized_size() + sizeof(HashJoinEntry) + 32;`（`:225`），再 `if (work_mem_bytes_ != 0 && memory_used > work_mem_bytes_)`（`:226`）→ `spilled_ = true`，清空 `all_entries_` 与 8192 个桶（`:231-235`），之后只继续往 spill 文件写（`:237` 的 `if (spilled_) continue;`）。
- **spill 文件在 build 一开始就被 mkstemp 打开并「边读边写」**（`:194-205`），不是超限后才创建；超限时如果文件没建成则报 `"failed to create hash join spill file"`（`:227-230`）。
- 文件命名（均由 `mkstemp` 生成，模板含 6 个 X）：
  - 首轮 build spill：`minidb_hash_join_XXXXXX`（`:196`，拼接前会补 `/`，`:194-195`）
  - Grace build 分区：`minidb_hj_build_XXXXXX`（`:287`）
  - Grace probe 分区：`minidb_hj_probe_XXXXXX`（`:308`）
- 分区函数：`static_cast<u32>(hash_value(key) % GRACE_PARTITIONS)`（build `:342`、probe `:366`），NULL 键归入 0 号分区（`:342`、`:366` 的三元表达式）。
- 磁盘格式：`u32 length` + `tuple.serialize_to_page` 的裸字节（`write_spill_tuple` `.cpp:86-94`；`read_spill_tuple` `.cpp:96-109`，长度上限 `64U*1024U*1024U`，`:100`）。
- 清理：`cleanup_spill()`（`.cpp:61-84`）`fclose` + `unlink` 全部路径；`prepare_grace_partitions` 成功后立即删掉初始 spill 文件（`:375-378`）。
- 分区装载：`load_next_partition()`（`.cpp:384-422`）按 `current_partition_` 递增，build 分区整表载入内存哈希表（`:398-415`），probe 侧只 `fopen` 流式读（`:417-419`）。
- **work_mem 如何读取**：构造参数 `u64 work_mem_bytes = 0`（`.h:32`）→ 实参 `db_.config().work_mem_bytes`（`executor_factory.cpp:382`）→ `DbConfig::work_mem_bytes = 16ULL*1024*1024`（`src/common/db_config.h:15`）。`temp_dir` 同理来自 `db_.config().temp_dir`（`executor_factory.cpp:383`，默认 `"/tmp"`，`db_config.h:19`）。
- `hash_value`（`.cpp:111-129`）：整型取原值、float/double 按位 memcpy 当整数、VARCHAR 用 `h = h * 31 + c`（`:119-125`），最后 **`return h % BUCKET_COUNT`**（`:128`，返回的已是桶号）。
- 连接判定用 `Value::operator==`（`:452`、`:520`），未走 `ExpressionEvaluator::compare_values`，与谓词路径的跨类型比较语义不同（差异点，文档未说明）。
- LEFT JOIN 空补：非 spilled `:457-469`；spilled `:526-536`。

### 2.4 Sort

文件：`src/sql/executor/sort_executor.h` / `.cpp`。
- 构造默认 `work_mem_bytes = 0, top_n = -1, temp_file_limit_bytes = 0, temp_dir = "/tmp"`（`.h:19-22`）。
- **`materialize()` 由 `next()` 首次调用触发**（`.cpp:319`），不在 `init()`；`init()` 只重置状态并清理旧 spill（`.cpp:76-87`）。

**内存排序 vs 外部归并**：
- 内存路径：`buffer_.sort(comparator)`（`.cpp:129-131`，注释说明 O(n log n) 替代旧的 O(n²) 插入排序）。
- 外部路径触发：`.cpp:106-114` —— `memory_used += r.tuple.serialized_size() + 64;`（`:104`）后 `if (work_mem_bytes_ != 0 && memory_used > work_mem_bytes_)` → `write_sorted_run()`（`:107`）→ `spilled_ = true`、清空 buffer、`memory_used = 0`。
- run 文件：`minidb_sort_XXXXXX`（`.cpp:205`），格式 `u32 length` + 元组字节（`:221-246`），长度上限 64MB（`:257`）。
- 归并：`init_merge()`（`.cpp:273-300`）为每个 run 开一个 `RunCursor` 并建堆 `merge_heap_`（`std::make_heap` + `worse_run` 比较器 `:295-298`）；`next()` 每次 `pop_heap` 取最小 run 头并补读下一条（`:324-343`）。
- `temp_file_limit` 检查：`if (temp_file_limit_bytes_ != 0 && temp_bytes_ + write_bytes > temp_file_limit_bytes_)`（`.cpp:224`）→ `set_executor_error("temp_file_limit exceeded during sort")`（`:227`）。实参来自 `db_.config().temp_file_limit_bytes`（工厂 `:417`，默认 10GB，`db_config.h:18`）。

**Top-N 堆的触发条件**：
- `if (top_n_ >= 0) { materialize_top_n(); return; }`（`.cpp:91-94`）——**判定是 `>= 0` 而不是 `> 0`**：`top_n == 0` 也进 Top-N 分支，并在 `materialize_top_n` 开头因 `if (top_n_ <= 0)` 直接置 `materialized_ = true` 返回空（`:138-141`）。
- `SortPlan::top_n` 由 planner 设置：`if (stmt.limit >= 0) sort->top_n = stmt.limit + (stmt.offset > 0 ? stmt.offset : 0);`（`src/sql/planner/planner.cpp:290`、`:588`、`:690`）。
- 堆实现：`top_heap_`（`std::vector<Tuple>`）+ `worse_first`（`.cpp:143-145`）；未满则 `push_heap`（`:156-160`）；已满则 `if (compare_tuples(tuple, top_heap_.front()) < 0)` 才替换堆顶（`:161-165`）；最后把堆内容拷回 `buffer_` 再整体排序（`:168-174`）。
- 比较器 `compare_tuples`（`.cpp:178-196`）：逐键求值；NULL 按 `nulls_first` 决定（`:184-189`）；`ascending` 取反（`:192`）；跨类型数值比较走 `compare_values_for_sort`（`:46-57`，同类型直接 `Value::compare` 保持 int64 精度）。

### 2.5 Aggregate

文件：`src/sql/executor/aggregate_executor.h` / `.cpp`。
- **聚合在 `init()` 内一次算完**：`AggregateExecutor::init()` → `compute_groups()`（`.cpp:31-36`），`next()` 只是游标吐 `result_groups_`（`.cpp:548-552`）。

**哈希聚合 vs 排序聚合的选择**：
- 有 GROUP BY 时：`if (work_mem_bytes_ != 0 && compute_groups_sort_spill()) return;`（`.cpp:218`）。
- `compute_groups_sort_spill()`（`.cpp:287-546`）**所有 return 都是 `true`**（`:368`(取消) `:379`(写 run 失败) `:427`(纯内存完成) `:432`(写 run 失败) `:466`/`:472`(打开/读失败) `:545`）。
- 工厂恒传 `db_.config().work_mem_bytes`（`executor_factory.cpp:459`，默认 16MB 非 0）。
- ⇒ **默认配置下走的一律是「排序聚合」**（即便数据全部装得下内存：无 run 时在 `.cpp:383-428` 做内存排序 + 顺序分组），`:222-284` 的哈希聚合路径只在 `work_mem_bytes_ == 0` 时可达。
- 哈希路径的超限处理是**报错**：`set_executor_error("work_mem exceeded during aggregate")`（`.cpp:248-250`），不做溢写。
- 排序聚合落盘：run 文件 `minidb_agg_XXXXXX`（`.cpp:325`）；每条记录先用 `key_for_tuple` 预算分组键并随元组一起缓冲（`struct Keyed { String key; Tuple tuple; }`，`.cpp:312`），比较器只比 `a.key < b.key`（`.cpp:322`、`:384`），注释（`:305-311`）解释这是为消除「每次比较都重新求值 group 表达式 + 分配 String」的开销；内存累计 `r.tuple.serialized_size() + kt.key.size() + 64`（`.cpp:374`），超限写 run（`:377`）；有 run 时 k 路归并（`worse_cursor` 比较器 `:483-485`）。

**AVG（及 SUM）在溢写/合并时如何处理 sum/count**：
- 累加器 `struct AggState { u64 count; Value value; bool has_value; HashMap<String,bool> seen; };`（`.cpp:38-45`）。
- 累加逻辑 `advance_agg(AggState*, AggFunc, const Value& input, bool distinct = false)`（`.cpp:86-128`）：DISTINCT 用 `make_values_key` + `seen` 去重（`:88-94`）；COUNT 只加计数（`:95-99`）；空输入直接 return（`:100`）；首个非空值经 `widen_int_for_sum` 加宽（`:102-105`，函数 `:71-75`）；SUM/AVG 共用 `state->value = state->value + input; state->count++;`（`:112-116`）。
- 终结 `finalize_agg`（`.cpp:130-150`）：COUNT → `i64(count)`；SUM/MIN/MAX → `state.value`；**AVG → `numeric_value_as_double(state.value) / static_cast<double>(state.count)`**（`:142-147`，函数 `:47-64`）。
- **关键结论：代码中不存在「部分聚合状态（partial agg）合并」**。溢写到 run 的是**原始输入元组**，合并阶段对同一 key 的所有元组重新调用 `advance_agg`（k 路归并路径 `:507-540`；纯内存路径 `:403-425`），因此 `sum/count` 是被**重算**而非跨 run 合并。所谓「AVG 溢出合并 sum/count」在本实现中对应的是「AVG 永远以 sum(64 位加宽)/count 表示」，不是 partial-state merge。
- HAVING：在分组产出后用 `Tuple check(output_schema_, row)` 求值，非 BOOL 报 `"HAVING expression must be BOOL"`（`having_passes`，`.cpp:77-84`）；四处调用：`:204-208`、`:278-282`、`:396-401`、`:499-505`。
- 无 GROUP BY 的两条快路径：`count_only` 判定（`.cpp:156-164`）→ `child_->fast_count(&cnt)`（`:167`）；`child_->fast_plain_aggregate(aggregates_, &fast_row)`（`:177`）。
- 注意：`ProjectExecutor`（`src/sql/executor/project.h:12-29`）与 `FilterExecutor`（`src/sql/executor/filter.h:21-35`）**都没有 override `fast_count`**，因此这两种子节点下快路径落到基类返回 false；全树 `fast_count` override 只有 `SeqScan`（`seq_scan.h:32`）、`IndexScan`（`index_scan_executor.h:31`）、`HashJoin`（`hash_join_executor.h:39`）、`IndexLookupJoin`（`index_lookup_join.h:34`）、`TracedExecutor`（`executor_factory.cpp:94`）。

### 2.6 Distinct

`src/sql/executor/distinct_executor.h:14-32` / `.cpp`：
- **全部去重也在 `init()` 内完成**（`.cpp:19-206`），`next()` 只吐 `seen_`（`.cpp:216-220`）。
- 内存路径：`HashMap<String,bool> keys`（`.cpp:23`）+ `make_tuple_key(t)`（`.cpp:87`）+ `Vector<Tuple> seen_`（`.cpp:104`）。
- 溢写触发：`if (work_mem_bytes_ != 0 && memory_used > work_mem_bytes_)`（`.cpp:90`、`:108`），累加 `t.serialized_size() + key.size() + 96`（`:89`、`:106`），run 文件 `minidb_distinct_XXXXXX`（`.cpp:39`），run 内排序用 `make_tuple_key` 比较（`:34-36`），归并用 `worse_cursor`（`:178-180`）+ 相同 key 只留一条（`:191-195`）。
- `tuples_equal`（`.cpp:208-214`）**在 next/init 路径中未被调用**（仅声明的辅助函数）。

### 2.7 Insert

`src/sql/executor/insert.h:18-44` / `.cpp`。`next()` 一次性执行（`executed_` 守卫 `.cpp:227-228`），输出单列 `affected_rows` INT32（`.cpp:105-108`、`:397-398`）。

**WAL 写入顺序（WAL-first 两阶段，代码注释 `.cpp:337`）**：
1. 校验与约束：`schema_.validate_row`（`:280-283`）→ `check_constraint_violation`（`:284-287`，函数 `.cpp:52-84`，语义：NULL → UNKNOWN → 通过，`:67`）。
2. 唯一键**逻辑锁**：`db_->lock_manager().lock_key(txn_id, table_id_, scoped, LockMode::kRowExclusive)`（`:296-300`），键名是 `String(static_cast<u64>(g)) + '|' + key`（`:293-295`）。
3. 唯一性检查：`violates_unique_constraints`（`:308-311`，函数 `.cpp:120-224`）；有唯一索引时走 `tree->search(lookup_key)` + `tuple_live_for_unique_check`（`:164-171`，函数 `.cpp:86-97`，用 `TxnState`/`snapshot_id` 判断旧版本是否仍「活着」）；无索引时**全堆扫描**（`:184-222`）。批内重复用 `pending_unique_keys`（`:313-318`）。
4. 构造元组：`tuple.set_xmin(txn_id); tuple.set_xmax(0);`（`:321-322`）；`db_->validate_index_keys`（`:323`）；`serialized_size() > kPageSize` → `"tuple too large for page"`（`:328-332`）；`serialize_to_page`（`:334-335`）。
5. **预留槽位（RAII）**：`auto prepare = heap_->prepare_insert(size);`（`:338`）→ `InsertReservation`（`:344`），其析构自动放堆闩（`src/storage/heap_file.h:83`）。
6. 行锁：`lock_record(..., predicted_rid, kRowExclusive)`（`:348-352`），失败 → `"could not serialize access due to concurrent update"`。
7. **写 WAL**：`lsn = wal_->log_insert(txn_id, table_id_, ins_page, ins_slot, buffer, size);`（`:356`）；`lsn == 0` → `"WAL write failed during insert"` 并放弃（`:360-363`，注释 `:357-359` 解释 LSN=0 会让 BufferPool 在 `page_lsn <= durable_lsn` 时不刷日志就落盘）。
8. **提交数据**：`reservation.commit(buffer, size, lsn)`（`:366`）；失败则先写补偿记录 `wal_->log_savepoint_undo_insert(...)`（`:369-374`）再报 `"heap insert failed"`（`:376`）。
9. **堆 undo 先于索引**：`txn_mgr_->record_insert(table_id_, record_id);`（`:384-386`）→ `db_->insert_index_entries(table_id_, tuple, record_id)`（`:387`），失败 → `"index insert failed"`（`:388`）。注释 `:381-383` 明确这是为了回滚时同时清掉堆行与部分索引项。
10. autocommit 场景释放行锁/键锁/表锁（`:399-409`）；无索引唯一约束场景会先取**表级 `kExclusive` 锁**（`need_table_lock` 判定 `:254-277`，加锁 `:276`）。

**索引维护**：`Database::insert_index_entries`（`src/database/database.cpp:1012-1043`）—— 每个索引先 `wal_->log_index_insert`（`:1026`），**`idx_lsn == 0` 就直接 return false 不改 B+ 树**（`:1027-1031`），再 `tree->insert(key, rid)`（`:1033`）。NULL 键列不索引（`:1023`）。

### 2.8 Update（含 HOT 与 Halloween）

`src/sql/executor/update.h:25-54` / `.cpp`。输出单列 `affected_rows`（`.cpp:144-147`）。

**HOT 式同页更新**：
- 资格判定：先由 SET 子句解析出被改列 `modified_cols`（`.cpp:262-266`），再 `bool hot_eligible = catalog_ ? !catalog_->any_column_indexed(table_id_, modified_cols) : false;`（`:269`）。即**只要被改列上没有任何索引**（含唯一/主键）就走 HOT。
- HOT 路径（`:448-520`）：
  1. `heap_->prepare_insert_in_page(old_rid.page_id, size)`（`:449`）→ `InPageReservation`（`src/storage/heap_file.cpp:388-412`，`predict_slot` 预留槽位）。
  2. `wal_->log_update(txn_id, table_id_, old_page, old_slot, old_page, hot_slot, buffer, size)`（`:455-458`），**新旧页相同**即 HOT 的日志特征；`lsn == 0` → `"WAL write failed during update"`（`:459-462`）。
  3. `reservation.commit(buffer, size, lsn)`（`:465`）；失败时**不允许**掉到非 HOT 分支重复记一条 kUpdate，而是写补偿 `log_savepoint_undo_insert` + `log_savepoint_undo_delete` 后报 `"HOT update install failed"`（`:466-482`，注释 `:467-468`）。
  4. `heap_->commit_old_tuple(old_page, old_slot, new_page, new_slot, txn_id, lsn)`（`:486-487`）—— **单次持页操作内**写 next_version（offset +16/+24）、xmax（offset +8）并盖 page LSN（实现 `src/storage/heap_file.cpp:340-372`）；失败则 `rollback_insert(new)` + 两条补偿 WAL + `"failed to invalidate old tuple version"`（`:492-506`）。
  5. undo 记录：`txn_mgr_->record_hot_delete(table_id_, old_rid); txn_mgr_->record_hot_insert(table_id_, new_record_id);`（`:514-515`）。
  6. **HOT 不触碰任何索引**（因此也没有 `insert_index_entries`）。
- 非 HOT 回退（`:525-608`）：`prepare_insert`（`:526`，可能落到 FSM 选中的别的页/新页）→ `log_update(old→new)`（`:536-539`）→ `commit`（`:546`）→ `commit_old_tuple`（`:566-567`）→ `record_delete(old) + record_insert(new)`（`:589-590`）→ `db_->insert_index_entries(table_id_, new_tuple, new_record_id)`（`:599`）。**旧版本的索引项故意不删**（注释 `:592-598`：SI 下较旧快照仍需通过该索引找到旧行，交给 GC 清）。

**Halloween 问题如何规避**（代码注释直接点名）：
- `.cpp:295-317`：**在开始任何改写之前，先把 WHERE 侧子执行器抽干成 RID 列表** `materialized_targets`，并用 `HashMap<String,bool> seen_targets` + `record_id_key(rid)`（`:119-124`）去重。注释 `:295-301` 写明：若边扫描边写，HOT 写入的同页新版本会满足「自己的写可见」，下一次 `child_->next()` 会再次匹配谓词，从而循环到页满——即经典 Halloween problem。因此「物化 RID」是唯一的规避手段（HOT 与非 HOT 分支都受益）。
- 之后主循环只遍历 `materialized_targets`（`:321-324`），每轮 `db_->read_tuple(table_id_, schema_, old_rid, &old_tuple)` 重新读当前版本（`:326`）。

**写写冲突检测（I2）**：`lock_record(old_rid, kRowExclusive)`（`:346-350`）→ `pool_->fetch_page(old_rid.page_id)` 重读 `xmax`：`std::memcpy(&cur_xmax, p->data() + lp->offset + 8, 8);`（`:369-370`），`cur_xmax != kInvalidTxnId && cur_xmax != txn_id` → `"could not serialize access due to concurrent update"`（`:372-375`）。注释 `:327-330` 强调**不静默跳过 xmax != 0 的行**。

**其它**：类型强制 `cast_value_for_column`（`:31-45`）；SET 表达式在**旧元组**上求值（`:341`）；唯一键锁与检查在 HOT 判定之后、安装之前（`:390-420`）；`validate_index_keys`（`:429-432`）。

### 2.9 Delete

`src/sql/executor/delete.h:17-35` / `.cpp`。输出单列 `deleted_rows`（`.cpp:14-17`）。
- 逐行：`lock_record`（`:40-44`）→ `lsn = wal_->log_delete(txn_id, table_id_, rid.page_id, rid.slot_idx)`（`:54`）→ `heap_->mark_deleted_if_current(page, slot, txn_id, lsn, &conflict)`（`:61-62`）。
- 失败补偿：`wal_->log_savepoint_undo_delete(...)`（`:67-72`），随后按 `conflict` 报 `"could not serialize access due to concurrent update"` 或 `"could not read row for delete"`（`:74-76`）。注释 `:63-65` 说明不补偿会导致「COMMIT + 崩溃后重放这条 delete」。
- **索引项故意不删**（注释 `:47-51`）：交给 GC，索引扫描靠可见性过滤。
- `record_delete`（`:79-81`）。流式扫描（**没有像 UPDATE 那样物化 RID**），因为 delete 只改 xmax 不改谓词结果。

### 2.10 trace_report

`src/sql/executor/trace_report.h`：
```cpp
struct TraceTimings { u64 plan_us; u64 executor_create_us; u64 execute_us; u64 actual_rows; };  // :9-14
const char* plan_node_type_name(PlanNodeType type);                                // :16
void assign_trace_node_ids(PlanNode* plan);                                        // :17
String build_trace_json(const String& sql, const PlanNode* plan,
                        const TraceContext& trace, const TraceTimings& timings,
                        const char* error);                                        // :18-20
TraceOptions trace_options_from_statement(u8 level, u32 channels, const String& events_path);  // :21-22
```
- `plan_node_type_name` 实现 `src/sql/executor/trace_report.cpp:22-39`，覆盖全部 15 个 `PlanNodeType`（枚举定义 `src/sql/planner/plan_node.h:18-21`）；另有 `child()` 抽取的 `case PlanNodeType`（`trace_report.cpp:47-75`、`:175-187`）。
- 采样点由 `TracedExecutor` 提供（`executor_factory.cpp:57-105`）：`init_calls/init_us`、`next_calls/next_us/output_rows`；`TraceContext` 的其它埋点在扫描器内部（`record_version_chain_step` `seq_scan.cpp:203`、`record_index_batch` `index_scan_executor.cpp:197-199`、`record_index_recheck` `:265/:269`、`record_heap_filter` `:289`）。
- REPL 输出：`build_trace_json(...)` 结果直接 printf（`src/repl/repl.cpp:674`），`err` 取自 `executor_error()`（`:671`）。

### 2.11 compiled_predicate / expression_evaluator

**CompiledPredicate**（`src/sql/executor/compiled_predicate.h` / `.cpp`）：
- 接口：`bool compile(const Expression* expr, const Schema& schema)`（`.h:44`）、`bool passes(const Tuple& tuple) const`（`.h:48`）、`bool compiled() const { return root_ >= 0; }`（`.h:52`）。
- 节点种类 `enum class Kind : u8 { kLiteral, kColumn, kCompare, kAnd, kOr, kNot, kIsNull, kIsNotNull };`（`.h:56-59`）；`u8 op_code`（`.h:61`，注释即映射 `0 ==, 1 !=, 2 <, 3 >, 4 <=, 5 >=`），实现常量 `constexpr u8 kCmpEq = 0, kCmpNe = 1, kCmpLt = 2, kCmpGt = 3, kCmpLe = 4, kCmpGe = 5;`（`.cpp:12`）。
- `compile_node`（`.cpp:27-94`）**只支持** 字面量、列引用、一元 `NOT/IS_NULL/IS_NOT_NULL`、二元 `AND/OR/6 种比较`；其它一律 `return -1`（`.cpp:93`）→ 调用方回退 AST 解释执行（`SeqScanExecutor::init` `seq_scan.cpp:96-101`、`FilterExecutor::next` `filter.cpp:25-35`、`IndexScanExecutor::next` `index_scan_executor.cpp:272-287`）。
- `passes` 是**单遍线性求值**（`.cpp:96-199`），`eval_stack_` 在 compile 时一次性 `assign(nodes_.size(), Value())`（`.cpp:23`，成员 `mutable std::vector<Value>` `.h:73`）。
- 比较统一走 `ExpressionEvaluator::compare_values(l, r)`（`.cpp:119`），注释 `.cpp:113-118` 说明原因：裸 `Value` 运算符在类型不同时按 type-id 排序，会导致「编译路径 ≠ 解释路径」。
- 3VL：AND（`.cpp:132-152`）、OR（`:153-172`）、NOT（`:173-182`）；NULL 视为不通过（`:191-198`）；任何非 BOOL 操作数 → `set_executor_error("predicate expression must be BOOL")`（`:139`、`:160`、`:177`、`:195`）。

**ExpressionEvaluator**（`src/sql/executor/expression_evaluator.h` / `.cpp`）：
- 四个静态方法：`evaluate`（`.h:17`）、`fast_evaluate`（`.h:22`，返回 bool 表示是否走了快路径）、`predicate_truth`（`.h:27`，返回 false 表示语义错误）、`compare_values`（`.h:35`，类型感知比较）。
- `predicate_truth`（`.cpp:96-102`）：NULL → 返回 true 且 `*truth = false`；非 BOOL → 返回 false（调用方据此报错）。
- `fast_evaluate` 覆盖三种形状：`col <op> literal`（`.cpp:134-158`）、`literal <op> col`（`:161-187`，比较符左右翻转 `:181-184`）、`col IS [NOT] NULL`（`:190-205`）。`literal-op-col` 分支只支持 6 个比较符，`+ - * /` 不算快路径。
- `evaluate`（`.cpp:210-373`）：NULL 传播（比较 `:233-237`、算术 `:272-276`）、AND/OR 3VL（`:240-269`）、`LIKE` 通配实现（`like_match` `.cpp:105-130`）、`COALESCE/NULLIF`（`:286-294`）、`CASE`（`:349-362`）、`CAST`（`:364-369`）；`ExprType::kSubquery` 直接返回 NULL（`:346-347`，这也是工厂要在建树期物化标量子查询的原因）。
- `compare_values` → `sql_compare_values`（`.cpp:35-57`）：数值族统一转 double 比较（`:36-40`），datetime 族按微秒比较并在必要时 cast（`:41-55`）。

---

## 3. 存储层

### 3.1 页格式常量（真实定义位置与值）

`src/common/config.h`：
```cpp
constexpr u32 kPageSize         = 8192;   // :12
constexpr u32 kPageHeaderSize   = 24;     // :13
constexpr u32 kLinePointerSize  = 6;      // :14  offset(2)+length(2)+flags(2)
constexpr u32 kPageTailReserved = 8;      // :15  bytes reserved at page tail (next_page_id)
constexpr u32 kDefaultPoolFrames = 256;   // :18
constexpr u32 kMaxPoolFrames     = 65536; // :19
constexpr u32 kBTreeOrder        = 128;   // :22
constexpr u32 kWalBufferSize     = kPageSize;  // :27
constexpr u32 kMaxLogRecordSize  = 256;        // :28
constexpr u32 kMaxPageChainHops  = 1'000'000;  // :38
```
- `PageHeader`（`src/storage/page.h:44-51`）：`u64 page_id; u64 lsn; u16 page_type; u16 free_space_offset; u16 num_tuples; u16 reserved;`，`#pragma pack(push,1)` + `static_assert(sizeof(PageHeader) == kPageHeaderSize)`（`:54-55`）。
- `LinePointer`（`src/storage/page.h:62-80`）：`u16 offset; u16 length; u16 flags;` + `static_assert(sizeof(LinePointer) == 6)`（`:83`）。
- 行指针标志：`LP_UNUSED = 0; LP_NORMAL = 1; LP_REDIRECT = 2; LP_DEAD = 3;`（`src/storage/page.h:34-37`）。
- `enum class PageType : u16 { kHeapData=1, kHeapMeta=2, kIndexData=3, kIndexMeta=4, kFreeList=5, kWalPage=6, kCatalogData=7 };`（`src/storage/page.h:20-28`）。
- `page.h:5-6` 布局注释：`[PageHeader 24B] [LinePointers 6B each] [FreeSpace] [TupleData]`。
- 数据区上界 `kDataUpperBound = kPageSize - kPageTailReserved = 8184`（`src/storage/page.cpp:15`）；元组按 8 字节 MAXALIGN 对齐（`page.cpp:10-12`、`heap_file.cpp:11-13`）。
- 页尾 8 字节存 `next_page_id`：写于 `heap_file.cpp:114-115`（旧页链接）、`:129-131`（新页置空）、`:270`（commit 新页）、`:286`（commit 链接）、恢复路径 `:795`；读取于 `fsm.cpp:71`（`raw + kPageSize - sizeof(PageId)`）。
- **页内元组头**（`src/record/tuple.cpp:36`）：`static constexpr u32 kTupleHeaderSize = 8 + 8 + 8 + 2 + 4;  // 30 bytes`，即 `[xmin 8][xmax 8][next_page 8][next_slot 2][num_cols 4][null_bitmap][values]`（布局注释 `tuple.h:5-6` 与 `tuple.cpp:33`）。序化实现 `tuple.cpp:38-73`。
  - 执行器里常见的 `length < 26` / `+26 > kPageSize`（如 `seq_scan.cpp:538`、`index_scan_executor.cpp:121`、`heap_file.cpp:354`、`:607`、`:655`、`:728`）是**MVCC 头前缀 26 字节**（xmin8+xmax8+next_page8+next_slot2），不是完整 30 字节元组头；只用 16 字节的地方（`seq_scan.cpp:415`、`heap_file.cpp:501`、`update.cpp:364`）是只读 xmin/xmax 的场景。

### 3.2 `Page` 类提供的读写方法签名

`src/storage/page.h:89-134`：
```cpp
Page();                                                  // :91  memset 0
void init(PageId page_id, PageType type);                // :94
PageHeader* header();  const PageHeader* header() const;// :97 / :100
byte* data();  const byte* data() const;                 // :105 / :106
LinePointer* line_pointer(u16 idx);                      // :109
const LinePointer* line_pointer(u16 idx) const;          // :110
SlotIdx insert_tuple(const byte* tuple_data, u16 length);                  // :114
SlotIdx insert_tuple_at(const byte* tuple_data, u16 length, SlotIdx target_slot);  // :115
bool mark_dead(SlotIdx idx);        // :116
bool reclaim_slot(SlotIdx idx);     // :117
bool redirect_slot(SlotIdx idx, SlotIdx target_slot);      // :118
SlotIdx redirect_target(SlotIdx idx) const;                // :119
const byte* get_tuple_data(SlotIdx idx) const;             // :121
u16 get_tuple_length(SlotIdx idx) const;                   // :122
u16 prune();                                               // :126
u16 get_free_space() const;                                // :129
bool has_enough_space(u16 tuple_size) const;                // :130
private: byte data_[kPageSize];                            // :133
```
实现要点：
- `line_pointer(idx)` 偏移 = `kPageHeaderSize + idx*kLinePointerSize`，越界返回 nullptr（`page.cpp:32-42`）。
- `insert_tuple` 两策略：① 复用可回收槽（DEAD 槽原地覆盖当 `aligned_len <= max_align(lp->length)`，否则记住 UNUSED 槽备用）`page.cpp:54-69`；② 从页尾向下分配并写行指针 `:72-102`，失败返回 `kNullSlot`（`:83`、`:86`）。
- `insert_tuple_at`：`target_slot > num` 直接失败；`target_slot < num` 且该槽已 valid 也失败（`page.cpp:109-113`）——这是 WAL-first 预留槽位能保证「预测槽位可用」的基础。
- `prune()`：先探测有无 DEAD（无则立即返回 0，`page.cpp:198-206`），再用 **8KB 栈缓冲**（`:219`）做两遍 memcpy 压缩，最后 `hdr->free_space_offset = kPageHeaderSize + num_tuples*kLinePointerSize`（`:237`）。
- `get_free_space()`（`:261-275`）、`has_enough_space()`（`:277-306`）。

### 3.3 `HeapFile`：插入/扫描/更新接口与 FSM 选页逻辑

接口（`src/storage/heap_file.h`）：
```cpp
Result<Pair<PageId, SlotIdx>> insert_tuple(const byte* data, u16 length, u64 lsn = 0);  // :44 单阶段
class InsertReservation { ... PageId page_id(); bool is_new_page(); SlotIdx predicted_slot();
                          Result<Pair<PageId,SlotIdx>> commit(const byte*, u16, u64 lsn); }; // :53-89
Result<InsertReservation> prepare_insert(u16 length);                                    // :91 WAL-first 两阶段
class InPageReservation { ... commit(const byte*, u16, u64 lsn); };                       // :94-123 HOT
Result<InPageReservation> prepare_insert_in_page(PageId page_id, u16 length);             // :125
bool commit_old_tuple(PageId, SlotIdx, PageId next_page, SlotIdx next_slot, u64 xmax, u64 lsn); // :128
void set_page_lsn(PageId, u64 lsn);                                                       // :133
Result<Pair<PageId, SlotIdx>> insert_tuple_in_page(PageId, const byte*, u16, u64 lsn = 0); // :136
bool rollback_insert(PageId, SlotIdx, u64 lsn = 0);                                        // :141
bool rollback_delete(PageId, SlotIdx, u64 lsn = 0);                                        // :144
bool mark_deleted(PageId, SlotIdx, u64 xmax, u64 lsn = 0);                                 // :147
bool mark_deleted_if_current(PageId, SlotIdx, u64 xmax, u64 lsn, bool* conflict);           // :148
bool set_xmin(PageId, SlotIdx, u64 xmin, u64 lsn = 0);                                     // :152
bool freeze_tuple(PageId, SlotIdx, u64 lsn = 0);                                           // :157
bool set_next_version(PageId, SlotIdx, PageId next_page, SlotIdx next_slot, u64 lsn = 0);   // :160
bool mark_dead(PageId, SlotIdx, u64 lsn = 0);                                              // :164
bool prune_obsolete_version(PageId, SlotIdx, u64 oldest_active_txn, u64 committed_xmax, u64 lsn = 0); // :165
bool recover_insert_at(PageId, SlotIdx, const byte*, u16, u64 lsn, bool* out_new_physical_tuple = nullptr); // :168
bool recover_update(PageId old_pid, SlotIdx old_slot, PageId new_pid, SlotIdx new_slot, u64 xmax, const byte*, u16, u64 lsn); // :170
PageId first_data_page_id() const;                                                        // :175
class LatchGuard { explicit LatchGuard(HeapFile& heap); ~LatchGuard(); };                  // :186-194
FreeSpaceMap& fsm();  VisibilityMap& vm();                                                // :222 / :226
```
- 元数据 `struct HeapMeta { u32 table_id; u32 reserved; u64 first_data_page_id; u64 last_data_page_id; u32 num_data_pages; u64 num_tuples; u64 num_deleted_tuples; };`（`.h:24-32`），**每 1024 次变更才落盘**：`note_meta_changed()`（`.cpp:886-892`，阈值 `>= 1024`）；`flush_meta()`（`:880-884`）；`save_meta()`（`:894-906`）；`load_meta()`（`:867-878`，读 `page->data() + kPageHeaderSize`）。
- **没有独立的「扫描」接口**：扫描由执行器直接用 `first_data_page_id()`（`.cpp:846-849`）+ `heap_->meta().num_data_pages` 线性推页（`seq_scan.cpp:111-115`、`:404`）。

**FSM 选页逻辑**（`HeapFile::prepare_insert`，`src/storage/heap_file.cpp:183-239`）：
1. 尚无数据页 → 直接给新页 `(new_pid, is_new_page=true, slot 0)`（`:188-191`）。
2. 先试 `meta_.last_data_page_id`（`:194`）：`prune()`（`:205`，返回 >0 才 mark_dirty）→ `has_enough_space(length)`（`:209`）→ `predict_slot`（`:210`，实现 `.cpp:153-175`，**不修改页**，镜像 `Page::insert_tuple` 的槽位选择）。
3. last page 满 → **FSM**：`PageId fsm_pid = fsm_.find_page(length);`（`:217`），要求 `fsm_pid != kNullPageId && fsm_pid != last_pid`（`:218`）；命中页再 prune/`has_enough_space`；若 FSM 记录陈旧（空间不足）→ `fsm_.update(fsm_pid, fsm_page->get_free_space())` 修正（`:231`）。
4. 都不行 → `allocate_new_page_id()`（`:237`，实现 `.cpp:855-859`：`page_num = meta_.num_data_pages + 1`）。
- FSM 本身（`src/storage/fsm.h` / `.cpp`）：一页一个 `u8` 类别，`fsm_encode(free) = min(free/32, 255)`（`.h:25-28`）、`fsm_decode(cat) = cat*32`（`.h:31-33`）、`fsm_needed_category(size) = ceil((size+6)/32)`（`.h:37-41`，+6 是 LinePointer 开销）；`update`/`find_page` 都是**线性扫描 Vector**（`.cpp:11-21`/`:23-32`，加 `Mutex latch_`）；`rebuild` 沿页链重建（`.cpp:55-75`）。
- 空间回收后更新 FSM 的点：`commit`（`.cpp:303`、`:323`）、`mark_dead`（`:637`）、GC（`gc.cpp:136`、`:259`）。
- VM 失效点：任何 INSERT（`.cpp:307`、`:328`）、DELETE（`:518`、`:559`）、GC（`gc.cpp:138`、`:147`、`:260`、`:266`）。

### 3.4 `BufferPool`：帧状态机、分区、LRU 与防污染、WAL-first

**帧状态机：代码里没有对应的 `enum`。** 状态由 4 个字段的组合表达，注释在 `src/storage/buffer_pool.h:53-64` 命名了 4 个状态：
```
Empty:    page_id == kNullPageId, pin_count == 0, !is_io_in_progress       // :54
Loading:  page_id 已在分区 page_table 中占位, pin_count == 1, io == true    // :55-56
Resident: 已映射, !is_io_in_progress, pin_count ∈ 0..N                      // :57
Evicting: 旧映射已删, 新 page_id 已占位, io == true                          // :58
```
`struct Frame`（`.h:65-80`）：`Page page; PageId page_id; std::atomic<u32> pin_count; std::atomic<bool> is_dirty; bool is_io_in_progress; u32 partition_idx; LinkedList<FrameIdx>::Node* lru_node;`。
真实迁移（`src/storage/buffer_pool.cpp`）：
- **Resident 命中**：读锁下 `pin_count.fetch_add(1)` 直接返回，**不动 LRU**（`:58-75`，注释 `:72` 明说 LRU 更新推迟到 unpin 或接受近似）；若命中但 `is_io_in_progress` 则走等待（`:63-66` → `:128-133`）。
- **未命中**：写锁下二次检查（`:88-101`）→ `find_victim_frame`（`:104`）→ 记录 `evict_page_id` / 计算 `need_wal_flush`（`:111-112`，判据 `victim_frame.is_dirty && evict_page_id != kNullPageId && wal_mgr_ && victim_frame.page.header()->lsn > wal_mgr_->durable_lsn()`）→ `pin_count.store(1)` + `is_io_in_progress = true`（`:113-114`）→ 删旧映射、装新映射（`:115-117`）。
- **Phase 2 无锁 I/O**（`:136-165`）：先 WAL-first（`:139-144`，失败则 `restore_evicted_frame` 回滚映射并返回 `kIOError`），再 `page_store_->write_page(evict_page_id, data, page.header()->lsn)`（`:145-146`），再读新页（`:155`）；读失败时把帧清成 Empty（`:157-164`）。
- **Phase 3 写锁下提交元数据**（`:168-181`）：`pin_count = 1`、`is_dirty = false`、`is_io_in_progress = false`、装映射、**LRU 定位**（见下）。

**分区数**：
- 构造 `BufferPool(PageStore* page_store, u32 pool_size, u64 wait_timeout_ms = 5000, u32 max_waiters = 1024, u32 partitions = 1, u32 flush_batch_size = 64)`（`.h:84-86`）。
- `if (partition_count_ > pool_size_) partition_count_ = pool_size_ == 0 ? 1 : pool_size_;`（`.cpp:23`）；帧按 `u32 part = i % partition_count_` 静态归属（`.cpp:26-30`）；`partition_for(PageId) = page_id % partition_count_`（`.cpp:454-456`）。
- Database 传入 `config_.buffer_pool_partitions`（`src/database/database.cpp:81`），默认 **16**（`src/common/db_config.h:71`）。
- 分区结构 `struct BufferPoolPartition { HashMap<PageId, FrameIdx> page_table; LinkedList<FrameIdx> lru_list; mutable RwLock latch; };`（`.h:47-51`）。

**LRU 与顺序扫描防污染**：
- 顺序提示判定（`.cpp:47-52`）：`bool sequential_hint = is_sequential || (last_fetch_page_id != kNullPageId && file_id_from_page(last_fetch_page_id) == file_id_from_page(page_id) && page_num_from_page(last_fetch_page_id) + 1 == page_num_from_page(page_id));`，用 `static thread_local PageId last_fetch_page_id` 记住上一次 fetch——即**即使调用方不传 `is_sequential`，同一文件内连续递增页号也会被判定为顺序访问**。
- 装入后的定位（`.cpp:176-180`）：`if (sequential_hint) partition.lru_list.move_node_to_back(...) else move_node_to_front(...)`。
- 链表语义（`src/container/linked_list.h:98-122`）：front = head = MRU 保留端；back = tail。
- 淘汰端：`find_victim_frame` 用 `for (auto it = partition.lru_list.rbegin(); ...)`（`.cpp:441`），`rbegin` 从 **tail** 开始（`linked_list.h:207`）→ **tail 是先被淘汰的一端**。
- 结论：**随机访问页进 MRU（front），顺序扫描载入的页被放到 tail，成为下一次淘汰的首选**——这就是「防污染」的实际手段（不是 PostgreSQL 的 midpoint insertion）。`find_victim_frame` 注释 `:430-438` 亦如此描述，并说明两遍策略：优先返回空帧（`f.page_id == kNullPageId`，`:445`），否则记住第一个 `pin_count == 0` 的可淘汰帧（`:446-449`）。

**脏页刷盘前的 WAL-first 检查函数**：
```cpp
bool BufferPool::flush_frame_wal_first(Frame& frame) {          // buffer_pool.cpp:512
    if (!wal_mgr_) return true;                                 // :513
    u64 page_lsn = frame.page.header()->lsn;                    // :514
    if (page_lsn > wal_mgr_->durable_lsn() && !wal_mgr_->flush_until(page_lsn)) return false;  // :515-517
    page_store_->set_durable_lsn(wal_mgr_->durable_lsn());      // :518
    return wal_mgr_->durable_lsn() >= page_lsn;                 // :519
}
```
调用点：淘汰预算 `need_wal_flush`（`:111-112`、`:226-227`）、淘汰执行（`:139`）、`new_page`（`:253`）、`flush_page`（`:368`）、`flush_all`（`:390`）。
相关：`WalManager::flush_until(u64 lsn)`（`src/recovery/wal.cpp:531-540`）在 `durable_lsn_ >= lsn` 时直接返回 true（快速路径，正是 checkpoint 回调期不死锁的原因）。

**其它**：
- `unpin_page`（`.cpp:294-310`）：读锁 + CAS 递减（`:303-306`），减到 0 时 `notify_buffer_available()`（`:307-309`）。
- `mark_dirty`（`:316-331`）/ `set_page_lsn`（`:333-353`）：都只需读锁（`is_dirty` 是 atomic；LSN 单调推进 `if (lsn > cur)`，`:345-348`）。
- `flush_page`（`:359-378`）、`flush_all`（`:380-424`，按 `flush_batch_size_` 组批调 `page_store_->write_pages`，逐分区持写锁）。
- 等待机制：`wait_for_buffer_slot()`（`:475-493`，超时/等待者上限由 `wait_timeout_ms_`、`max_waiters_` 控制）、`notify_buffer_available()`（`:495-498`）。
- `stats()`（`:532-550`）返回 `BufferPoolStats{hits, misses, waiters, wait_timeouts, wait_rejections, dirty_pages, partitions}`（`buffer_pool.h:24-32`）。
- 一处可疑行为：`fetch_page` Phase 3 里**再次** `insert_page_mapping(partition, page_id, victim)`（`:174`），而 Phase 1 已经装过一次（`:117`）；是否产生重复键取决于 `src/container/hash_map.h` 的 `operator[]` 语义（未在本次核对范围内）。

### 3.5 `DiskManager`：fd 缓存与 doublewrite 写路径

接口（`src/storage/disk_manager.h:15-51`）：`read_page(PageId, byte*)`、`write_page(PageId, const byte*)`、`create_file`、`delete_file`、`allocate_file_id`、`flush`；私有 `recover_double_write()`、`write_page_direct(pid, data, fsync_after)`、`read_page_direct`、`page_to_path`、`page_to_offset`、`get_fd`。
- 构造：`DiskManager(const String& db_path)` 委托给 `(path, doublewrite=true, checksum=true, fd_cache_limit=1024)`（`src/storage/disk_manager.cpp:48-49`，与 `src/common/db_config.h:78-80` 默认一致）；建 `<db>/catalog|tables|indexes|wal` 四个目录（`:56-67`）；`if (doublewrite_enabled_) recover_double_write();`（`:68`）。
- **fd 缓存**：`HashMap<String,int> fd_cache_`（`.h:45`）+ `Mutex latch_`；`get_fd(path)`（`.cpp:257-272`）：命中直接返回（`:259-260`）；未命中 `open(path, O_RDWR|O_CREAT, 0644)`（`:262`，失败不缓存 `:263`）；**容量超限时把整个缓存全部 close 并重建空 map**（`.cpp:264-269`，粗粒度清空而非 LRU 淘汰；README 自称 "LRU file descriptor cache"）；`create_file` 复用 `next_file_id_++` 并把 fd 塞进缓存（`:137-148`）；`delete_file` 先 close 再 unlink（`:150-159`）；`flush()` 对所有缓存 fd `fsync`（`:166-171`）；析构 close 全部（`:71-78`）。
- 页路径映射 `page_to_path`（`.cpp:238-250`）：`file_id == 1` → `<db>/catalog/1.cat`；`file_id < 1000` → `<db>/tables/<file_id>.heap`；否则 `<db>/indexes/<file_id>.btree`。偏移 `page_to_offset = page_num * kPageSize`（`:252-255`）。
- 校验和：`kChecksumSeed = 0xA5A5`（`:17`）；`page_checksum` 逐字节累加+循环左移，**跳过字节 22/23**（即 `PageHeader::reserved`，`:22`）；`write_page` 先把 `hdr->reserved` 清零再写入校验和（`:99-102`）；`read_page` 校验失败 → `std::memset(page_data, 0, kPageSize)`（`:84-93`，注释 "Torn/corrupt page: zero the buffer"）；`page_has_checksum` 以 `page_type != 0 && reserved != 0` 判定（`:29-32`）。
- **doublewrite 写路径**（`write_page`，`.cpp:95-131`）：
  1. `byte page_copy[kPageSize]` 拷贝 + 算校验和（`:96-102`）。
  2. `open(doublewrite_path_, O_RDWR|O_CREAT)`（`:105`，`doublewrite_path_ = db_path + "/doublewrite.bin"`，`:53`）。
  3. 写 `DoubleWriteHeader{u64 magic; PageId page_id; u16 checksum; u16 reserved; u32 page_size;}`（`:34-42`，packed，`magic = kDoubleWriteMagic = 0x4D44425752495445` = "MDBWRITE"，`:16`）到偏移 0，紧接着 `pwrite` 8KB 页镜像，然后 `fsync`（`:107-116`），`close`（`:117`）。
  4. `write_page_direct(pid, page_copy, false)` 写主文件（`:120`，内部循环 `pwrite` 直到写完，`:190-208`）。
  5. **再次 open** doublewrite 文件，写全零头并 `fsync`，close（`:122-130`）。
  - 特征：**每次页写 2 次 open/close + 2 次 fsync，单页粒度**，不是 PostgreSQL 的批量 doublewrite buffer。
- 崩溃恢复：`recover_double_write()`（`.cpp:210-232`）—— 打开 doublewrite，校验 `magic == kDoubleWriteMagic && page_size == kPageSize`（`:215-216`），再校验页镜像 checksum（`:218-219`），通过则 `write_page_direct(dw.page_id, page, /*fsync_after=*/true)`（`:221`），最后把文件 truncate 并 fsync（`:225-231`）。

### 3.6 PageStore 抽象（概览）

`src/storage/page_store.h`：
- 请求/响应结构：`PageReadRequest{PageId page_id; byte* data;}`（`:15-20`）、`PageWriteRequest{PageId page_id; const byte* data; LSN page_lsn;}`（`:22-29`）、`PageIOResult{PageId page_id; Status status;}`（`:31-37`）。
- 抽象接口（`:39-53`）：`read_page`、`write_page`、`flush`、`delete_file` 纯虚；`read_pages`/`write_pages` 默认实现是**逐个循环**（`src/storage/page_store.cpp:5-27`）；可选 `set_durable_lsn` / `durable_lsn` / `is_remote`（`:50-52`）。
- `LocalPageStore`（`:55-81`）直接转发 `DiskManager`，`write_page` 忽略 `page_lsn`（`:64-69`）。
- 远端模式（概览，未逐行核对）：`PageServer`、`RemotePageStore`、`PageServerTcp`、`RemotePageStoreClient`；`Database` 构造在 `storage_mode == "remote"` 时装配 `RemotePageStore`（`src/database/database.cpp:60-77`）。`src/storage/README.md:80-88` 自述局限（replica 只是目录、无 Raft、无 failover、无分布式锁、无多写者分布式事务、远程 redo 存整页镜像）。

---

## 4. 事务与并发

### 4.1 `TransactionManager` 关键 API 签名（真实）

`src/transaction/transaction.h`：
```cpp
explicit TransactionManager(Database* db);                       // :155
Transaction* begin();                                            // :159
bool commit(Transaction* txn);                                   // :160
bool rollback(Transaction* txn);                                 // :161
bool rollback_to_savepoint(Transaction* txn, u32 mark);           // :166
void record_insert(u32 table_id, const RecordId& rid);            // :167
void record_delete(u32 table_id, const RecordId& rid);            // :168
void record_hot_insert(u32 table_id, const RecordId& rid);        // :169
void record_hot_delete(u32 table_id, const RecordId& rid);        // :170
Transaction* current() const;                                    // :173
u64 next_snapshot_id();                                          // :174
bool is_visible(u64 xmin, u64 xmax, const Transaction& txn) const;  // :180
bool is_txn_committed(u64 txn_id) const;                         // :182
bool get_txn_state(u64 txn_id, TxnState* out) const;             // :183
u64  get_commit_id(u64 txn_id) const;                            // :184
u64  get_oldest_active_txn_id() const;                           // :185
bool has_active_transactions() const;                            // :188
void ensure_next_txn_id_at_least(u64 next_id);                   // :189
u64  next_txn_id() const;                                        // :190
void set_default_isolation(IsolationLevel level);                // :198
TxnStatusLog* status_log() const; void set_status_log(UniquePtr<TxnStatusLog>);  // :204-205
```
`Transaction` 侧：
```cpp
u64 id() const; u64 snapshot_id() const; TxnState state() const; u64 commit_id() const;  // :107-110
const Vector<UndoRecord>& undo_records() const;   // :111
const Vector<ReadRecord>& read_set() const;       // :112
const Vector<u64>& active_snapshot() const;       // :113
IsolationLevel isolation() const;                 // :114
void record_insert/record_delete/record_hot_insert/record_hot_delete(u32, const RecordId&);  // :121-124
void record_read(u32 table_id, const RecordId& rid);   // :128
void record_ddl(UndoType type, u32 table_id, DdlUndoInfo&& info);   // :130
u32 undo_mark() const { return undo_records_.size(); }   // :135   ← 语句级保存点就是 undo log 长度
void truncate_undo(u32 mark);                            // :138
```
- `undo_mark()` **不是新机制**：它就是 `undo_records_.size()`（`:135`），配合 `truncate_undo(mark)`（实现 `.cpp:81-97`，同时同步截断并行的 `ddl_undo_infos_`）。
- `commit()` 实现（`src/transaction/transaction.cpp:197-307`）的**顺序非常关键**：
  1. SSI 冲突检测前置（`:211-215`，见 §4.3）；
  2. `u64 commit_lsn = db_->wal().log_commit(txn_id);`，`== 0` 则走 `rollback`（`:222-229`）——**先 WAL 落盘，再翻 slot 状态**；
  3. 拿 `latch_` 后 `commit_id = next_txn_id_++`、`slot->state = kCommitted`、`txn->set_commit_id/set_state`（`:235-254`）；
  4. 把 `undo_records_` 的 (table_id, rid) 投影写入 `committed_history_` 并 `prune_committed_history()`（`:261-274`）；
  5. 释放 latch 后：`db_->commit_ddl_deferred(...)`（`:281-283`）、`status_log_->record(txn_id, kCommitted)`（`:287`）、对每条 `kDelete/kHotDelete` undo 调 `heap->prune_obsolete_version(rid, oldest_active, txn_id, commit_lsn)`（`:289-298`）、`lock_manager().unlock_all(txn_id)`（`:300`）。
- `rollback()`（`:403-452`）：先把 slot 置 `kAborted`（`:411-417`）→ `status_log_->record(xid, kAborted)`（**在释放锁之前**，`:419-425`，注释解释 C1 竞态）→ `log_abort`（`:427`）→ `unlock_all`（`:430`）→ **逆序** `apply_undo_record(..., for_savepoint=false, ...)`（`:435-444`）。`apply_undo_record`（`:317-401`）：DDL 类型（`static_cast<u8>(rec.type) >= 10`）分派到 `db->undo_*`（`:321-349`）；`kInsert` → `delete_index_entries` + `rollback_insert`（`:358-369`）；`kHotInsert` → 只 `rollback_insert`（`:370-378`）；`kDelete/kHotDelete` → `rollback_delete`（清 xmax 即恢复可见，索引项不动，注释 `:388-389`）。
- `rollback_to_savepoint`（`:454-480`）：逆序 apply，`abort_lsn = 0`，`for_savepoint = true`（要求写补偿 WAL，失败返回 false 让调用方整体 abort，`:470-476`），最后 `truncate_undo(mark)`（`:478`）。

### 4.2 快照结构体的真实字段 + 可见性判断

**没有独立的 `Snapshot` 结构体。** 相关结构：
```cpp
struct TxnSlot {                    // transaction.h:94-100
    u64     txn_id;
    u64     snapshot_id;
    u64     commit_id;              // 0 = 未提交
    TxnState state;
    PageId  home_page;              // 注释标注 (reserved)
};
enum class TxnState : u8 { kActive = 0, kCommitted = 1, kAborted = 2 };   // :83
enum class IsolationLevel : u8 { kSnapshot = 0, kSerializable = 1 };      // :89-92
static constexpr u64 kInvalidTxnId = 0;   // :76
static constexpr u64 kFrozenTxnId  = 2;   // :81
```
`Transaction` 私有字段（`:141-150`）：`txn_id_, snapshot_id_, commit_id_, state_, active_snapshot_(Vector<u64>), undo_records_, ddl_undo_infos_, read_set_, isolation_, resource_acquired_`。**「快照」= `snapshot_id_` + `active_snapshot_` 这一对**（`.cpp:13-18` 构造）。

快照取值（`begin()`，`.cpp:152-195`）：单遍扫描槽位，收集所有 `kActive` 的 txn_id 作为 `active_snapshot`（`:168-175`），然后
```cpp
u64 txn_id = next_txn_id_++;          // :181
u64 snapshot_id = next_txn_id_;       // :182  注释：Snapshot = 下一个 ID
```
→ 即 `snapshot_id = txn_id + 1`；`begin()` 末尾 `db_->wal().log_begin(txn_id)`（`:192`）、`g_current_txn = txn`（`:193`，`thread_local` 定义 `.cpp:11`）。槽位数来自 `db->config().max_active_transactions`（默认 256，`db_config.h:61`），`alloc_slot` 线性找空闲槽（`.cpp:134-146`）。

**可见性判断函数**：`bool TransactionManager::is_visible(u64 xmin, u64 xmax, const Transaction& txn) const`（`src/transaction/transaction.cpp:513`）。完整布尔表达式（按代码顺序）：

```cpp
if (xmin == kInvalidTxnId) return false;                          // :515
if (xmin == kFrozenTxnId) return xmax == kInvalidTxnId;           // :516
if (xmin == txn.id()) return (xmax != txn.id());                  // :517  （自己的写可见）
if (xmin < txn.snapshot_id()) {                                   // :553
    if (was_active_in_snapshot(xmin)) return false;               // :554
    if (is_uncommitted(xmin))         return false;               // :555
    if (xmax == kInvalidTxnId)        return true;                // :557
    if (xmax == txn.id())             return false;               // :558
    if (xmax >= txn.snapshot_id())    return true;                // :559  删除发生在我 begin 之后
    if (was_active_in_snapshot(xmax)) return true;                // :560  删除者当时还在跑
    if (is_uncommitted(xmax))         return true;                // :561  删除被回滚/正在中止
    return false;                                                 // :562  删除已提交
}
return false;                                                     // :567  xmin >= snapshot_id
```
两个闭包：
- `was_active_in_snapshot(id)`：线性扫 `txn.active_snapshot()`（`:519-527`）。
- `is_uncommitted(xid)`（`:533-545`）：**先查持久 CLOG** `status_log_->status(xid, &s)`，命中且 `s == kAborted` → true；否则查 live slot `get_txn_state`，`!= kCommitted` → true；两者都查不到 → false（按 SI 假定「早已结束 = 已提交」）。

与文档公式的差异（写笔记要强调）：
- `docs/TRANSACTION_MVCC.md:252-259` 给的是经典 SI 公式（只看 xmin/xmax 与 snapshot 的关系）；
- **代码额外插入了 `status_log` / live-slot 的 abort 判定**（`:554-555`、`:560-561`），用于修补「快照建立后才有事务中止」的可见性竞态（`.cpp:548-552` 注释称之为 C1）；
- **`commit_id` 完全不参与可见性判断**（`get_commit_id` 只被别处调用），文档若声称「按 commit_id 与 snapshot_id 比较」即为过度简化；
- `xmin >= snapshot_id` 一律不可见（`:565-567`），没有例外分支。

### 4.3 SSI-lite：读写集结构名与冲突检测位置

- 读集元素：`struct ReadRecord { u32 table_id; RecordId rid; };`（`transaction.h:71-74`）。
- 写集（提交历史）：`struct CommittedWriteSet { u64 commit_id; u64 txn_id; Vector<ReadRecord> writes; };`（`transaction.h:216-220`），存放于 `Vector<CommittedWriteSet> committed_history_`（`:221`）。
- 记录读集：`Transaction::record_read(u32, const RecordId&)`（`.cpp:20-36`）——**仅 `isolation_ == kSerializable` 才记**（`:21`），线性去重（`:26-31`）。调用点：`seq_scan.cpp:147`、`:181`、`:224`、`index_scan_executor.cpp:294-296`（`if (track_reads)`，判定见 `seq_scan.cpp:91-92` 与 `index_scan_executor.cpp:183-184`）。
- **冲突检测函数**：`bool TransactionManager::ssi_check_conflict(const Transaction& txn) const`（`.cpp:662-682`）：
  - 跳过自己 `entry.txn_id == txn.id()`（`:669`）；
  - 跳过 `entry.commit_id < snapshot`（我 begin 之前就提交的，`:670`）；
  - 双重循环找交集：`reads[r].table_id == wr.table_id && reads[r].rid == wr.rid` → `return true`（`:671-679`）。
- 触发位置：`commit()` 内、**写 commit 记录之前**（`.cpp:211-215`）：
  `if (txn->isolation() == IsolationLevel::kSerializable && ssi_check_conflict(*txn)) { rollback(txn); return false; }`
- 历史裁剪：`prune_committed_history()`（`.cpp:690-713`）：没有活跃事务时**整个清空**（`:702-705`），否则保留 `commit_id >= oldest_snapshot` 的项（`:707-712`）。
- 语义边界（代码注释自述，`:206-210`）：比严格 SSI 更保守——只读集与并发写集有任何重叠就 abort，注释认为「可证明可串行化」，而非只裁 dangerous structure。

### 4.4 CLOG（`txn_status_log`）枚举与持久化格式

- 状态枚举：`enum class TxnFinalState : u8 { kCommitted = 1, kAborted = 2 };`（`src/transaction/txn_status_log.h:30-33`）。与内存态 `TxnState`（`transaction.h:83`，含 `kActive = 0`）是**两套枚举**。
- 持久化格式（`src/transaction/txn_status_log.cpp:14-19`）：
  - `static constexpr size_t kRecordSize = sizeof(u64) + sizeof(u8);` → **每条 9 字节**：8 字节小端 xid + 1 字节状态；
  - 文件 `<db_dir>/wal/txn_status.log`（`:22`；Database 传入 `db_dir + "/wal"`，`src/database/database.cpp:93-94`）；
  - 打开方式 `O_RDWR | O_CREAT | O_APPEND`（`:24`）；
  - 构造时整文件重放进 `HashMap<u64,u8> states_`，**读不满 9 字节的撕裂尾被静默丢弃**（`:26-39`）；
  - `record(xid, state)`（`:49-65`）：**先写内存 map 再 write + fsync**（注释 `:52-55`：并发 `is_visible()` 必须立刻看到 abort/commit，不能等 fsync）；
  - `status(xid, out)`（`:67-73`）；`size()`（`:75-78`）；
  - 无截断/分段（头注释 `txn_status_log.h:17-19` 明说 truncation 是 future work）。
- 用途：`is_visible` 的 `is_uncommitted`（`transaction.cpp:534-539`）、`is_txn_committed` 的槽位复用兜底（`.cpp:586-591`，注释 `:579-585` 说明若没有它，GC 的 `is_garbage` 会把已提交的删除者看成未提交，死版本永不回收——无界空间泄漏）。`commit/rollback` 各写一次（`.cpp:287`、`:425`）。

### 4.5 `LockManager`：锁模式、加锁 API、死锁检测

- 锁模式（`src/concurrency/lock_manager.h:27-32`）：
```cpp
enum class LockMode : u8 {
    kAccessShare     = 0,   // SELECT
    kRowExclusive    = 1,   // INSERT/UPDATE/DELETE
    kExclusive       = 2,   // CREATE INDEX
    kAccessExclusive = 3,   // DROP TABLE / ALTER TABLE
};
static constexpr u32 kNumLockModes = 4;   // :34
```
- 加锁 API（`.h:67-69`）：`Status lock_table(u64 txn_id, u32 table_id, LockMode mode);`、`Status lock_record(u64 txn_id, u32 table_id, const RecordId& rid, LockMode mode);`、`Status lock_key(u64 txn_id, u32 table_id, const String& key, LockMode mode);`；释放 `.h:72-77`（`unlock_table/unlock_record/unlock_key/unlock_all`）；检测 `.h:80` `bool detect_deadlock(u64 txn_id);`。
- 兼容矩阵 `kLockCompatibility[4][4]`（`.cpp:22-28`）：
```
held\req   AS     RX     X      AX
AS        true   true   true   false
RX        true   true   false  false
X         true   false  false  false
AX        false  false  false  false
```
`is_compatible(held, requested)` 查表（`.cpp:46-48`）；`can_grant` 要求**双向**兼容（`.cpp:59`）。
- 等待超时：`static constexpr u32 kLockWaitTimeoutMs = 2000;`（`.cpp:155` 表锁、`:252` 记录锁，各自函数内定义）。
- 锁升级：只在 `lock_table` 内原地尝试（`.cpp:109-137`），前提是新模式与其他已授予者兼容；不满足则**立即返回** `kLockConflict, "lock upgrade blocked by other holders"`（`.cpp:139-143`，不等待）。`lock_record` 遇到已持有的弱锁升级请求直接返回 `"record lock upgrade blocked"`（`.cpp:237-242`）。
- 键锁实现：`lock_key` 把逻辑键折叠成伪 RID：`RecordId pseudo(logical_lock_key(table_id, key), 0); return lock_record(...)`（`.cpp:313-315`）；`logical_lock_key`（`.cpp:39-44`，`table_id * 1099511628211ull ^ Hash<String>()(key) ^ 0xD15EA5E5D15EA5E5ull`）；`record_lock_key`（`.cpp:32-37`，`table_id * 11400714819323198485ull` 再与 page/slot 混合）。
- **等待图死锁检测函数名与实现**：`LockManager::detect_deadlock(u64 txn_id)`（`.cpp:405-457`）：
  - 不建显式图，而是现场求 holder：lambda `find_holder_for(u64 waiter)` 遍历 `lock_table_`，对「waiter 未获授予的请求」收集所有已授予且不兼容的 holder（`.cpp:410-428`）；
  - `HashMap<u64,bool> seen` + 显式栈做 DFS（`.cpp:431-454`）；
  - **判环条件**：`if (holders[i] == txn_id) return true;`（`.cpp:449`）——只有回到起点才算环，注释 `:439-443` 解释「汇聚 DAG 上访问已见节点不是环」；
  - **调用点只有一处**：`lock_table` 的等待循环 `if (detect_deadlock(txn_id)) break;`（`.cpp:182-184`）；`lock_record` 的等待循环（`.cpp:253-290`）不调用它。
- **「youngest aborts」的实现位置：不存在**（见 §0 第 3 条）。真实受害者选择 = 请求者自己：`detect_deadlock` 返回 true 或超时 → 从等待队列移除自己的请求（`.cpp:194-203` / `:292-301`）→ 返回 `Status(ErrorCode::kLockConflict, "lock wait timeout or deadlock")`（`.cpp:211`）/ `"record lock wait timeout"`（`.cpp:310`）。`lock_record` 超时处注释明确否决了撤销他人锁的做法（`.cpp:281-284`）。
- 记账：`txn_locks_`（txn → 表 id 列表，`.h:91`）、`txn_record_locks_`（txn → 记录锁哈希键列表，`.h:93`）、`record_locks_`（`.h:92`）；`unlock_all` 先表后记录并 `erase` 两个索引（`.cpp:371-403`）。
- 锁释放时机（事务层）：`commit` → `.cpp:300`；`rollback` → `.cpp:430`。

---

## 5. WAL 与恢复

### 5.1 记录头字段与字节布局

`src/recovery/wal.h:59-67`：
```cpp
struct WalRecord {          // 注意：没有 #pragma pack
    u32     magic;          // == kWalRecordMagic
    u32     crc;            // CRC32 over header(crc zeroed) + payload
    u64     lsn;
    u64     txn_id;
    WalType type;           // u16 底层
    u32     data_len;
    // 后随 data_len 字节 payload
};
static constexpr u32 kWalRecordMagic = 0xD8BA110Cu;   // wal.h:57
```
- **布局的实际证据**：`write_record` 在填头之前 `std::memset(&hdr, 0, sizeof(hdr));`（`wal.cpp:168-169`），注释 `wal.cpp:162-167` 写着："WalRecord is not packed, so there are padding bytes between `type` (u16) and `data_len` (u32). The CRC is computed over sizeof(hdr)"。
- ⇒ 声明字段偏移 0/4/8/16/24，而 `data_len` 因 4 字节对齐落在 28，`u64` 使结构对齐为 8 ⇒ **`sizeof(WalRecord) == 32`（按 ABI 推算）**。代码中**没有 `static_assert`**，严格讲这是推算而非编译验证；`docs/WAL_RECOVERY_PROTOCOL.md:10-23` 的 30 字节/偏移 26 与代码不符。
- CRC32：标准反射实现，多项式 `0xEDB88320`（`wal.cpp:43-60`），查表懒初始化；写时 `hdr.crc = 0` 占位后计算覆盖整头 + payload（`:171-177`）；恢复时同样把头 crc 清零后复算比对（`:611-618`）。
- LSN：`next_lsn_` 初值 1（`wal.cpp:70`），每写一条记录 `next_lsn_.fetch_add(1)`（`:185`）；写记录时 `hdr.lsn = next_lsn_.load()`（`:172`）。
- 段大小（默认 64MB，`wal.cpp:63` / `db_config.h:36`）：写记录后 `fstat` 检查 `st.st_size + write_buf_pos_ > segment_size_bytes_` → `flush_buffer()` + `fsync` 并把 `durable_lsn_` 推到 `last_written_lsn_`（`:192-198`）。**这只触发刷盘，不做文件轮转**（文件名恒为 `wal.log`，`:78`）。
- 缓冲区：`static constexpr u32 kWalBufferSize = 8192; byte write_buf_[kWalBufferSize];`（`wal.h:171-172`）；`append_to_buffer` 在 `len > kWalBufferSize` 时先 flush 再直写（`.cpp:106-109`）。

### 5.2 记录类型枚举全部取值与编号

`src/recovery/wal.h:18-37`：

| WalType | 值 |
|---|---|
| `kTxnBegin` | 1 |
| `kTxnCommit` | 2 |
| `kTxnAbort` | 3 |
| `kInsert` | 10 |
| `kDelete` | 11 |
| `kUpdate` | 12 |
| `kIndexInsert` | 13 |
| `kIndexDelete` | 14 |
| `kPageAlloc` | 20 |
| `kCheckpoint` | 30 |
| `kDdl` | 40 |
| `kSavepointUndoInsert` | 50 |
| `kSavepointUndoDelete` | 51 |

`enum class DdlOp : u8 { kCreateTable=1, kDropTable=2, kCreateIndex=3, kDropIndex=4, kAlterAddColumn=5, kAlterDropColumn=6, kAlterRenameColumn=7 };`（`wal.h:43-51`）。类型名映射 `wal_type_name`（`wal.cpp:20-37`）。

载荷布局（全部 `__attribute__((packed))`，逐条来自代码）：
- `kInsert`：`{u32 table_id; u64 page_id; u16 slot_idx; u16 data_size;}` + tuple 字节（`wal.cpp:227-245`，`kStackBufSize = 1024`，超过则 malloc）。
- `kDelete`：`{u32 table_id; u64 page_id; u16 slot_idx;}`（`:253-260`）。
- `kUpdate`：`{u32 table_id; u64 old_page_id; u16 old_slot_idx; u64 new_page_id; u16 new_slot_idx; u16 data_size;}` + 新元组字节（`:269-290`）。
- `kIndexInsert` / `kIndexDelete`：`{u32 index_id; u64 page_id; u16 slot_idx; u16 key_size;}` + `key.encode(...)`（`:300-320` / `:385-405`，`kStackBufSize = 512`）。
- `kSavepointUndoInsert` / `kSavepointUndoDelete`：`{u32 table_id; u64 page_id; u16 slot_idx;}`（`:327-336` / `:341-350`）。
- `kDdl`：`{u8 op; u32 table_id; u32 aux; u16 name_len;}` + name（`:354-372`），**写完立即 `flush()` 保证 DDL 审计记录已落盘**（`:374-379`）。
- `kPageAlloc`：枚举存在（`wal.h:27`），但本次核对未发现任何写入点（**未确认**，可能是历史遗留取值）。

### 5.3 组提交实现

- 入口：`u64 WalManager::flush_commit(u64 lsn)`（`src/recovery/wal.cpp:474-519`），由 `log_commit` 调用（`wal.cpp:206-214`，`flush_commit` 返回 false 则 `log_commit` 返回 0 → 事务层 rollback）。
- 延迟参数名：`group_commit_delay_ms_`（成员，`wal.h:158`；构造参数 `group_commit_delay_ms`，`wal.h:74`；开关 `group_commit_enabled_`，`wal.h:157`）。默认来自 `DbConfig::wal_group_commit = true`、`wal_group_commit_delay_ms = 2`（`src/common/db_config.h:39-40`），在 `database.cpp:83-87` 装配。
- 三条路径：
  1. `!fsync_enabled_`：flush buffer 后直接把 `durable_lsn_` 推到 `lsn`（不等磁盘）（`:478-483`）。
  2. `!group_commit_enabled_ || group_commit_delay_ms_ == 0`：flush + `fsync`，`durable_lsn_ = last_written_lsn_`，broadcast（`:485-491`）。
  3. 组提交：`pending_commit_waiters_++`（`:493`），`leader = (pending_commit_waiters_ == 1)`（`:494`）。**leader 只有在 `pending_commit_waiters_ > 1` 时才 `commit_cond_.timed_wait(latch_, group_commit_delay_ms_)`**（`:495-501`，注释 `:496-498`：独自提交时等待纯属浪费）；随后 `flush_buffer() && fsync() == 0`，成功则 `durable_lsn_ = last_written_lsn_`，`group_commit_batches_++`、`commit_batch_id_++`、`pending_commit_waiters_ = 0`、broadcast（`:502-508`）。follower 记录进入时的 `commit_batch_id_`，以 `durable_lsn_ < lsn && commit_batch_id_ == entry_batch` 为等待谓词（`:514-517`）——即使自己的 LSN 没落盘，也能在批关闭时醒来看失败。
- 统计：`group_commit_batches()`、`buffer_flushes()`、`buffered_bytes()`（`wal.h:132-134`）。

### 5.4 checkpoint 流程与被截断的内容

- 函数名：`u64 WalManager::checkpoint(CheckpointPageFlush flush_pages_cb, void* ctx, bool allow_truncate = true)`（声明 `wal.h:113-114`，重载 `checkpoint()` `wal.h:115` 转调 `checkpoint(nullptr, nullptr, true)`；实现 `wal.cpp:410-472`）。
- 回调类型：`using CheckpointPageFlush = void (*)(void* ctx);`（`wal.h:109`）。
- 四阶段（全程持 `latch_`，注释 `:415-418` 说明为何不复用 `write_record`）：
  1. **写 kCheckpoint 标记**（内联构造 header，`txn_id = 0`、`data_len = 0`）（`wal.cpp:419-436`）；
  2. `flush_buffer()` + `fsync` + `durable_lsn_.store(last_written_lsn_)` + `commit_cond_.broadcast()`（`:438-442`）；
  3. **持 WAL 锁调用 `flush_pages_cb(ctx)`**（`:444-451`，注释解释此时所有脏页满足 `page_lsn <= durable_lsn_`，`flush_frame_wal_first` 走快路径不会重入 WAL 锁 → 不会死锁）；
  4. **截断**：`allow_truncate && fd_ >= 0` 时 `close(fd_)` → `open(path, O_WRONLY|O_CREAT|O_TRUNC)` 重开 `wal.log`（`:457-467`），保留 `next_lsn_`/`durable_lsn_`，`bytes_since_checkpoint_` 归零（`:466`）。**被截断的是整个 `wal.log`**，不是按 LSN 段裁剪；若 `allow_truncate == false`，`bytes_since_checkpoint_` 不重置，后台循环会持续重试（注释 `:468-469`）。
- Database 侧调用：`Database::checkpoint()`（`src/database/database.cpp:1355-1366`）—— 先对所有 heap `flush_meta()`（`:1356-1358`）；`const bool allow_truncate = !txn_manager_.has_active_transactions();`（`:1362`）；再 `wal_->checkpoint(&Database::flush_pages_for_checkpoint_trampoline, this, allow_truncate)`（`:1363-1364`）；最后 `save_control_file(false)`（`:1365`）。
- 回调实现：`Database::flush_pages_for_checkpoint()`（`:1338-1353`）→ `page_store_->set_durable_lsn(wal_->durable_lsn())`（`:1343`）→ `pool_->flush_all()`（`:1344`）→ `page_store_->flush()`（`:1345`）→ 同步 catalog 统计（`:1346-1352`）。
- 触发点：析构（`:151`）、后台线程按时间/字节阈值（`:189-195`，`checkpoint_timeout_ms` 默认 60s、`checkpoint_wal_size_bytes` 默认 256MB，`db_config.h:33-34`）、`Database::vacuum()`（`:1396`）、以及多处 DDL 路径（grep 命中 `database.cpp:668,678,753,763,820,830`，未逐行核对语义）。

### 5.5 崩溃恢复入口与重放步骤

- 入口函数：`bool WalManager::recover(Database* db)`（`src/recovery/wal.cpp:564-1012`）；唯一调用点 `src/database/database.cpp:97`：
```cpp
if (wal_->recover(this)) {
    catalog_.for_each_index([](IndexEntry& e, void*) { e.state = IndexState::kInvalid; }, &_ctx);  // :104-106
    if (!fault_active("skip_index_rebuild")) rebuild_all_indexes();                                 // :110-112
    flush();                                                                                        // :114
}
```
- **注意：不是「单遍前向重放」，而是「两遍扫描同一文件 + 逆序 undo」**：
  - **第一遍（分类）**：`while (read_record(fd, &hdr, &scratch_payload))`（`:644`）——`read_record` 逐条校验 magic / `data_len <= kMaxReplayDataLen(= kPageSize + 256, :595)` / CRC（`:601-619`），任一不满足即**当作日志结束停止**（`:604-618`）；记录 `max_lsn`、`max_txn_id`（`:645-646`）；`kTxnCommit → committed[txn]`、`kTxnAbort → aborted[txn]`（`:647-650`）；解析并收集 `kDdl` 记录到 `ddl_records`（`:651-674`）。
  - **第二遍（redo）**：`lseek(fd, 0, SEEK_SET)`（`:685`）后重扫，只处理 `kInsert/kDelete/kUpdate` 与两条 savepoint 补偿记录（`:694-702`）。
    - 已提交 → 就地 redo；未提交 → 收集 `ReplayRef{hdr, data_offset}` 进 `undo_refs`（`:744-751`）。
    - `kInsert` → `heap->recover_insert_at(page, slot, data, size, lsn, &new_tuple)`（`:767-769`，`new_tuple` 置 `needs_index_rebuild`）；`recover_insert_at` 用「页前 64 字节是否全零」判断未初始化并 `init`（`heap_file.cpp:760-767`），并用 `page->header()->lsn >= lsn` 幂等跳过（`:768`）。
    - `kDelete` → 先 `page_lsn_at(page) >= hdr.lsn` 幂等跳过（`:783-786`），否则 `mark_deleted(page, slot, txn_id, lsn)`（`:787`）。
    - `kUpdate` → 新/旧页各自 `page_lsn_at` 判断（`:806-809`），两页都完成则 `continue`（`:810-812`）；否则新页 `recover_insert_at`（`:816-821`）+ 旧页 `mark_deleted` 与 `set_next_version`（`:825-829`），旧页失败时回滚新页插入（`:830-835`）。
    - savepoint 补偿 → `heap->rollback_insert/rollback_delete`（`:735-739`，同样先做 `page_lsn_at` 幂等判断 `:731-734`）。
  - **undo 阶段（逆序）**：`:863-926`。`kInsert` → `rollback_insert_if_xmin`（只有该槽当前 xmin 仍等于记录里的 txn_id 才回滚，防止槽位已被后来的已提交插入复用；lambda `:845-861`）；`kDelete` → `rollback_delete`（`:900`）；`kUpdate` → `rollback_delete(old)` + `rollback_insert_if_xmin(new)`（`:917-923`）。
  - **DDL 逆序撤销**：`:933-993`（kCreateTable / kCreateIndex / kAlterAddColumn / kAlterDropColumn 可逆；`kDropTable`、`kDropIndex` 只置 `needs_index_rebuild`，注释解释 WAL 里没有完整 schema 无法逆（`:947-957`、`:968-971`）；`kAlterRenameColumn` 直接跳过（`:986-991`））。
  - **收尾**：`next_lsn_`/`durable_lsn_`/`last_written_lsn_` 只增不减（`:1004-1006`，注释 `:997-1003` 说明重置会重新引入 checkpoint 死锁）；`db->txn_manager().ensure_next_txn_id_at_least(max_txn_id + 1)`（`:1007-1009`）。
- 其它细节：`page_lsn_at` lambda（`:622-630`）；`skip_bytes` lambda 定义后标注为「保留可读性、所有路径都改读 payload」（`:591-593`、`:620`）。

### 5.6 索引为什么是 lazy rebuild（代码依据）

1. `recover()` 的返回值**不是「成功/失败」**，而是 **「是否需要重建索引」**：函数内维护 `bool needs_index_rebuild = false;`（`wal.cpp:676`），在多种情况下置 true（savepoint 补偿 `:740`、新物理元组 `:770`、delete `:788`、update `:839`、undo 各分支 `:886/:901/:918/:922`、DDL `:944/:956/:965/:970`）。
2. 最关键的一条：`bool saw_committed_heap_dml = false;`（`:682`），任何**已提交**的 kInsert/kDelete/kUpdate 记录都会把它置 true（`:703-708`），收尾时 `if (saw_committed_heap_dml) needs_index_rebuild = true;`（`:1010`）。原因写在注释里（`:677-682`）：**索引页没有像堆页那样的 page-LSN 保护**——已提交的堆插入可能已刷盘，而对应的 B 树叶子还是脏页；崩溃后堆 redo 因 `page_lsn >= lsn` 被跳过，如果只靠「是否真的重放了堆记录」来决定重建，就会漏掉索引不一致。
3. 调用方 `Database` 构造：`if (wal_->recover(this))` → 先把所有 `IndexEntry::state` 置 `kInvalid`（`database.cpp:99-106`，注释解释这是为了让 recovery 与 rebuild 之间进来的 SQL 看到受保护状态而不是半成品树）→ `rebuild_all_indexes()`（`:111`，实现 `:1127`）→ `flush()`（`:114`）。因此**「重建整个索引」是当前唯一路径**；`DbConfig::recover_indexes_lazy`（`db_config.h:25`，解析 `db_config.cpp:145`）无读取点（§0 第 8 条）。
4. `WalType::kIndexInsert/kIndexDelete` 仍会被写入（`log_index_insert` `wal.cpp:297-323`；调用点 `database.cpp:1026` / `:1055`），但 `recover()` 第二遍**只认 kInsert/kDelete/kUpdate + savepoint**（`:694-702`），索引记录在重放中被忽略——即索引日志目前是「写了不用」。
5. `src/recovery/README.md:26-28` 与代码一致：「Index replay is currently a lazy rebuild via `Database::rebuild_all_indexes`; full physical index redo is tracked in `docs/ACID_TODO.md` A2」。

### 5.7 GC 判定垃圾的真实条件表达式

`bool GarbageCollector::is_garbage(const Tuple& t, u64 oldest_active)`（`src/recovery/gc.cpp:17-32`）：
```cpp
if (t.xmin() == 0) return false;                                  // :18
if (!txn_mgr_->is_txn_committed(t.xmin())) return false;           // :21
if (t.xmin() >= oldest_active) return false;                       // :22
if (t.xmax() == 0) return false;                                   // :25
if (!txn_mgr_->is_txn_committed(t.xmax())) return false;           // :28
if (t.xmax() >= oldest_active) return false;                       // :29
return true;                                                      // :31
```
即：**xmin 已提交且早于最老活跃事务，xmax 非 0、已提交且早于最老活跃事务**（该版本对所有现存与将来的快照都不可见）。

配套：`is_all_visible_tuple`（`:34-43`）——`xmin != 0 && xmax == 0 && (xmin == kFrozenTxnId || (is_txn_committed(xmin) && xmin < oldest_active))`；注释 `:35-36` 强调「没有垃圾 ≠ all-visible」（未提交的插入没有垃圾，但 IndexOnlyScan 不能跳过堆 MVCC）。

GC 执行流程（`run_gc`，`:45-161`）：
- `oldest_active = txn_mgr_->get_oldest_active_txn_id()`（`:46`）；
- 通过 `catalog_->for_each_table(scan_callback, &ctx)`（`:159`）逐表处理；每表取**活 HeapFile** 并持 `HeapFile::LatchGuard`（`:64-67`，注释 `:62-63` 说明栈上临时 HeapFile 的 latch 不互斥并发 DML）；
- 增量游标 `HashMap<u32,u32> last_gc_page_`（`gc.h:50`）循环推进（`gc.cpp:76-78`、`:156`）；
- 命中垃圾：**先删索引项** `gc->db_->delete_index_entries(te.table_id, tuple, RecordId(page_id, slot))`（`:106-107`，注释 `:102-105`：DELETE 故意不删，GC 是唯一清理点），再按版本链目标分派——同页 → `page->redirect_slot(slot, next_slot)`（`:111-115`），跨页/链尾 → `page->mark_dead(slot)`（`:116-125`）；
- 有垃圾 → `page->prune()` + `mark_dirty` + `fsm().update` + `vm().clear_page()`（`:131-139`）；无垃圾且全可见 → `vm().set_visible(page_id)`（`:140-144`）；否则 `vm().clear_page()`（`:145-148`）。
- `run_vacuum`（`:163-277`）：`freeze_horizon = oldest_active`（`:168`）；跳过已 frozen 页（`:195`）；冻结就是把 `kFrozenTxnId` 直接 memcpy 到元组头的 xmin 位置（`:241-244`，条件 `:234-238`）；页级结论 set_frozen / set_visible / clear（`:261-267`）；处理完把增量游标归零（`:273`）。
- 触发：`Database::maybe_gc()`（`src/database/database.cpp:1368-1391`）——**显式事务中直接返回**（`:1371-1373`）、`gc_enabled` 关闭返回（`:1375-1377`）、`ops_since_gc_ >= gc_ops_threshold`（默认 10000，`db_config.h:51`）才 `gc_->run_gc(gc_max_pages_per_cycle)`（默认 128，`:52`）；调用点 `server.cpp:408`、`server.cpp:1400`、`repl.cpp:911`；后台线程见 `database.cpp:174-195`（100ms tick，GC 间隔默认 5000ms）。

---

## 6. 一条 DML 的端到端追踪：`UPDATE users SET score=100 WHERE id=2`

前提（**未确认**：具体表/索引定义不可知；假定 `users(id PK, score)`，`id` 上有索引/主键 → 子计划为 IndexScan；若 `id` 无索引则第 4 步换成 `SeqScanExecutor::next`）。

| # | 调用 | 位置 |
|---|---|---|
| 1 | `REPL::execute_sql(sql)`（或 `Server::execute_plan_result`） | `src/repl/repl.cpp:343` / `src/network/server.cpp:298` |
| 2 | `db_.txn_manager().begin()`（SELECT/DML 都需要快照） | `repl.cpp:722-729` / `server.cpp:304-311` |
| 3 | → `TransactionManager::begin()` → 捕获 `active_snapshot`、`txn_id = next_txn_id_++`、`snapshot_id = next_txn_id_` | `src/transaction/transaction.cpp:152-195`（关键行 `:181-182`） |
| 4 | → `db_->wal().log_begin(txn_id)` → `write_record(kTxnBegin,...)` → `append_to_buffer` → `write_direct` | `transaction.cpp:192` → `src/recovery/wal.cpp:202-204` → `:157-200` → `:103-118` → `:90-101` |
| 5 | `Planner::plan(stmt)` 产出 `UpdatePlan`，child = 选中的扫描（可选再包一层 `FilterPlan`） | `src/sql/planner/planner.cpp:816`、child 赋值 `:849`、Filter 包装 `:844` |
| 6 | `ExecutorFactory::create(plan)`，`case PlanNodeType::kUpdate` | `src/sql/executor/executor_factory.cpp:116` → `:312-344` |
| 7 | → `db_.lock_manager().lock_table(txn_id, table_id, LockMode::kRowExclusive)`（失败 → `set_executor_error("could not acquire table lock")`） | `executor_factory.cpp:314-319` |
| 8 | → 扫描子执行器建树（`case kIndexScan`：取 `key_columns` 供重校验） | `executor_factory.cpp:164-189` |
| 9 | → `new UpdateExecutor(&db_.pool(), heap, schema, SET 子句, child, tm, table_id, &db_.wal(), &db_.catalog(), &db_)` | `executor_factory.cpp:339-343` |
| 10 | `exec->init()` → `UpdateExecutor::init()` → `child_->init()` | `src/sql/executor/update.cpp:150-153` |
| 11 | `exec->next()` → `UpdateExecutor::next()`；`hot_eligible = !catalog_->any_column_indexed(table_id_, {score})` | `update.cpp:252`、`:262-269` |
| 12 | **抽干 WHERE 侧到 RID 列表**（Halloween 规避）：`child_->next()` + `child_->last_record_id(&rid)` + `seen_targets` 去重 | `update.cpp:295-317`；`last_record_id` 实现 `src/sql/executor/index_scan_executor.cpp:414-418` |
| 13 | 子扫描内部：`IndexScanExecutor::next()` → `index_->range_scan_batch(...)`（32 条/批）→ 取堆页（可复用 `cached_heap_page_`）→ `follow_version_chain` → `TransactionManager::is_visible` → `record_read`（仅 Serializable） | `src/sql/executor/index_scan_executor.cpp:180-303`（批 `:194-196`、链 `:245-246`、可见性 `:73-78`、`record_read` `:294-296`） |
| 14 | 逐目标：`db_->read_tuple(table_id_, schema_, old_rid, &old_tuple)` → `pool_->fetch_page(rid.page_id, true)` → `PageStore::read_page` → `DiskManager::read_page`（校验和失败则整页清零） | `src/database/database.cpp:972-990` → `src/storage/buffer_pool.cpp:43` → `src/storage/page_store.h:59-63` → `src/storage/disk_manager.cpp:84-93` |
| 15 | 求值 SET：`ExpressionEvaluator::evaluate(*set_clauses_[i].second, old_tuple)` + `cast_value_for_column(new_val, target_type)` | `update.cpp:337-344`；`src/sql/executor/expression_evaluator.cpp:210` |
| 16 | `lock_record(txn_id, table_id_, old_rid, kRowExclusive)`（失败 → 序列化冲突错误） | `update.cpp:346-350` |
| 17 | **写写冲突检测**：重新 fetch 该页，`memcpy(&cur_xmax, data + lp->offset + 8, 8)`，`cur_xmax != kInvalidTxnId && cur_xmax != txn_id` → `set_executor_error("could not serialize access due to concurrent update")` | `update.cpp:356-376`（`memcpy` 在 `:370`） |
| 18 | 约束校验：`schema_.validate_row` + `check_constraint_violation` | `update.cpp:378-385` |
| 19 | 唯一键逻辑锁 `lock_key` + `violates_unique_constraints`（仅当 SET 触及唯一列） | `update.cpp:390-414` |
| 20 | 构造新元组：`set_xmin(txn_id)`、`set_xmax(0)`、`set_next_version(kNullPageId, 0)`、`serialize_to_page` | `update.cpp:425-441` |
| 21a | **HOT 分支**（score 无索引时）：`heap_->prepare_insert_in_page(old_page, size)` → `predict_slot` | `update.cpp:449`；`src/storage/heap_file.cpp:388-412` |
| 22a | `wal_->log_update(txn, table, old_page, old_slot, old_page, hot_slot, buf, size)` **（WAL 先写）** | `update.cpp:455-458` → `src/recovery/wal.cpp:265-295` → `write_record` `:157-200` |
| 23a | `InPageReservation::commit(...)` → `page->insert_tuple_at(...)` → `pool_->set_page_lsn` → `mark_dirty`（新元组落页） | `src/storage/heap_file.cpp:418-446` → `src/storage/page.cpp:105-134` → `src/storage/buffer_pool.cpp:333-353`、`:316-331` |
| 24a | `heap_->commit_old_tuple(old_page, old_slot, new_page, new_slot, txn_id, lsn)`：写 `next_page@+16`、`next_slot@+24`、`xmax@+8`，盖 page LSN 后 unpin | `src/storage/heap_file.cpp:340-372` |
| 25a | `txn_mgr_->record_hot_delete(old_rid)` + `record_hot_insert(new_rid)`（**不碰索引**） | `update.cpp:512-516` → `src/transaction/transaction.cpp:54-68` |
| 21b | **非 HOT 分支**（score 有索引时）：`heap_->prepare_insert(size)` → `InsertReservation`（可能落到 FSM 选中的页或新页） | `update.cpp:525-531`；`src/storage/heap_file.cpp:183-239`（FSM 查询 `:217`） |
| 22b | `wal_->log_update(txn, table, old_page, old_slot, new_page, new_slot, ...)` | `update.cpp:535-544` |
| 23b | `InsertReservation::commit(...)`（含 FSM 更新 `fsm_.update` 与 `vm_.clear_page`） | `src/storage/heap_file.cpp:245-333`（FSM `:303`/`:323`、VM `:307`/`:328`） |
| 24b | `heap_->commit_old_tuple(...)`（同 24a） | `update.cpp:566-567`；`heap_file.cpp:340-372` |
| 25b | `record_delete(old)` + `record_insert(new)` **先于**索引维护 | `update.cpp:588-591` |
| 26b | `db_->insert_index_entries(table_id_, new_tuple, new_record_id)`：逐索引 `wal_->log_index_insert(...)`（lsn==0 直接失败）→ `tree->insert(key, rid)`；**旧索引项不删**（留给 GC） | `update.cpp:592-603` → `src/database/database.cpp:1012-1043` → `src/recovery/wal.cpp:297-323` |
| 27 | 输出 `affected_rows` 单行结果 | `update.cpp:619-621` |
| 28 | `TransactionManager::commit(txn)`：SSI 检查（Serializable 才做）→ `wal().log_commit(txn)` → `flush_commit`（组提交/fsync）→ 翻 slot 为 kCommitted → 写 `committed_history_` → **`status_log_->record(kCommitted)`** → 对 kDelete/kHotDelete 做 `prune_obsolete_version` → `lock_manager().unlock_all(txn_id)` | `src/transaction/transaction.cpp:197-307`（SSI `:211-215`、`log_commit` `:222`、发布 `:235-254`、历史 `:261-274`、CLOG `:287`、剪枝 `:289-298`、解锁 `:300`）；`log_commit` `src/recovery/wal.cpp:206-214`；`flush_commit` `:474-519`；`prune_obsolete_version` `src/storage/heap_file.cpp:643-685` |
| 29 | 脏页最终落盘：淘汰或 `flush_page/flush_all` 时 **先** `flush_frame_wal_first(frame)`（`wal_->flush_until(page_lsn)`）**再** `page_store_->write_page`；`LocalPageStore` → `DiskManager::write_page`（doublewrite header + 页镜像 + fsync → 主文件 pwrite → 清 doublewrite） | `src/storage/buffer_pool.cpp:512-520`、`:139-152`、`:359-378`；`src/storage/page_store.h:64-69`；`src/storage/disk_manager.cpp:95-131` |
| 30 | 崩溃后的对应回放：`recover()` 第二遍按 `kTxnCommit` 判定该 txn 已提交 → 重放 kUpdate（新页 `recover_insert_at` + 旧页 `mark_deleted`/`set_next_version`，`page_lsn` 幂等）→ 因 `saw_committed_heap_dml` 返回 true → `rebuild_all_indexes()` | `src/recovery/wal.cpp:791-842`、`:1010-1011`；`src/database/database.cpp:97-114` |

（`INSERT` 的对应链路见 §2.7：`wal_->log_insert` 在 `heap_->prepare_insert` 之后、`InsertReservation::commit` 之前；`DELETE` 见 §2.9。）

---

## 7. 「未确认」清单（写笔记时不要写成确定结论）

1. `sizeof(WalRecord)` 的确切值：代码无 `static_assert`，**按 ABI 推算为 32 字节**；未编译验证。文档写 30 字节（`docs/WAL_RECOVERY_PROTOCOL.md:10-23`）。
2. `WalType::kPageAlloc = 20`：枚举存在，但本次未找到任何写入点（**未确认**是否死枚举）。
3. `HashMap::operator[]` 的插入/覆盖语义：`BufferPool::fetch_page` Phase 3 对同一 page_id 二次 `insert_page_mapping`（`buffer_pool.cpp:117` 与 `:174`），是否产生重复键取决于 `src/container/hash_map.h`（未在本次核对范围）。
4. `DiskManager` 的 `fd_cache_limit` 号称 LRU，实际是「超限清空整个缓存」（`disk_manager.cpp:264-269`）；是否刻意简化未在注释中说明。
5. `src/network/server.cpp` 中除 `:298-412` 之外的 `executor_error()` 检查点（`:272-288`、`:529-537`、`:847-862`、`:1182-1197`、`:1291-1374`）只由 grep 命中，未逐行确认其所在函数（应为 `Server::execute_sql` `:443` 与 `Server::execute_sql_streaming` `:895` 及其内部游标路径）。
6. 其它文档（`docs/ACID_TODO.md`、`docs/CONCURRENCY_CONTROL.md` 等）中大量「已实现 ✅」标记未逐一与代码对照；本文只核对了本任务指定的 4 份文档与 5 个目录 README。
7. `docs/QUERY_EXECUTION.md:173,262` 的「late materialisation」描述与代码一致（投影下推），但未提及「版本链仍按全 schema 反序列化」这一代价（`seq_scan.cpp:196-197` 等）。
8. `RemotePageStore` / `PageServer` / `RemotePageStoreClient` 仅做概览（`page_store.h:39-81`、`database.cpp:60-77`、`src/storage/README.md`），未逐行核对远端 WAL 镜像格式与 LogIndex 重建。
9. §0 第 1 条若需硬结论，建议在可编译环境加 `static_assert(sizeof(WalRecord) == 32)` 或 `printf("%zu")` 验证后再写入正式笔记。
