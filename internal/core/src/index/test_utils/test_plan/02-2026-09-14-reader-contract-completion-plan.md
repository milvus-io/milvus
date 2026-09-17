# 标量与模式读取器契约测试完成计划

> **执行授权更新：** 用户现已明确要求“编译运行”。已授权 configure、编译、测试列表和执行。下文此前的禁止构建/运行说明描述的是已完成的仅源码阶段，不再限制本轮。生产实现修复仍不在范围内；记录运行时缺陷，但不压制正确测试。

## 范围与基线

- 工作分支：`SegcoreRefactor/6-tests-index`；保留现有未提交 demo 工作。
- 为此任务获取的 Master 参考：`a876f471053edb2f68a06a9894afee9810ea7906`。
- 使用当前声明式框架完成普通 `ScalarPredicateReaderTest` 和 `PatternMatchReaderTest` 用例，包括 master 覆盖缺口。
- 必要时扩展普通数据集、operator、配置和 driver 支持。初始 demo 的限制不是遗漏用例的理由。
- 将每个后端配置 × 数据集 × operator 参数组合保留为独立 GTest 参数实例。注册保留 descriptors 和小参数；数据、读取器和大期望结果在执行时创建。
- 延续用户对 configure、编译、测试执行和测试二进制列表的禁止。本任务验收仅在源码层面进行。
- 不提交、推送、重写此前 stack 提交或修改生产实现。以带证据和正确期望行为的临时未提交问题说明记录实现缺陷。

## 归属

| 工作流 | 负责人 | 可编辑范围 |
|---|---|---|
| 标量契约审计与用例 | sol 5.6 xhigh / scalar_contract | ScalarPredicateReaderTest.cpp 及其临时覆盖/问题说明 |
| 模式契约审计与用例 | sol 5.6 xhigh / pattern_contract | PatternMatchReaderTest.cpp 及其临时覆盖/问题说明 |
| 共享支持与后端矩阵 | sol 5.6 xhigh / framework_support | index/test_utils、系列能力测试、index_tests CMake 源码接线、临时支持说明 |
| 需求、API 决策、集成验收 | Astra / root | 本计划、覆盖协调、目标静态检查、任务协调 |

契约代理在共享支持实现前提出其数据集和 helper 需求。文件归属避免并发编辑公共 catalog。代理保留独立编辑的文件，并向 root 报告范围变动。

## 覆盖要求

1. 清点全部相关 master 测试文件和共享 generators，而非仅匹配新契约名的文件。将证据固定到上述 master SHA。
2. 将每个相关 master 测试或不同参数组映射到新用例系列。将每项分类为已实现、已存在或带具体原因的明确暂缓。
3. 标量操作：In、NotIn、6 个单边比较 operator 和 4 个区间包容组合。覆盖空和重复 keys、重复值、misses/all hits、null 排除、边界/相等/反转 ranges 及适用 primitive types。
4. 模式操作：LIKE、字面量 prefix/suffix/substring 和 regex。保留 wildcard syntax 与字面字符的区别；覆盖适用的 null、空、Unicode、escaping、duplicate 和大输入用例。复杂 syntax 可使用手写期望 offsets。
5. 语义允许时跨契约复用数据；每个用例的数据集选择仍明确。共享 oracles 必须独立于生产 query 代码。
6. 后端选择复用生产 registries 和 capability metadata。添加 master query 覆盖需要的普通受支持 backend/type/load/build profiles。保留未筛选的系列能力承诺，以免缺失能力静默移除全部相关用例。
7. 将逐 query routing guards 与静态 capability matching 分开处理。一个 capability bit 不能证明每个 query argument 都被接受。
8. 仅明确暂缓需要不同 custom harness 的场景，如 input/reader 生命周期销毁、损坏 artifacts、取消、执行计划、远程服务和 candidate/nested-domain 验证。仅缺少 demo 便利性不能成为暂缓理由。

## 验收顺序

1. 批准代理的源码清单和提议的共享 API；协调数据集名、所有权、null 语义和注册语义的差异。
2. 对照每个覆盖矩阵协调实现，包括弥补 master 缺口的额外用例和每个声明的暂缓项。
3. 检查有类型 argument 所有权、bool/string input adapters、惰性生成、bitmap 大小、valid-row 语义、后端选择和独立参数命名。
4. 实现后执行独立静态集成审查，包括源码接线和能力承诺覆盖。修复测试/框架问题；记录生产问题，但不压制正确测试。
5. 检查 diffs 和未跟踪文件的空白/源码接线错误，并在有用时静态枚举 descriptors。不得将这些检查称作测试执行或运行时正确性证明。
6. 交付测试、覆盖映射和任何问题说明链接，并明确未解决的 custom 场景和不构建/不运行状态。

## 进展

- Master 已获取并固定；3 个独立 sol 5.6 xhigh 工作流已分派。
- 标量和模式源码清单已接受；详细 master-to-case 映射仍是交付内容。
- 已批准共享数据扩展：可选的 absent validity、带 packed validity subviews 的显式 batch sizes、连续 bool input 所有权和惰性拥有 string data。
- 已批准后端扩展：primitive types、普通 memory/mmap profiles，以及通过生产 artifact/loader metadata 解析的 hybrid builder output。Hybrid 使用实际数据集 cardinality 选择具体系列。
- 模式策略决定：静态 capability matching 保持自动；Caps 不足时，每个 backend 对预期逐操作 routing/support 只声明一次。优化拒绝的操作仍有直接结果断言。明确不支持的操作有异常断言。数据相关 routing 使用专用已知 fixture，而非生产 cost model 的副本。
- 空总输入在 Bitmap/Sorted 读取器存在前被拒绝；将该场景映射到 builder-contract 负测试。all-null 非空数据和空 query arguments 仍在范围内。
- 源码检查发现 Marisa embedded-NUL truncation；保留长度感知期望结果，并记录生产问题，不修复或跳过。
- Inverted scalar string query arguments 也在 keyword-query FFI 丢失 embedded-NUL 长度；保留正确测试，并与 Marisa 的 build/query truncation 分开记录。
- Backend matrix frozen at 136 configurations: bitmap/sort/inverted/hybrid each cover eight types, two nullability settings, and heap/mmap requests; Marisa/FM cover VARCHAR with the same settings. Dataset `requires_nullable` selects compatible configurations before data generation and is checked against generated data at execution.
- Requested mmap does not always imply physical mapping: bitmap bool/int8 and low-cardinality bitmap delegates use the production heap fallback. High-cardinality fixtures exercise actual bitmap mapping where reachable; the coverage report must distinguish both paths.
- Independent static reviews are closed: scalar author reviewed shared support/pattern cases, pattern author reviewed scalar cases, and root reconciled coverage and design decisions. Test-helper publication, fixture-index, missing bool/LIKE case, and overly broad exception-catch findings were fixed and rechecked. No current actionable test/framework finding remains in these reviews. The later authorized runtime results are recorded below.
- Invalid regex currently throws UnexpectedError through the production assertion path. Tests explicitly check this current query error, while the production ledger records the classification concern separately. Unsupported operations check their exact Unsupported code. Neither error path fabricates a result bitmap or catches setup failures.

## 已交付覆盖与验收证据

下述 descriptor 数量来自源码声明和资格规则。展开后的参数数量随后由下文记录的成功 GTest 列表和完整执行确认。

| 契约 | 用例描述符 | 后端展开的 GTest 参数 |
|---|---:|---:|
| ScalarPredicateReader | 506 | 5,570 |
| PatternMatchReader | 207 | 2,372 |
| 两个请求契约合计 | 713 | 7,942 |

中心后端表包含 136 个配置。独立系列能力守卫和现有 Lookup demo 是上述计数之外的额外测试。

本地审查材料（临时、未提交）：

- [标量 master 到用例映射、类型/operator 数量和明确暂缓项](10-scalar-predicate-coverage.md)
- [模式 master 到用例映射、精确 syntax 组和明确暂缓项](11-pattern-match-coverage.md)
- [后端/input/IO 矩阵和物理 mmap 区别](09-reader-framework-coverage.md)
- [独立共享框架静态审查](26-reader-framework-static-review.md)
- [独立标量静态审查](27-scalar-predicate-static-review.md)
- [独立模式静态审查](28-pattern-match-static-review.md)
- [集中生产问题台账](19-reader-contract-issues.md)

Root source checks cover all 15 test/support source files: each C++ source is listed once in index_tests, each requested contract has one parameterized test body executing only its own callback, old aggregated unittest helper headers are absent, and newline/trailing-whitespace checks pass. The completed sources pass formatter dry-run and diff checks. No commits or pushes were performed.

Explicit exclusions remain zero-row builder rejection, NaN ordering without a declared cross-family contract, ingestion/missing-binlog synthesis, candidate/consumer rechecks, execution-plan behavior, artifact corruption/lifetime/resource accounting, and benchmark/unbounded fuzz machinery. Ordinary deterministic data generation, query errors, batches, nullability, and supported backend load profiles are included. The detailed mappings above distinguish covered query semantics from these deferred behaviors.

## 构建与运行时验收

- One Sol 5.6 xhigh agent owns configure/build/run operations to avoid concurrent build-tree mutations. Other agents diagnose bounded failures when assigned; Astra coordinates and verifies results.
- Build the independent index_tests target and its necessary production dependencies in the existing configured workspace. Preserve full commands, logs, and exit status.
- List actual GTest instances, reconcile the two contract suites against 5,570 + 2,372 expected instances, and count additional family/Lookup tests separately.
- Execute the full index_tests inventory with XML output. If a crash interrupts the run, isolate it and execute the remaining inventory separately so a single process crash does not conceal unexecuted cases.
- Correct test/framework/build-wiring defects, retaining the same contract expectations and coverage. Document production failures with runtime evidence; do not modify production behavior or disable cases to obtain a green result.
- Final acceptance reports actual totals, pass/fail/skip/crash outcomes, build/run exits, artifact paths, and any limitations. The earlier static counts are not substituted for execution results.

Authorized runtime evidence uses the existing Ninja Release build with ASan disabled. `BUILD_UNIT_TEST=ON` was configured successfully and the focused `index_tests` target built successfully. The target links the already self-contained `libmilvus_core.so` runtime image and its one directly used Folly shared library, avoiding duplicate static dependency objects from `milvus_core`'s legacy public link interface.

The final GTest listing exited 0 with 7,960 unique tests: 5,570 Scalar predicate parameters, 2,372 Pattern parameters, six Lookup parameters, and 12 family guards. The cleanup-measured final run executed all 7,960 tests without a process crash in 89.821 seconds: 7,816 passed, 144 failed, and zero were skipped or disabled. Pattern had 2,322 passes and 50 Marisa embedded-NUL failures. Scalar had 5,476 passes and 94 failures: 24 Marisa embedded-NUL failures, 18 Inverted embedded-NUL failures, and 52 Inverted signed-zero failures. All 12 family guards and all six Lookup cases passed. The remaining failures preserve the length-aware and numeric contract expectations and are recorded as production findings; no expected bitmap or backend eligibility was weakened.

The complete runtime artifacts are under `/tmp/segcore-index-test-run-20260914-230828`: `configure-final.log`, `build-final.log`, `list-final.log`, `full-run-cleanup.log`, and `index-tests-cleanup.xml`. The run used an isolated `TMPDIR`; it held an active Inverted directory during execution and contained zero child entries after normal process exit. No known backend temporary pattern, workspace core dump, or new apport entry remained. The empty scratch ancestor and evidence logs/XML are retained intentionally; `36-2026-09-14-reader-contract-cleanup-summary.md` records the side-effect inventory and snapshot limits. The first full run and XML are retained separately as `full-run.log` and `index-tests-first.xml`; its 2,046 missing-`field_id` setup failures led to the shared backend metadata correction. `full-run-2.log` and `index-tests-second.xml` retain the preceding corrected run with the same 144 failing-name set.
