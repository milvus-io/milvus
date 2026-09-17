# JSON 契约独立静态审查

日期：2026-09-15

范围：`contracts/query/JsonIndexReaderTest.cpp`、
`test_utils/JsonDataSets.cpp`、冻结的专用清单和计划，以及固定 master
`a876f471053edb2f68a06a9894afee9810ea7906` 中适用的 JSON 读取器用例。审查遵循
`~/.claude/CODE_REVIEW_GUIDE.md`。未运行构建或测试。

初始审查发现四个可操作缺口，均已在审查的快照中修正：

- 固定 master 的 LIKE 行 `B_b` 和 `A%e` 现具有带手动 offset 的独立 JsonFlat Match 描述符；
- 投影外层 router 显式拒绝同级 predicate、pattern 和 Ngram 接口 cast，而其已解析子项暴露适当接口；
- 投影的错误 path 和有类型 `Exists` 调用现以当前精确 `UnexpectedError` 构造断言已记录的协议拒绝；
- 每个成功解析的子项现检查 Count、Row 域、逻辑 cast 的 ValueType 和 caps/接口一致性。数值 int64 查询正确保留已解析的 DOUBLE ValueType。

审查也修正了静态展开计数：清单有两个可空 `JsonProjectedString` Ngram profile，因此其两个描述符展开为四个参数。最终源码计数为 133 个声明式描述符 / 980 个后端展开参数，加一个本地 `JsonResolvedReader` 所有权测试，共 981 个 GTest。

已重新手动检查 BOOL 的六种比较、BOOL/VARCHAR/INT64/DOUBLE 的四种区间端点组合、混合数值精度、ARRAY 任一元素语义、field/comparable 空值掩码，以及投影后的缺失/JSON 空值/cast 失败/field 空值存在性。数据集元数据和逻辑输入形状与中央 selector 对齐。未发现剩余可操作规格或代码质量问题。

两个 cast 词汇描述符有意保留源码确认的 JsonFlat `CastTypesOf`/`Resolve` 生产不一致。运行时行为和任何额外生产问题在集中式门禁开启前仍未验证。
