---
title: Dynamic Level Size for Level-Based Compaction
layout: post
author: sdong
category: blog
redirect_from:
  - /blog/2207/dynamic-level/
---

In this article, we follow up on the first part of an answer to one of the questions in our [AMA](https://www.reddit.com/r/IAmA/comments/3de3cv/we_are_rocksdb_engineering_team_ask_us_anything/ct4a8tb), the dynamic level size in level-based compaction.

<!--truncate-->

Level-based compaction is the original LevelDB compaction style and one of the two major compaction styles in RocksDB (See [our wiki](https://github.com/facebook/rocksdb/wiki/RocksDB-Basics#multi-threaded-compactions)). In RocksDB we introduced parallelism and more configurable options to it but the main algorithm stayed the same, until we recently introduced the dynamic level size mode.


In level-based compaction, we organize data to different sorted runs, called levels. Each level has a target size.  Usually target size of levels increases by the same size multiplier. For example, you can set target size of level 1 to be 1GB, and size multiplier to be 10, and the target size of level 1, 2, 3, 4 will be 1GB, 10GB, 100GB and 1000GB. Before level 1, there will be some staging file flushed from mem tables, called Level 0 files, which will later be merged to level 1. Compactions will be triggered as soon as actual size of a level exceeds its target size. We will merge a subset of data of that level to next level, to reduce size of the level. More compactions will be triggered until sizes of all the levels are lower than their target sizes. In a steady state, the size of each level will be around the same size of the size of level targets.


Level-based compaction’s advantage is its good space efficiency. We usually use the metric space amplification to measure the space efficiency. In this article ignore the effects of data compression so space amplification= size_on_file_system / size_of_user_data.


How do we estimate space amplification of level-based compaction? We focus specifically on the databases in steady state, which means database size is stable or grows slowly over time. This means updates will add roughly the same or little more data than what is removed by deletes. Given that, if we compact all the data all to the last level, the size of level will be equal as the size of last level before the compaction. On the other hand, the size of user data will be approximately the size of DB if we compact all the levels down to the last level. So the size of the last level will be a good estimation of user data size. So total size of the DB divided by the size of the last level will be a good estimation of space amplification.


Applying the equation, if we have four non-zero levels, their sizes are 1GB, 10GB, 100GB, 1000GB, the size amplification will be approximately (1000GB + 100GB + 10GB + 1GB) / 1000GB = 1.111, which is a very good number. However, there is a catch here: how to make sure the last level’s size is 1000GB, the same as the level’s size target? A user has to fine tune level sizes to achieve this number and will need to re-tune if DB size changes. The theoretic number 1.11 is hard to achieve in practice. In a worse case, if you have the target size of last level to be 1000GB but the user data is only 200GB, then the actual space amplification will be (200GB + 100GB + 10GB + 1GB) / 200GB = 1.555, a much worse number.


To solve this problem, my colleague Igor Kabiljo came up with a solution of dynamic level size target mode. You can enable it by setting options.level_compaction_dynamic_level_bytes=true. In this mode, size target of levels are changed dynamically based on size of the last level. Suppose the level size multiplier to be 10, and the DB size is 200GB. The target size of the last level is automatically set to be the actual size of the level, which is 200GB, the second to last level’s size target will be automatically set to be size_last_level / 10 = 20GB, the third last level’s will be size_last_level/100 = 2GB, and next level to be size_last_level/1000 = 200MB. We stop here because 200MB is within the range of the first level. In this way, we can achieve the 1.111 space amplification, without fine tuning of the level size targets. More details can be found in [code comments of the option](https://github.com/facebook/rocksdb/blob/v3.11/include/rocksdb/options.h#L366-L423) in the header file.
