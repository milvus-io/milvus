////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include <gtest/gtest.h>
#include <thread>
#include <easylogging++.h>
#include <stdlib.h>
#include <time.h>

#include "utils.h"
#include "db/DBMetaImpl.h"
#include "db/Factories.h"
#include "db/Utils.h"
#include "db/MetaConsts.h"

using namespace zilliz::vecwise::engine;

TEST_F(MetaTest, GROUP_TEST) {
    auto group_id = "meta_test_group";

    meta::GroupSchema group;
    group.group_id = group_id;
    auto status = impl_->add_group(group);
    ASSERT_TRUE(status.ok());

    auto gid = group.id;
    group.id = -1;
    status = impl_->get_group(group);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(group.id, gid);
    ASSERT_EQ(group.group_id, group_id);

    group.group_id = "not_found";
    status = impl_->get_group(group);
    ASSERT_TRUE(!status.ok());

    group.group_id = group_id;
    status = impl_->add_group(group);
    ASSERT_TRUE(!status.ok());
}

TEST_F(MetaTest, GROUP_FILE_TEST) {
    auto group_id = "meta_test_group";

    meta::GroupSchema group;
    group.group_id = group_id;
    auto status = impl_->add_group(group);

    meta::GroupFileSchema group_file;
    group_file.group_id = group.group_id;
    status = impl_->add_group_file(group_file);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(group_file.file_type, meta::GroupFileSchema::NEW);

    auto file_id = group_file.file_id;

    auto new_file_type = meta::GroupFileSchema::INDEX;
    group_file.file_type = new_file_type;

    status = impl_->update_group_file(group_file);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(group_file.file_type, new_file_type);

    meta::DatesT dates;
    dates.push_back(meta::Meta::GetDate());
    status = impl_->delete_group_partitions(group_file.group_id, dates);
    ASSERT_FALSE(status.ok());

    dates.clear();
    for (auto i=2; i < 10; ++i) {
        dates.push_back(meta::Meta::GetDateWithDelta(-1*i));
    }
    status = impl_->delete_group_partitions(group_file.group_id, dates);
    ASSERT_TRUE(status.ok());

    group_file.date = meta::Meta::GetDateWithDelta(-2);
    status = impl_->update_group_file(group_file);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(group_file.date, meta::Meta::GetDateWithDelta(-2));
    ASSERT_FALSE(group_file.file_type == meta::GroupFileSchema::TO_DELETE);

    dates.clear();
    dates.push_back(group_file.date);
    status = impl_->delete_group_partitions(group_file.group_id, dates);
    ASSERT_TRUE(status.ok());
    status = impl_->get_group_file(group_file.group_id, group_file.file_id, group_file);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(group_file.file_type == meta::GroupFileSchema::TO_DELETE);
}

TEST_F(MetaTest, ARCHIVE_TEST_DAYS) {
    srand(time(0));
    DBMetaOptions options;
    options.path = "/tmp/vecwise_test";
    int days_num = rand() % 100;
    std::stringstream ss;
    ss << "days:" << days_num;
    options.archive_conf = ArchiveConf("delete", ss.str());

    auto impl = meta::DBMetaImpl(options);
    auto group_id = "meta_test_group";

    meta::GroupSchema group;
    group.group_id = group_id;
    auto status = impl.add_group(group);

    meta::GroupFilesSchema files;
    meta::GroupFileSchema group_file;
    group_file.group_id = group.group_id;

    auto cnt = 100;
    long ts = utils::GetMicroSecTimeStamp();
    std::vector<int> days;
    for (auto i=0; i<cnt; ++i) {
        status = impl.add_group_file(group_file);
        group_file.file_type = meta::GroupFileSchema::NEW;
        int day = rand() % (days_num*2);
        group_file.created_on = ts - day*meta::D_SEC*meta::US_PS - 10000;
        status = impl.update_group_file(group_file);
        files.push_back(group_file);
        days.push_back(day);
    }

    impl.archive_files();
    int i = 0;

    for (auto file : files) {
        status = impl.get_group_file(file.group_id, file.file_id, file);
        ASSERT_TRUE(status.ok());
        if (days[i] < days_num) {
            ASSERT_EQ(file.file_type, meta::GroupFileSchema::NEW);
        } else {
            ASSERT_EQ(file.file_type, meta::GroupFileSchema::TO_DELETE);
        }
        i++;
    }

    impl.drop_all();
}

TEST_F(MetaTest, ARCHIVE_TEST_DISK) {
    DBMetaOptions options;
    options.path = "/tmp/vecwise_test";
    options.archive_conf = ArchiveConf("delete", "disk:41");

    auto impl = meta::DBMetaImpl(options);
    auto group_id = "meta_test_group";

    meta::GroupSchema group;
    group.group_id = group_id;
    auto status = impl.add_group(group);

    meta::GroupFilesSchema files;
    meta::GroupFileSchema group_file;
    group_file.group_id = group.group_id;

    auto cnt = 10;
    auto each_size = 2UL;
    for (auto i=0; i<cnt; ++i) {
        status = impl.add_group_file(group_file);
        group_file.file_type = meta::GroupFileSchema::NEW;
        group_file.rows = each_size * meta::G;
        status = impl.update_group_file(group_file);
        files.push_back(group_file);
    }

    impl.archive_files();
    int i = 0;

    for (auto file : files) {
        status = impl.get_group_file(file.group_id, file.file_id, file);
        ASSERT_TRUE(status.ok());
        if (i < 5) {
            ASSERT_TRUE(file.file_type == meta::GroupFileSchema::TO_DELETE);
        } else {
            ASSERT_EQ(file.file_type, meta::GroupFileSchema::NEW);
        }
        ++i;
    }

    impl.drop_all();
}

TEST_F(MetaTest, GROUP_FILES_TEST) {
    auto group_id = "meta_test_group";

    meta::GroupSchema group;
    group.group_id = group_id;
    auto status = impl_->add_group(group);

    int new_files_cnt = 4;
    int raw_files_cnt = 5;
    int to_index_files_cnt = 6;
    int index_files_cnt = 7;

    meta::GroupFileSchema group_file;
    group_file.group_id = group.group_id;

    for (auto i=0; i<new_files_cnt; ++i) {
        status = impl_->add_group_file(group_file);
        group_file.file_type = meta::GroupFileSchema::NEW;
        status = impl_->update_group_file(group_file);
    }

    for (auto i=0; i<raw_files_cnt; ++i) {
        status = impl_->add_group_file(group_file);
        group_file.file_type = meta::GroupFileSchema::RAW;
        status = impl_->update_group_file(group_file);
    }

    for (auto i=0; i<to_index_files_cnt; ++i) {
        status = impl_->add_group_file(group_file);
        group_file.file_type = meta::GroupFileSchema::TO_INDEX;
        status = impl_->update_group_file(group_file);
    }

    for (auto i=0; i<index_files_cnt; ++i) {
        status = impl_->add_group_file(group_file);
        group_file.file_type = meta::GroupFileSchema::INDEX;
        status = impl_->update_group_file(group_file);
    }

    meta::GroupFilesSchema files;

    status = impl_->files_to_index(files);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(files.size(), to_index_files_cnt);

    meta::DatePartionedGroupFilesSchema dated_files;
    status = impl_->files_to_merge(group.group_id, dated_files);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(dated_files[group_file.date].size(), raw_files_cnt);

    status = impl_->files_to_index(files);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(files.size(), to_index_files_cnt);

    meta::DatesT dates = {group_file.date};
    status = impl_->files_to_search(group_id, dates, dated_files);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(dated_files[group_file.date].size(),
            to_index_files_cnt+raw_files_cnt+index_files_cnt);
}
