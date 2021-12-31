import os
import argparse

parser = argparse.ArgumentParser(description='manual to this script')
parser.add_argument('--milvus_release_version', type=str, default="2.0.0-rc6")
parser.add_argument('--commit_id', type=str, default="20210910-020f109")
parser.add_argument('--milvus_old_version', type=str, default="2.0.0-rc4")
parser.add_argument('--pymilvus_release_version', type=str, default="2.0.0rc6")
parser.add_argument('--release_date', type=str, default="2021-09-10")
parser.add_argument('--keep_milvus', action='store_true')
parser.add_argument('--clear_log', action='store_true')
args = parser.parse_args()

milvus_release_version = args.milvus_release_version
commit_id = args.commit_id
milvus_old_version = args.milvus_old_version
pymilvus_release_version = args.pymilvus_release_version
release_date = args.release_date
keep_milvus = args.keep_milvus
clear_log = args.clear_log
MILVUS_IMAGE_REPO = "milvusdb/milvus"
home_path = os.path.expanduser('~')
default_check_dir = home_path + "/Release_test"
default_log_path = "/tmp/ci_logs"
release_check_dir = os.getenv("RELEASE_CHECK_DIR", default_check_dir)
release_log_path = os.getenv("CI_LOG_PATH", default_log_path)

RELEASE_EMAIL_USERNAME = os.getenv("RELEASE_EMAIL_USERNAME")
RELEASE_EMAIL_PASSWORD = os.getenv("RELEASE_EMAIL_PASSWORD")
RELEASE_SMTP_SERVER = os.getenv("RELEASE_SMTP_SERVER")
# receive_email_address = {"milvus_tag_owner": "xiangyu.wang@zilliz.com",
#                          "pymilvus_tag_owner": "xiangyu.wang@zilliz.com",
#                          "docker_image_owner": "jie.zeng@zilliz.com",
#                          "release_notes_owner": "xiaofan.luan@zilliz.com",
#                          "milvus_doc_owner": "wuchuanzi.yu@zilliz.com",
#                          "test_owner": "yanliang.qiao@zilliz.com",
#                          "release_verifier": "binbin.lv@zilliz.com",
#                          "milvus_helm_owner": "jie.zeng@zilliz.com"}
# copy_email_address = ["libin.shen@zilliz.com", "xiaofan.luan@zilliz.com", "yanliang.qiao@zilliz.com"]
receive_email_address = {"milvus_tag_owner": "binbin.lv@zilliz.com",
                         "pymilvus_tag_owner": "binbin.lv@zilliz.com",
                         "docker_image_owner": "binbin.lv@zilliz.com",
                         "release_notes_owner": "binbin.lv@zilliz.com",
                         "milvus_doc_owner": "binbin.lv@zilliz.com",
                         "test_owner": "binbin.lv@zilliz.com",
                         "release_verifier": "binbin.lv@zilliz.com",
                         "milvus_helm_owner": "binbin.lv@zilliz.com"}
copy_email_address = ["yanliang.qiao@zilliz.com"]
results_mapping = {True: "PASS", False: "FAIL"}
release_check_item_status = {"milvus_tag": "FAIL",
                             "docker_image": "FAIL",
                             "pymilvus_tag": "FAIL",
                             "release_notes": "FAIL",
                             "milvus_doc": "FAIL",
                             "milvus_helm": "FAIL",
                             "milvus_docker_compose": "FAIL",
                             "milvus_test": "FAIL",
                             "release_verify": "FAIL"}

