import constants as cons
from prettytable import PrettyTable

check_item_status = cons.release_check_item_status

def create_report(check_item_status):

    table = PrettyTable(["Owner", "Check items", "Status"])
    table.add_row([cons.receive_email_address["test_owner"], "QA confirms milvus deliverable", "PASS"])
    table.add_row([cons.receive_email_address["milvus_tag_owner"], "checkout a branch point to specified commit",
                   check_item_status["milvus_tag"]])
    table.add_row([cons.receive_email_address["milvus_tag_owner"], "Release milvus source code",
                   check_item_status["milvus_tag"]])
    table.add_row([cons.receive_email_address["docker_image_owner"], "Upload docker images to Docker Hub",
                   check_item_status["docker_image"]])
    table.add_row([cons.receive_email_address["pymilvus_tag_owner"], "Release pymilvus source code",
                   check_item_status["pymilvus_tag"]])
    table.add_row([cons.receive_email_address["release_notes_owner"], "Write release notes",
                   check_item_status["release_notes"]])
    table.add_row([cons.receive_email_address["milvus_doc_owner"], "Publish milvus docs in milvus.io",
                   check_item_status["milvus_doc"]])
    table.add_row([cons.receive_email_address["milvus_helm_owner"], "Release milvus-helm source code",
                   check_item_status["milvus_helm"]])
    table.add_row([cons.receive_email_address["milvus_helm_owner"], "Update docker image in docker-compose.yaml",
                   check_item_status["milvus_docker_compose"]])
    table.add_row([cons.receive_email_address["test_owner"], "Release test: clone, install, test",
                   check_item_status["milvus_test"]])
    table.add_row([cons.receive_email_address["release_verifier"], "Verify release results: all steps done",
                   check_item_status["release_verify"]])

    table.align["Check items"] = "l"

    return table

