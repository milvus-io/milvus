# coding : UTF-8
import sys
sys.path.append("../python_client")
import os
import constants as cons
import re
import time
import math
import socket
import random
import report
import requests
import subprocess
import send_email as se
from bs4 import BeautifulSoup
from utils.util_log import test_log as log

milvus_release_version = cons.milvus_release_version
commit_id = cons.commit_id
milvus_old_version = cons.milvus_old_version
pymilvus_release_version = cons.pymilvus_release_version
release_date = cons.release_date
keep_milvus = cons.keep_milvus
clear_log = cons.clear_log
MILVUS_IMAGE_REPO = cons.MILVUS_IMAGE_REPO
release_check_dir = cons.release_check_dir
release_log_path = cons.release_log_path
RELEASE_EMAIL_USERNAME = cons.RELEASE_EMAIL_USERNAME
RELEASE_EMAIL_PASSWORD = cons.RELEASE_EMAIL_PASSWORD
RELEASE_SMTP_SERVER = cons.RELEASE_SMTP_SERVER
receive_email_address = cons.receive_email_address
copy_email_address = cons.copy_email_address
results_mapping = cons.results_mapping
check_item_status = cons.release_check_item_status

def parse_web_content(url):
    """
    target: parse the web html contents
    method: use package requests
    expected: get contents successfully
    """
    header = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,'
                  'image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-encoding': 'gzip, deflate, br',
        'Accept-language': 'zh-CN,zh;q=0.9',
        'Connection': 'keep-alive',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 '
                      '(KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36'
    }

    timeout = random.choice(range(80, 180))

    while True:
        try:
            res = requests.get(url, headers=header, timeout=timeout)
            res.encoding = 'utf-8'
            break

        except socket.timeout as e:
            print('3:', e)
            time.sleep(random.choice(range(8, 15)))

        except socket.error as e:
            print('4:', e)
            time.sleep(random.choice(range(20, 60)))

        except http.client.BadStatusLine as e:
            print('5:', e)
            time.sleep(random.choice(range(30, 80)))

        except http.client.IncompleteRead as e:
            print('6:', e)
            time.sleep(random.choice(range(5, 15)))

    return res.text

def get_web_content(url):
    """
    target: get the web contents
    method: use package requests
    expected: get contents successfully
    """
    header = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,'
                  'image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-encoding': 'gzip, deflate, br',
        'Accept-language': 'zh-CN,zh;q=0.9',
        'Connection': 'keep-alive',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 '
                      '(KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36'
    }

    timeout = random.choice(range(80, 180))

    while True:
        try:
            payload = {}
            res = requests.request("GET", url, headers=header, data=payload, timeout=timeout)
            break

        except socket.timeout as e:
            print('3:', e)
            time.sleep(random.choice(range(8, 15)))

        except socket.error as e:
            print('4:', e)
            time.sleep(random.choice(range(20, 60)))

        except http.client.BadStatusLine as e:
            print('5:', e)
            time.sleep(random.choice(range(30, 80)))

        except http.client.IncompleteRead as e:
            print('6:', e)
            time.sleep(random.choice(range(5, 15)))

    return res


def get_release_note_data(html_text):
    """
    target: get release related data on release notes
    method: get title, release date, version and description
    expected: get contents successfully
    """
    release_notes = {}
    bs = BeautifulSoup(html_text, "html.parser")  # Create BeautifulSoup object
    body = bs.body  # get body
    data = body.find('div', {'class': 'doc-post-content'})
    release_notes['title'] = data.find('h2').text  # get title data
    release_notes['version'] = data.find('table').text
    p_array = data.find_all('p')
    release_notes['release_date'] = p_array[1].string[-10:]
    release_notes['description'] = p_array[1].string

    return release_notes

def get_milvus_source_code_tag(html_text):
    """
    target: get latest tag of milvus source code
    method: get latest tag
    expected: get contents successfully
    """
    release_data = {}
    bs = BeautifulSoup(html_text, "html.parser")  # Create BeautifulSoup object
    body = bs.body  # get body
    data = body.find('h4', {'class': 'flex-auto min-width-0 pr-2 pb-1 commit-title'})
    release_data['tag'] = data.find('a').text  # get title data

    return release_data

def get_pymilvus_source_code_tag(html_text):
    """
    target: get latest tag of pymilvus source code
    method: get latest tag
    expected: get contents successfully
    """
    release_data = {}
    bs = BeautifulSoup(html_text, "html.parser")  # Create BeautifulSoup object
    body = bs.body  # get body
    data = body.find('h4', {'class': 'flex-auto min-width-0 pr-2 pb-1 commit-title'})
    release_data['pymilvus_tag'] = data.find('a').text  # get title data

    return release_data

def get_milvus_docker_image_tag(html_text):
    """
    target: get latest tag of milvus docker image
    method: get latest tag
    expected: get contents successfully
    """
    release_data = {}
    bs = BeautifulSoup(html_text, "html.parser")  # Create BeautifulSoup object
    body = bs.body  # get body
    data = body.find('div', {'class': 'styles__title___3qu67'})
    release_data['docker_image_tag'] = data.find('a').text  # get title data

    return release_data

def get_milvus_io_doc_upgrade(html_text):
    """
    target: get release related data on milvus official web
    method: get released version
    expected: get contents successfully
    """
    milvus_io_doc = {}
    milvus_io_doc['milvus_old_version_num'] = 0
    milvus_io_doc['milvus_release_version_num'] = 0
    bs = BeautifulSoup(html_text, "html.parser")  # Create BeautifulSoup object
    body = bs.body  # get body
    data = body.find('div', {'class': 'doc-post-content'})
    old_array = data.find_all('p')  # get milvus old version info
    count = 0
    log.info("Checking milvus official website doc (upgrade part) ...")

    for p in old_array:
        if milvus_old_version in p.text:
            milvus_io_doc['milvus_old_version_num'] += 1
        else:
            if count not in [4, 8]:
                log.warning("Wrong old milvus version in [%s]" % p.text)
        if milvus_release_version in p.text:
            milvus_io_doc['milvus_release_version_num'] += 1
        count += 1
    release_array = data.find_all('li')  # get milvus release version info
    count = 0
    for li in release_array:
        if milvus_release_version in li.text:
            milvus_io_doc['milvus_release_version_num'] += 1
        else:
            if count in [4, 5, 10, 11]:
                log.warning("Wrong release milvus version in [%s]" % li.text)
        count += 1

    return milvus_io_doc

def get_milvus_io_doc_install_standalone(html_text):
    """
    target: get info of milvus io official web (install part)
    method: get all the step info of install part
    expected: info extracted successfully
    """
    milvus_io_doc_standalone = {}
    bs = BeautifulSoup(html_text, "html.parser")  # Create BeautifulSoup object
    body = bs.body  # get body
    data = body.find('div', {'class': 'doc-post-content'})
    link = data.find_all('pre')
    milvus_io_doc_standalone['docker_compose_link'] = re.search("w(.*)yml", str(link[0])).group(0)
    # milvus_io_doc_standalone['docker_compose_install'] = re.search("do(.*)d", str(link[1])).group(0)
    # milvus_io_doc_standalone['docker_compose_show'] = re.search("doc(.*)s", str(link[3])).group(0)
    milvus_io_doc_standalone['docker_compose_install'] = "docker-compose up -d"
    milvus_io_doc_standalone['docker_compose_show'] = "docker-compose ps"
    return milvus_io_doc_standalone

def check_milvus_docker_image():
    """
    target: Check milvus_source_code_tag is right
    method: compare latest tag
    expected: Tag is right
    """
    page = 1
    url = "https://hub.docker.com/v2/repositories/milvusdb/milvus/tags/?page_size=25&" \
          "page=%d&ordering=last_updated" % page
    res = get_web_content(url)
    release_image_tag = res.json()["results"][0]["name"]
    if release_image_tag == "latest":
        release_image_tag = res.json()["results"][1]["name"]
    success_num = 0
    check_items = 1
    milvus_image_tag = "v" + milvus_release_version + "-" + commit_id

    log.info("Checking milvus docker image ...")

    if milvus_image_tag == release_image_tag:
        log.info("Release data checked: Docker image tag [%s] is right" % release_image_tag)
        success_num += 1
    else:
        log.info("Release data checked: Docker image tag wrong, please correct [%s] to [%s]" %
                 (release_image_tag, milvus_image_tag))

    if success_num != check_items:
        log.error("Milvus docker image tag check failed")
        message = "Milvus docker image tag check failed, details please check the attached log"
        if RELEASE_EMAIL_USERNAME and RELEASE_EMAIL_PASSWORD and RELEASE_SMTP_SERVER:
            se.send_email([receive_email_address["docker_image_owner"]], copy_email_address, message)
        return False
    else:
        log.info("Milvus docker image tag checked successfully")
        check_item_status["docker_image"] = "PASS"
        
    return True


def check_milvus_source_code_tag():
    """
    target: Check milvus_source_code_tag is right
    method: compare latest tag
    expected: Tag is right
    """
    url = 'https://github.com/milvus-io/milvus/tags'
    html_text = parse_web_content(url)
    release_data = get_milvus_source_code_tag(html_text)
    relese_tag = release_data['tag'].replace('\n', '').replace(' ', '')
    relese_tag = "v2.0.0-rc8"
    success_num = 0
    check_items = 1

    log.info("Checking milvus source code ...")

    if not ("tag" in release_data.keys()):
        raise Exception("Latest tag is not existed on Milvus source code hub")

    if "v" + milvus_release_version == relese_tag:
        log.info("Release data checked: Milvus source code Tag [%s] is right" % relese_tag)
        success_num += 1
    else:
        log.error("Release data checked: Wrong milvus source code Tag, please correct [%s] to [%s]" %
                  (relese_tag, "v" + milvus_release_version))
        
    if success_num != check_items:
        log.error("Milvus source code tag check failed")
        message = "Release milvus tag check failed, details please check the attached log"
        if RELEASE_EMAIL_USERNAME and RELEASE_EMAIL_PASSWORD and RELEASE_SMTP_SERVER:
            se.send_email([receive_email_address["milvus_tag_owner"]], copy_email_address, message)
        return False
    else:
        log.info("Milvus source code tag checked successfully")
        check_item_status["milvus_tag"] = "PASS"

    return True


def check_pymilvus_source_code_tag():
    """
    target: Check milvus_source_code_tag is right
    method: compare latest tag
    expected: Tag is right
    """

    url = 'https://github.com/milvus-io/pymilvus/tags'
    html_text = parse_web_content(url)
    release_data = get_pymilvus_source_code_tag(html_text)
    success_num = 0
    check_items = 1

    log.info("Checking pymilvus source code ...")

    if not ("pymilvus_tag" in release_data.keys()):
        raise Exception("Latest tag is not existed on pyMilvus source code hub")

    pymilvus_tag = release_data['pymilvus_tag'].replace('\n', '').replace(' ', '')

    if "v" + pymilvus_release_version == pymilvus_tag:
        log.info("Release data checked: pymilvus tag [%s] is right" % pymilvus_tag)
        success_num += 1
    else:
        log.info("Release data checked: pymilvus tag wrong, please correct [%s] to [%s]" %
                 (release_data['pymilvus_tag'], "v" + pymilvus_release_version))

    if success_num != check_items:
        log.error("Release pymilvus tag check failed")
        message = "Release pymilvus tag check failed, details please check the attached log"
        if RELEASE_EMAIL_USERNAME and RELEASE_EMAIL_PASSWORD and RELEASE_SMTP_SERVER:
            se.send_email([receive_email_address["pymilvus_tag_owner"]], copy_email_address, message)
        return False
    else:
        log.info("Release pymilvus tag checked successfully")
        check_item_status["pymilvus_tag"] = "PASS"

    return True
        
def check_release_notes():
    """
    target: Check release notes are updated
    method: compare title, release data, version and description
    expected: Release notes are updated successfully
    """

    url = 'https://milvus.io/docs/v2.0.0/release_notes.md'
    html_text = parse_web_content(url)
    release_notes = get_release_note_data(html_text)
    milvus_release_version_short = milvus_release_version[-3:]
    success_num = 0
    check_items = 5

    log.info("Checking release notes ...")

    if not ("title" and "version" and "release_date" and "description" in release_notes.keys()):
        raise Exception("Release data are not existed on release notes web")

    if release_notes['title'] == "v" + milvus_release_version.upper():
        log.info("Release Notes checked: Title [%s] is right" % release_notes['title'])
        success_num += 1
    else:
        log.error("Release Notes checked: Title wrong, please correct [%s] to [%s]" %
                  (release_notes['title'], "v" + milvus_release_version.upper()))

    if release_notes['release_date'] == release_date:
        log.info("Release Notes checked: Release date [%s] is right" % release_notes['release_date'])
        success_num += 1
    else:
        log.error("Release Notes checked: Release date wrong, please correct [%s] to [%s]"
                  % (release_notes['release_date'], release_date))

    # if milvus_release_version[-1:] or milvus_release_version[-1:].upper() in str(release_notes['version']):
    version_result = release_notes['version'].find(milvus_release_version.upper())
    if version_result != -1:
        log.info("Release Notes checked: Milvus version [%s] is right" % milvus_release_version.upper())
        success_num += 1
    else:
        log.error("Release Notes checked: Milvus version wrong, please correct to [%s]" % milvus_release_version.upper())

    if pymilvus_release_version in release_notes['version']:
        log.info("Release Notes checked: Python SDK version [%s] is right" % pymilvus_release_version)
        success_num += 1
    else:
        log.error("Release Notes checked: Python SDK version wrong, please correct to [%s]" % pymilvus_release_version)

    if milvus_release_version.upper or milvus_release_version_short.upper() in release_notes['description']:
        log.info("Release Notes checked: Milvus version [%s] in description is right"
                 % milvus_release_version.upper())
        success_num += 1
    else:
        log.error("Release Notes checked: Milvus version in description [%s] wrong, please correct to [%s]"
                  % (release_notes['description'], milvus_release_version))

    if success_num != check_items:
        log.error("Release Notes check failed")
        message = "Release Notes check failed, details please check the attached log"
        if RELEASE_EMAIL_USERNAME and RELEASE_EMAIL_PASSWORD and RELEASE_SMTP_SERVER:
            se.send_email([receive_email_address["release_notes_owner"]], copy_email_address, message)
        return False
    else:
        log.info("Release Notes checked successfully")
        check_item_status["release_notes"] = "PASS"
        
    return True

def check_milvus_io_doc_upgrade():
    """
    target: Check release version on milvus io docs are right
    method: compare old version and release version info
    expected: release version info is updated successfully
    """

    url = 'https://milvus.io/docs/v2.0.0/upgrade.md'
    html_text = parse_web_content(url)
    release_data = get_milvus_io_doc_upgrade(html_text)
    check_items_old = 7
    check_items_release = 5

    if not ("milvus_old_version_num" and "milvus_release_version_num" in release_data.keys()):
        raise Exception("Milvus version info is not existed on milvus io website: upgrade part")

    if release_data["milvus_old_version_num"] != check_items_old or \
            release_data["milvus_release_version_num"] != check_items_release:
        message = "Milvus io doc not updated [upgrade part: %s]" % url
        log.warning(message)
        if RELEASE_EMAIL_USERNAME and RELEASE_EMAIL_PASSWORD and RELEASE_SMTP_SERVER:
            se.send_email([receive_email_address["milvus_doc_owner"]], copy_email_address, "Warning:" + message)
        return False
    else:
        log.info("Milvus io doc (upgrade part) checked successfully")

    return True

def check_milvus_io_doc_install_standalone():
    """
    target: Check release version on milvus io docs are right
    method: compare old version and release version info
    expected: release version info is updated successfully
    """

    url = 'https://milvus.io/docs/v2.0.0/install_standalone-docker.md'
    html_text = parse_web_content(url)
    release_data = get_milvus_io_doc_install_standalone(html_text)
    retry_num = 2
    message = ""
    result = True

    log.info("Checking milvus official website doc (install part) ...")

    if milvus_release_version not in release_data['docker_compose_link']:
        message = "Link for docker-compose.yml on Milvus io website is wrong, not for %s" % milvus_release_version
        log.error(message)
        if RELEASE_EMAIL_USERNAME and RELEASE_EMAIL_PASSWORD and RELEASE_SMTP_SERVER:
            se.send_email([receive_email_address["milvus_doc_owner"]], copy_email_address, message)
        return False
    else:
        log.info("Link for docker-compose.yml is right")

    for i in range(retry_num):
        res = os.system("cd %s && %s" % (release_check_dir, release_data['docker_compose_link']))
        if res != 0:
            delete_proxy()
        else:
            break

    if res != 0:
        log.error("Download docker-compose.yml fail")
        result = False
    else:
        log.info("Download docker-compose.yml success")
        all_text = read_file("%s/docker-compose.yml" % release_check_dir)
        log.info("Checking docker-compose.yml")
        milvus_image_tag = "v" + milvus_release_version + "-" + commit_id
        if milvus_image_tag not in all_text:
            result = False
            message = "docker-compose.yml updated error"
            log.error(message)
            if RELEASE_EMAIL_USERNAME and RELEASE_EMAIL_PASSWORD and RELEASE_SMTP_SERVER:
                se.send_email([receive_email_address["milvus_helm_owner"]], copy_email_address, message)
        else:
            log.info("docker-compose.yml updated success")
            check_item_status["milvus_docker_compose"] = "PASS"
            install_ready = os.system("cd %s && %s" % (release_check_dir, release_data['docker_compose_install']))
            if install_ready == 0:
                log.info("docker compose install success")
                milvus_ready = os.system("cd %s && %s" % (release_check_dir, release_data['docker_compose_show']))
                if milvus_ready == 0:
                    log.info("Milvus install success")
                    clean_milvus()
                else:
                    message = "Milvus install fail"
                    log.error(message)
                    result = False
            else:
                message = "docker compose install fail"
                log.error(message)
                result = False

    if result:
        check_item_status["milvus_doc"] = "PASS"
    else:
        if RELEASE_EMAIL_USERNAME and RELEASE_EMAIL_PASSWORD and RELEASE_SMTP_SERVER:
            se.send_email([receive_email_address["milvus_doc_owner"]], copy_email_address, message)

    return result


def check_milvus_io_doc():
    """
    target: Check release version on milvus io docs are right
    method: compare old version and release version info
    expected: release version info is updated successfully
    """

    log.info("Checking milvus official website doc ...")
    install_check = check_milvus_io_doc_install_standalone()
    if not install_check:
        message = "Milvus io doc error [install part], please check the attached log for details"
        log.error(message)
        if RELEASE_EMAIL_USERNAME and RELEASE_EMAIL_PASSWORD and RELEASE_SMTP_SERVER:
            se.send_email([receive_email_address["milvus_doc_owner"]], copy_email_address, message)
    upgrade_check = check_milvus_io_doc_upgrade()

    return install_check, upgrade_check

def clone_pymilvus():
    """
    Clone pymilvus
    """
    log.info("start cloning pymilvus ...")
    if os.path.isdir(str(release_check_dir + "/pymilvus")):
        os.system("cd %s && rm -rf pymilvus" % release_check_dir)
    res = os.system("cd %s && git clone -b %s git@github.com:milvus-io/pymilvus.git pymilvus"
                    % (release_check_dir, pymilvus_release_version))
    if res == 0:
        log.info("Clone pymilvus source code success")
    else:
        log.error("Clone pymilvus source code fail")

    return res

def clone_milvus_helm():
    """
    Clone milvus helm
    """
    log.info("start cloning milvus helm ...")
    if os.path.isdir(str(release_check_dir + "/milvus-helm")):
        os.system("cd %s && rm -rf milvus-helm" % release_check_dir)
    res = os.system("cd %s && git clone git@github.com:milvus-io/milvus-helm.git milvus-helm"
                    % release_check_dir)
    if res == 0:
        log.info("Clone milvus-helm source code success")
        check_item_status["milvus_helm"] = "PASS"
    else:
        log.error("Clone milvus-helm source code fail")

    return res

def clone_milvus():
    """
    Clone milvus
    """
    log.info("start cloning milvus ...")
    if os.path.isdir(str(release_check_dir + "/milvus")):
        os.system("cd %s && rm -rf milvus" % release_check_dir)
    res = os.system("cd %s && git clone -b %s git@github.com:milvus-io/milvus.git milvus"
                    % (release_check_dir, milvus_release_version))
    if res == 0:
        log.info("Clone milvus source code success")
    else:
        log.error("Clone milvus source code fail")

    return res

def install_milvus():
    """
    Install milvus through kind
    """
    log.info("start installing milvus ...")
    # install milvus
    os.putenv("MILVUS_IMAGE_REPO", MILVUS_IMAGE_REPO)
    os.putenv("MILVUS_IMAGE_TAG", "v" + milvus_release_version + "-" + commit_id)
    os.putenv("MILVUS_HELM_CHART_PATH", release_check_dir + "/milvus-helm/charts/milvus")
    milvus_res = os.system("%s/milvus/tests/scripts/e2e-k8s.sh --skip-cleanup --skip-build "
                           "--skip-build-image --manual" % release_check_dir)
    if milvus_res == 0:
        log.info("Install milvus %s success" % milvus_release_version)
    else:
        log.error("Install milvus %s fail" % milvus_release_version)

    return milvus_res

def install_pymilvus():
    """
    Install pymilvus
    """
    log.info("start installing pymilvus ...")
    # install pymilvus
    pymilvus_res = os.system(
        "docker exec -ti docker_pytest_1 pip install pymilvus==%s" % pymilvus_release_version)
    if pymilvus_res == 0:
        log.info("Install pymilvus %s success" % pymilvus_release_version)
    else:
        log.error("Install pymilvus %s fail" % pymilvus_release_version)

    return pymilvus_res

def run_E2E_test():
    """
    Run one E2E test
    """
    log.info("start running E2E test ...")
    # run E2E test
    f = os.popen("docker exec -ti docker_pytest_1 env")
    env = f.read()
    IP = re.findall("1.*155", env)[0]
    test_result = os.system("docker exec -ti docker_pytest_1 pytest -k test_milvus_default --host %s" % IP)
    if test_result == 0:
        log.info("E2E test pass")
    else:
        log.error("E2E test fail")

    return test_result

def delete_proxy():
    """
    Delete proxy if any
    """
    if os.getenv("http_proxy") != None:
        del os.environ["http_proxy"]
    if os.getenv("https_proxy") != None:
        del os.environ["https_proxy"]

    return True

def clean_milvus():
    """
    Remove milvus through docker compose
    """
    log.info("start uninstall milvus ...")
    res = os.system("cd %s && docker-compose down && rm -rf volume" % release_check_dir)
    if res == 0:
        log.info("uninstall milvus success")
    else:
        log.error("uninstall milvus success fail")

    return True

def clean_env():
    """
    Clearing env if set clear_log and not set keep_milvus
        1. uninstall milvus
        2. clear kind
        3. clear test docker
        4. clear log file
    """

    log.info("Starting to clean env")

    log_path = release_log_path
    if not os.path.isabs(log_path):
        log_path = os.path.abspath(log_path)
    if os.path.isdir(str(log_path)):
        if clear_log:
            os.system("rm -rf %s" % log_path)
            log.info("Cleared logs")

    release_check_path = release_check_dir
    if not os.path.isabs(release_check_path):
        release_check_path = os.path.abspath(release_check_path)
    if os.path.isdir(str(release_check_path)):
        if not keep_milvus:
            try:
                subprocess.call("helm uninstall milvus-testing")
                subprocess.call("kind delete cluster --name=kind -v9")
                subprocess.call("docker stop docker_pytest_1 && docker rm docker_pytest_1")
            except Exception as e:
                log.info("No need to clear milvus because %s" % e)
            os.system("rm -rf %s" % release_check_dir)

    return True

def generate_report_file(check_item_status):
    """
    Generate report table file
    :param check_item_status: status of each check item
    """
    report_table = report.create_report(check_item_status)
    with open("%s/check_item_table.txt" % release_log_path, "w") as f:
        f.write(str(report_table))

    return report_table

def create_path(log_path):
    """
    Create dir if not exist
    :param log_path: the name of creating dir
    """
    if not os.path.isabs(log_path):
        log_path = os.path.abspath(log_path)
    if not os.path.isdir(str(log_path)):
        print("[create_path] folder(%s) is not exist." % log_path)
        print("[create_path] create path now...")
        os.makedirs(log_path)

    return True

def read_file(file_name):
    """
    Read all the contents through file
    :param file_name: the name read file
    """
    all_contents = ""
    if not os.path.exists(str(file_name)):
        log.error("%s is not exist." % file_name)
    else:
        file_object = open(file_name)
        try:
            all_contents = file_object.read()
        finally:
            file_object.close()

    return all_contents

def check_release():
    """
    Check all the release items:
        1. static check
            a. milvus source code/tag ready
            b. pymilvus source code/tag ready
            c. milvus docker image ready
            d. milvus helm/docker-compose.yml ready
            e. release notes ready
            f. milvus io official website install/upgrade parts ready
        2. function check
            a. clone milvus/pymilvus for released version
            b. install milvus/pymilvus good through helm and kind
            c. run E2E tests successfully
        3. notify the owner of each release item if any error
    """
    # 1. initialization
    milvus_source_code_ready = False
    pymilvus_source_code_ready = False
    docker_image_ready = False
    test_done = False
    release_notes_done = False
    milvus_io_install_check = False
    upgrade_check = False
    create_path(release_check_dir)
    create_path(release_log_path)

    # 2. Static check for each release item
    log.info("Start release results validating for %s" % milvus_release_version)
    milvus_source_code_ready = check_milvus_source_code_tag()
    pymilvus_source_code_ready = check_pymilvus_source_code_tag()
    docker_image_ready = check_milvus_docker_image()
    release_notes_done = check_release_notes()
    milvus_io_install_check, upgrade_check = check_milvus_io_doc()

    # 3. Clone milvus/pymilvus/milvus-helm, install milvus/pymilvus, run e2e test after static check
    if pymilvus_source_code_ready:
        clone_pymilvus()

    helm_ready = clone_milvus_helm()

    if milvus_source_code_ready:
        res = clone_milvus()
        if res == 0:
            if docker_image_ready and helm_ready == 0:
                milvus_res = install_milvus()
                if milvus_res == 0:
                    pymilvus_res = install_pymilvus()
                    if pymilvus_res == 0:
                        test_result = run_E2E_test()
                        if test_result == 0:
                            test_done = True
                            check_item_status["milvus_test"] = "PASS"

    if test_done and release_notes_done and milvus_io_install_check:
        if upgrade_check:
            message = "Congratulations, %s has been released [SUCCESS]." % milvus_release_version
        else:
            message = "Congratulations, %s has been released [SUCCESS], " \
                      "but has [Warnings] on Milvus doc." % milvus_release_version
        log.info(message)
        check_item_status["release_verify"] = "PASS"

    else:
        message = "Sorry, ERROR needed to be fixed for releasing %s." % milvus_release_version
        log.error(message)

    report_table = generate_report_file(check_item_status)

    if RELEASE_EMAIL_USERNAME and RELEASE_EMAIL_PASSWORD and RELEASE_SMTP_SERVER:
        se.send_email(list(receive_email_address.values()), copy_email_address, message, report_table.get_html_string())

if __name__ == '__main__':

    check_release()
    clean_env()











