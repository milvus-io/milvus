import requests
requests.packages.urllib3.disable_warnings()
url = "https://api.github.com/repos/milvus-io/milvus/actions/workflows"

payload={}
token = "" # your token
headers = {
    "Authorization": f"token {token}",
}

response = requests.request("GET", url, headers=headers, data=payload)
pod_kill_workflow_id = [w["id"] for w in response.json()["workflows"] if "Pod Kill" in w["name"]][0]
pod_failure_workflow_id = [w["id"] for w in response.json()["workflows"] if "Pod Failure" in w["name"]][0]



response_pod_kill = requests.request("GET", f"https://api.github.com/repos/milvus-io/milvus/actions/workflows/{pod_kill_workflow_id}/runs", headers=headers, data=payload, verify=False)
pod_kill_runs = [r["id"] for r in response_pod_kill.json()["workflow_runs"] if r["status"] == "completed" and r["event"] == "schedule"]

response_pod_failure = requests.request("GET", f"https://api.github.com/repos/milvus-io/milvus/actions/workflows/{pod_failure_workflow_id}/runs", headers=headers, data=payload, verify=False)
pod_failure_runs = [r["id"] for r in response_pod_failure.json()["workflow_runs"] if r["status"] == "completed" and r["event"] == "schedule"]


# pod kill analysis
pod_kill_result = {}
for run in pod_kill_runs:
    pod_kill_jobs_url = f"https://api.github.com/repos/milvus-io/milvus/actions/runs/{run}/jobs"
    response_pod_kill = requests.request("GET", pod_kill_jobs_url, headers=headers, data=payload, verify=False)
    for r in response_pod_kill.json()["jobs"]:
        if r["name"] not in pod_kill_result:
            pod_kill_result[r["name"]] = {"success": 0, "failure": 0}
        if r["status"] == "completed" and r["conclusion"] == "success":
            pod_kill_result[r["name"]]["success"] += 1
        elif r["status"] == "completed" and r["conclusion"] != "success":
            pod_kill_result[r["name"]]["failure"] += 1
    
for k,v in pod_kill_result.items():
    print(f"{k} success: {v['success']}, failure: {v['failure']}")


# pod failure analysis
pod_failure_result = {}
for run in pod_failure_runs:
    pod_failure_jobs_url = f"https://api.github.com/repos/milvus-io/milvus/actions/runs/{run}/jobs"
    response_pod_failure = requests.request("GET", pod_failure_jobs_url, headers=headers, data=payload, verify=False)
    for r in response_pod_failure.json()["jobs"]:
        if r["name"] not in pod_failure_result:
            pod_failure_result[r["name"]] = {"success": 0, "failure": 0}
        if r["status"] == "completed" and r["conclusion"] == "success":
            pod_failure_result[r["name"]]["success"] += 1
        elif r["status"] == "completed" and r["conclusion"] == "failure":
            pod_failure_result[r["name"]]["failure"] += 1
    
for k,v in pod_failure_result.items():
    print(f"{k} success: {v['success']}, failure: {v['failure']}")
