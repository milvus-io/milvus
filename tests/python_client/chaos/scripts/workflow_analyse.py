import requests
requests.packages.urllib3.disable_warnings() # noqa
url = "https://api.github.com/repos/milvus-io/milvus/actions/workflows"

payload = {}
token = ""  # your token
headers = {
    "Authorization": f"token {token}",
}

response = requests.request("GET", url, headers=headers, data=payload)


def analysis_workflow(workflow_name, workflow_response):
    workflow_id = [w["id"] for w in workflow_response.json()["workflows"] if workflow_name in w["name"]][0]    
    runs_response = requests.request("GET", f"https://api.github.com/repos/milvus-io/milvus/actions/workflows/{workflow_id}/runs", headers=headers, data=payload, verify=False)    
    workflow_runs = [r["id"] for r in runs_response.json()["workflow_runs"] if r["status"] == "completed" and r["event"] == "schedule"]
    results = {}
    for run in workflow_runs:
        job_url = f"https://api.github.com/repos/milvus-io/milvus/actions/runs/{run}/jobs"
        job_response = requests.request("GET", job_url, headers=headers, data=payload, verify=False)
        for r in job_response.json()["jobs"]:
            if r["name"] not in results:
                results[r["name"]] = {"success": 0, "failure": 0}
            if r["status"] == "completed" and r["conclusion"] == "success":
                results[r["name"]]["success"] += 1
            elif r["status"] == "completed" and r["conclusion"] != "success":
                results[r["name"]]["failure"] += 1
    return results


for workflow in ["Pod Kill"]:
    result = analysis_workflow(workflow, response)
    print(f"{workflow}:")
    for k, v in result.items():
        print(f"{k} success: {v['success']}, failure: {v['failure']}")    
    print("\n")
