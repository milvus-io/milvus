"""Provision old Milvus and run only the T11 forward rolling upgrade."""

from run_running_requests_ci import main


if __name__ == "__main__":
    main(upgrade=True)
