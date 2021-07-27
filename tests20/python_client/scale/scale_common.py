import os

from scale import constants
from utils.util_log import test_log as log


def get_milvus_chart_env_var(var=constants.MILVUS_CHART_ENV):
    """ get log path for testing """
    try:
        milvus_helm_chart = os.environ[var]
        return str(milvus_helm_chart)
    except Exception as e:
        milvus_helm_chart = constants.MILVUS_CHART
        log.warning(f"Failed to get environment variables: {var}, use default: {milvus_helm_chart}, error: {str(e)}")
        return milvus_helm_chart
