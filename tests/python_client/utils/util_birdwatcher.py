import os
import re
from utils.util_log import test_log as log


def extraction_all_data(text):
    # Patterns to handle the specifics of each key-value line
    patterns = {
        'Segment ID': r"Segment ID:\s*(\d+)",
        'Segment State': r"Segment State:\s*(\w+)",
        'Collection ID': r"Collection ID:\s*(\d+)",
        'PartitionID': r"PartitionID:\s*(\d+)",
        'Insert Channel': r"Insert Channel:(.+)",
        'Num of Rows': r"Num of Rows:\s*(\d+)",
        'Max Row Num': r"Max Row Num:\s*(\d+)",
        'Last Expire Time': r"Last Expire Time:\s*(.+)",
        'Compact from': r"Compact from:\s*(\[\])",
        'Start Position ID': r"Start Position ID:\s*(\[[\d\s]+\])",
        'Start Position Time': r"Start Position ID:.*time:\s*(.+),",
        'Start Channel Name': r"channel name:\s*([^,\n]+)",
        'Dml Position ID': r"Dml Position ID:\s*(\[[\d\s]+\])",
        'Dml Position Time': r"Dml Position ID:.*time:\s*(.+),",
        'Dml Channel Name': r"channel name:\s*(.+)",
        'Binlog Nums': r"Binlog Nums:\s*(\d+)",
        'StatsLog Nums': r"StatsLog Nums:\s*(\d+)",
        'DeltaLog Nums': r"DeltaLog Nums:\s*(\d+)"
    }

    refined_data = {}
    for key, pattern in patterns.items():
        match = re.search(pattern, text)
        if match:
            refined_data[key] = match.group(1).strip()

    return refined_data


class BirdWatcher:
    """

    birdwatcher is a cli tool to get information about milvus
    the command:
    show segment info
    """

    def __init__(self, etcd_endpoints, root_path):
        self.prefix = f"birdwatcher --olc=\"#connect --etcd {etcd_endpoints} --rootPath={root_path},"

    def parse_segment_info(self, output):
        splitter = output.strip().split('\n')[0]
        segments = output.strip().split(splitter)
        segments = [segment for segment in segments if segment.strip()]

        # Parse all segments
        parsed_segments = [extraction_all_data(segment) for segment in segments]
        parsed_segments = [segment for segment in parsed_segments if segment]
        return parsed_segments

    def show_segment_info(self, collection_id=None):
        cmd = f"{self.prefix} show segment info --format table\""
        if collection_id:
            cmd = f"{self.prefix} show segment info --collection {collection_id} --format table\""
        log.info(f"cmd: {cmd}")
        output = os.popen(cmd).read()
        # log.info(f"{cmd} output: {output}")
        output = self.parse_segment_info(output)
        for segment in output:
            log.info(segment)
        seg_res = {}
        for segment in output:
            seg_res[segment['Segment ID']] = segment
        return seg_res


if __name__ == "__main__":
    birdwatcher = BirdWatcher("10.104.18.24:2379", "rg-test-613938")
    res = birdwatcher.show_segment_info()
    print(res)

