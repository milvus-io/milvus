def get_resolved_scalar_index_version(nodes):
    for node in nodes:
        configurations = node.get("infos", {}).get("system_configurations", {})
        version = configurations.get("resolved_scalar_index_version")
        if version is not None:
            return int(version)
    return None


def get_segment_max_size(nodes):
    """Return DataCoord's configured segment max size in MiB."""
    for node in nodes:
        configurations = node.get("infos", {}).get("system_configurations", {})
        max_size = configurations.get("segment_max_size")
        if max_size is not None:
            return int(max_size)
    return None
