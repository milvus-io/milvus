def get_resolved_scalar_index_version(nodes):
    for node in nodes:
        configurations = node.get("infos", {}).get("system_configurations", {})
        version = configurations.get("resolved_scalar_index_version")
        if version is not None:
            return int(version)
    return None
