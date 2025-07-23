def find_equipment_poc_connections(
    db: Database,
    equipment_pocs: list[tuple],
    equipment_poc_nodes: dict[int, list[tuple]]
) -> Optional[list[tuple]]:
    """
    Analyze downstream connections for each equipment POC.
    """
    pathfinder = NetworkPathfinder(db, unattended=True)
    connections: list[tuple] = []
    node_to_poc = build_node_to_poc_map(equipment_poc_nodes)

    print(f'\n  Analyzing connections for {len(equipment_pocs)} POCs...')

    for idx, poc_data in enumerate(equipment_pocs, 1):
        (
            equipment_id, equipment_guid, equipment_node_id,
            poc_id, poc_node_id, utility_no,
            utility, reference, is_loopback
        ) = poc_data

        if idx % 50 == 0:
            print(f'   + Processed {idx}/{len(equipment_pocs)} POCs...')

        # Build forbidden node set: exclude other non-loopback PoCs from same equipment
        forbidden_node_ids = {
            node for _, _, node, loopback in equipment_poc_nodes.get(equipment_id, [])
            if not loopback
        }
        forbidden_node_ids.add(equipment_node_id)
        if poc_node_id:
            forbidden_node_ids.discard(poc_node_id)

        # Pathfinding
        paths = find_downstream_path(
            pathfinder,
            s_node_id=poc_node_id,
            forbidden_node_ids=forbidden_node_ids,
            utility_no=utility_no or 0,
            data_codes={15000, 50001, 50002, 50003},
        )

        if not paths:
            continue

        for path in paths:
            endpoint_node_id = getattr(path, "e_node_id", None)
            if not endpoint_node_id:
                continue

            target_equipment_id, target_poc_id = find_poc_by_node_id(endpoint_node_id, node_to_poc)
            if not target_equipment_id or not target_poc_id:
                continue

            if target_equipment_id == equipment_id and not is_loopback:
                continue

            is_active = determine_connection_activity(
                equipment_id, target_equipment_id, poc_id, target_poc_id, path
            )

            connection_type = determine_connection_type(path)

            connection = (
                equipment_id,
                target_equipment_id,
                poc_id,
                target_poc_id,
                path.path_id,
                connection_type,
                is_active,
            )
            connections.append(connection)

    print(f'       - Analysis complete. Found {len(connections)} connections.')
    return connections


from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Iterator

def find_equipment_poc_connections(
    db: Database,
    equipment_pocs: list[tuple],
    equipment_poc_nodes: dict[int, list[tuple]]
) -> Optional[list[tuple]]:
    """
    Analyze downstream connections for each equipment POC using parallel threads.
    Returns list of connection tuples for insertion.
    """
    pathfinder = NetworkPathfinder(db, unattended=True)
    node_to_poc = build_node_to_poc_map(equipment_poc_nodes)
    total_pocs = len(equipment_pocs)

    print(f'\n  Analyzing connections for {total_pocs} POCs (parallel)...')

    def analyze_single_poc(poc_data: tuple) -> list[tuple]:
        (
            equipment_id, equipment_guid, equipment_node_id,
            poc_id, poc_node_id, utility_no,
            utility, reference, is_loopback
        ) = poc_data

        forbidden_node_ids = {
            node for _, _, node, loopback in equipment_poc_nodes.get(equipment_id, [])
            if not loopback
        }
        forbidden_node_ids.add(equipment_node_id)
        if poc_node_id:
            forbidden_node_ids.discard(poc_node_id)

        paths = find_downstream_path(
            pathfinder,
            s_node_id=poc_node_id,
            forbidden_node_ids=forbidden_node_ids,
            utility_no=utility_no or 0,
            data_codes={15000, 50001, 50002, 50003},
        )

        if not paths:
            return []

        connections = []
        for path in paths:
            endpoint_node_id = getattr(path, "e_node_id", None)
            if not endpoint_node_id:
                continue

            target_equipment_id, target_poc_id = find_poc_by_node_id(endpoint_node_id, node_to_poc)
            if not target_equipment_id or not target_poc_id:
                continue

            if target_equipment_id == equipment_id and not is_loopback:
                continue

            is_active = determine_connection_activity(
                equipment_id, target_equipment_id, poc_id, target_poc_id, path
            )
            connection_type = determine_connection_type(path)

            connections.append((
                equipment_id,
                target_equipment_id,
                poc_id,
                target_poc_id,
                path.path_id,
                connection_type,
                is_active,
            ))

        return connections

    # Use ThreadPoolExecutor — appropriate if `find_downstream_path` releases GIL (e.g., IO, C extensions)
    connections: list[tuple] = []
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(analyze_single_poc, poc): i for i, poc in enumerate(equipment_pocs, 1)}

        for future in as_completed(futures):
            idx = futures[future]
            if idx % 50 == 0:
                print(f'   + Processed {idx}/{total_pocs} POCs...')

            try:
                result = future.result()
                if result:
                    connections.extend(result)
            except Exception as e:
                print(f'     ! Error in POC #{idx}: {e}')

    print(f'       - Analysis complete. Found {len(connections)} connections.')
    return connections
