import pytest
import random
import numpy as np
from shapely import wkt

from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from pymilvus import DataType
from utils.util_log import test_log as log

prefix = "client_geometry"
default_nb = ct.default_nb
default_dim = ct.default_dim
default_limit = ct.default_limit


def generate_wkt_by_type(
    wkt_type: str, bounds: tuple = (0, 100, 0, 100), count: int = 10
) -> list:
    """
    Generate WKT examples dynamically based on geometry type

    Args:
        wkt_type: Type of WKT geometry to generate
        bounds: Coordinate bounds as (min_x, max_x, min_y, max_y)
        count: Number of geometries to generate

    Returns:
        List of WKT strings
    """

    def validate_and_log_wkt(wkt_string: str, geom_type: str) -> str:
        """Validate WKT using shapely and log debug info"""
        try:
            geom = wkt.loads(wkt_string)
            log.debug(f"Generated {geom_type} geometry: {wkt_string}")
            log.debug(f"Shapely validation passed - Type: {geom.geom_type}, Valid: {geom.is_valid}")
            if not geom.is_valid:
                log.warning(f"Generated invalid geometry: {wkt_string}, Reason: {geom.is_valid_reason if hasattr(geom, 'is_valid_reason') else 'Unknown'}")
            return wkt_string
        except Exception as e:
            log.error(f"Failed to parse WKT {geom_type}: {wkt_string}, Error: {e}")
            raise ValueError(f"Invalid WKT generated for {geom_type}: {wkt_string}")

    if wkt_type == "POINT":
        points = []
        for _ in range(count):
            wkt_string = f"POINT ({random.uniform(bounds[0], bounds[1]):.2f} {random.uniform(bounds[2], bounds[3]):.2f})"
            points.append(validate_and_log_wkt(wkt_string, "POINT"))
        return points

    elif wkt_type == "LINESTRING":
        lines = []
        for _ in range(count):
            points = []
            num_points = random.randint(2, 6)  # More varied line complexity
            for _ in range(num_points):
                x = random.uniform(bounds[0], bounds[1])
                y = random.uniform(bounds[2], bounds[3])
                points.append(f"{x:.2f} {y:.2f}")
            wkt_string = f"LINESTRING ({', '.join(points)})"
            lines.append(validate_and_log_wkt(wkt_string, "LINESTRING"))
        return lines

    elif wkt_type == "POLYGON":
        polygons = []
        for _ in range(count):
            # Generate varied polygon shapes
            if random.random() < 0.7:  # 70% rectangles
                x = random.uniform(bounds[0], bounds[1] - 50)
                y = random.uniform(bounds[2], bounds[3] - 50)
                width = random.uniform(10, 50)
                height = random.uniform(10, 50)
                polygon_wkt = f"POLYGON (({x:.2f} {y:.2f}, {x + width:.2f} {y:.2f}, {x + width:.2f} {y + height:.2f}, {x:.2f} {y + height:.2f}, {x:.2f} {y:.2f}))"
            else:  # 30% triangles
                x1, y1 = (
                    random.uniform(bounds[0], bounds[1]),
                    random.uniform(bounds[2], bounds[3]),
                )
                x2, y2 = (
                    random.uniform(bounds[0], bounds[1]),
                    random.uniform(bounds[2], bounds[3]),
                )
                x3, y3 = (
                    random.uniform(bounds[0], bounds[1]),
                    random.uniform(bounds[2], bounds[3]),
                )
                polygon_wkt = f"POLYGON (({x1:.2f} {y1:.2f}, {x2:.2f} {y2:.2f}, {x3:.2f} {y3:.2f}, {x1:.2f} {y1:.2f}))"
            polygons.append(validate_and_log_wkt(polygon_wkt, "POLYGON"))
        return polygons

    elif wkt_type == "MULTIPOINT":
        multipoints = []
        for _ in range(count):
            points = []
            num_points = random.randint(2, 8)  # 2-8 points per multipoint
            for _ in range(num_points):
                x = random.uniform(bounds[0], bounds[1])
                y = random.uniform(bounds[2], bounds[3])
                points.append(f"({x:.2f} {y:.2f})")
            wkt_string = f"MULTIPOINT ({', '.join(points)})"
            multipoints.append(validate_and_log_wkt(wkt_string, "MULTIPOINT"))
        return multipoints

    elif wkt_type == "MULTILINESTRING":
        multilines = []
        for _ in range(count):
            lines = []
            num_lines = random.randint(2, 5)  # 2-5 lines per multilinestring
            for _ in range(num_lines):
                line_points = []
                num_points = random.randint(2, 4)
                for _ in range(num_points):
                    x = random.uniform(bounds[0], bounds[1])
                    y = random.uniform(bounds[2], bounds[3])
                    line_points.append(f"{x:.2f} {y:.2f}")
                lines.append(f"({', '.join(line_points)})")
            wkt_string = f"MULTILINESTRING ({', '.join(lines)})"
            multilines.append(validate_and_log_wkt(wkt_string, "MULTILINESTRING"))
        return multilines

    elif wkt_type == "MULTIPOLYGON":
        multipolygons = []
        for _ in range(count):
            polygons = []
            num_polygons = random.randint(2, 4)  # 2-4 polygons per multipolygon
            for _ in range(num_polygons):
                x = random.uniform(bounds[0], bounds[1] - 30)
                y = random.uniform(bounds[2], bounds[3] - 30)
                size = random.uniform(10, 30)
                polygon_coords = f"(({x:.2f} {y:.2f}, {x + size:.2f} {y:.2f}, {x + size:.2f} {y + size:.2f}, {x:.2f} {y + size:.2f}, {x:.2f} {y:.2f}))"
                polygons.append(polygon_coords)
            wkt_string = f"MULTIPOLYGON ({', '.join(polygons)})"
            multipolygons.append(validate_and_log_wkt(wkt_string, "MULTIPOLYGON"))
        return multipolygons

    elif wkt_type == "GEOMETRYCOLLECTION":
        collections = []
        for _ in range(count):
            # Generate varied geometry collections
            collection_types = random.randint(2, 4)  # 2-4 geometries per collection
            geoms = []

            for _ in range(collection_types):
                geom_type = random.choice(["POINT", "LINESTRING", "POLYGON"])
                if geom_type == "POINT":
                    x, y = (
                        random.uniform(bounds[0], bounds[1]),
                        random.uniform(bounds[2], bounds[3]),
                    )
                    geoms.append(f"POINT({x:.2f} {y:.2f})")
                elif geom_type == "LINESTRING":
                    x1, y1 = (
                        random.uniform(bounds[0], bounds[1]),
                        random.uniform(bounds[2], bounds[3]),
                    )
                    x2, y2 = (
                        random.uniform(bounds[0], bounds[1]),
                        random.uniform(bounds[2], bounds[3]),
                    )
                    geoms.append(f"LINESTRING({x1:.2f} {y1:.2f}, {x2:.2f} {y2:.2f})")
                else:  # POLYGON
                    x, y = (
                        random.uniform(bounds[0], bounds[1] - 20),
                        random.uniform(bounds[2], bounds[3] - 20),
                    )
                    size = random.uniform(5, 20)
                    geoms.append(
                        f"POLYGON(({x:.2f} {y:.2f}, {x + size:.2f} {y:.2f}, {x + size:.2f} {y + size:.2f}, {x:.2f} {y + size:.2f}, {x:.2f} {y:.2f}))"
                    )

            wkt_string = f"GEOMETRYCOLLECTION({', '.join(geoms)})"
            collections.append(validate_and_log_wkt(wkt_string, "GEOMETRYCOLLECTION"))
        return collections

    else:
        raise ValueError(f"Unsupported WKT type: {wkt_type}")


def generate_spatial_query_data_for_function(
    spatial_func, base_data, geo_field_name="geo"
):
    """
    Generate query geometry for specific spatial function based on base data
    Ensures the query will match multiple results (>1)

    Args:
        spatial_func: The spatial function name (e.g., "ST_INTERSECTS", "ST_CONTAINS")
        base_data: List of base geometry data with format [{"id": int, geo_field_name: "WKT_STRING"}, ...]
        geo_field_name: Name of the geometry field in base_data (default: "geo")

    Returns:
        query_geom: WKT string of the query geometry that should match multiple base geometries
    """
    import re

    def parse_point(wkt):
        """Extract x, y from POINT WKT"""
        match = re.search(r"POINT \(([0-9.-]+) ([0-9.-]+)\)", wkt)
        if match:
            return float(match.group(1)), float(match.group(2))
        return None, None

    def parse_polygon_bounds(wkt):
        """Extract min/max bounds from POLYGON WKT"""
        match = re.search(r"POLYGON \(\(([^)]+)\)\)", wkt)
        if match:
            coords = match.group(1).split(", ")
            xs, ys = [], []
            for coord in coords:
                parts = coord.strip().split()
                if len(parts) >= 2:
                    xs.append(float(parts[0]))
                    ys.append(float(parts[1]))
            if xs and ys:
                return min(xs), max(xs), min(ys), max(ys)
        return None, None, None, None

    if spatial_func == "ST_INTERSECTS":
        # Create a large query polygon that will intersect with many geometries
        all_coords = []
        for item in base_data:
            if "POINT" in item[geo_field_name]:
                x, y = parse_point(item[geo_field_name])
                if x is not None and y is not None:
                    all_coords.append((x, y))
            elif "POLYGON" in item[geo_field_name]:
                min_x, max_x, min_y, max_y = parse_polygon_bounds(item[geo_field_name])
                if min_x is not None:
                    all_coords.append(((min_x + max_x) / 2, (min_y + max_y) / 2))

        if all_coords and len(all_coords) >= 5:
            # Use more coordinates to ensure larger coverage area
            target_coords = all_coords[: min(10, len(all_coords))]
            center_x = sum(coord[0] for coord in target_coords) / len(target_coords)
            center_y = sum(coord[1] for coord in target_coords) / len(target_coords)
            # Increase size to cover more geometries
            size = 40

            query_geom = f"POLYGON (({center_x - size / 2} {center_y - size / 2}, {center_x + size / 2} {center_y - size / 2}, {center_x + size / 2} {center_y + size / 2}, {center_x - size / 2} {center_y + size / 2}, {center_x - size / 2} {center_y - size / 2}))"
        else:
            # Fallback: create a large central polygon
            query_geom = "POLYGON ((30 30, 70 30, 70 70, 30 70, 30 30))"

    elif spatial_func == "ST_CONTAINS":
        # For ST_CONTAINS, we need base geometries (polygons) to contain the query geometry (point)
        # Create a small query point that can be contained by many base polygons
        all_polygons = []
        for item in base_data:
            if "POLYGON" in item[geo_field_name]:
                min_x, max_x, min_y, max_y = parse_polygon_bounds(item[geo_field_name])
                if min_x is not None:
                    # Store polygon bounds with some margin to ensure point is well inside
                    margin = (max_x - min_x) * 0.2  # 20% margin
                    all_polygons.append(
                        (min_x + margin, max_x - margin, min_y + margin, max_y - margin)
                    )

        if all_polygons and len(all_polygons) >= 3:
            # Try to find a point that's inside multiple polygons by checking for actual overlap
            target_polygons = all_polygons[: min(8, len(all_polygons))]
            best_point = None
            best_count = 0

            # Try several candidate points and pick the one inside most polygons
            for _ in range(20):
                # Generate candidate point within the first polygon
                if target_polygons:
                    p = target_polygons[0]
                    if p[0] < p[1] and p[2] < p[3]:  # Valid polygon bounds
                        import random

                        candidate_x = random.uniform(p[0], p[1])
                        candidate_y = random.uniform(p[2], p[3])

                        # Count how many polygons contain this point
                        count = sum(
                            1
                            for poly in target_polygons
                            if poly[0] <= candidate_x <= poly[1]
                            and poly[2] <= candidate_y <= poly[3]
                        )

                        if count > best_count:
                            best_count = count
                            best_point = (candidate_x, candidate_y)

            if best_point and best_count >= 2:
                query_geom = f"POINT ({best_point[0]} {best_point[1]})"
            else:
                # Fallback: use center of first polygon
                p = target_polygons[0]
                if p[0] < p[1] and p[2] < p[3]:
                    center_x = (p[0] + p[1]) / 2
                    center_y = (p[2] + p[3]) / 2
                    query_geom = f"POINT ({center_x} {center_y})"
                else:
                    query_geom = "POINT (50 50)"
        else:
            # If no suitable polygons, create a larger polygon that will contain points
            # This reverses the logic - create a query polygon that contains many points
            points = []
            for item in base_data:
                if "POINT" in item[geo_field_name]:
                    x, y = parse_point(item[geo_field_name])
                    if x is not None and y is not None:
                        points.append((x, y))

            if len(points) >= 3:
                # Create a polygon that contains multiple points
                target_points = points[: min(10, len(points))]
                min_x = min(p[0] for p in target_points) - 5
                max_x = max(p[0] for p in target_points) + 5
                min_y = min(p[1] for p in target_points) - 5
                max_y = max(p[1] for p in target_points) + 5
                query_geom = f"POLYGON (({min_x} {min_y}, {max_x} {min_y}, {max_x} {max_y}, {min_x} {max_y}, {min_x} {min_y}))"
            else:
                # Last fallback: create a large central polygon
                query_geom = "POLYGON ((25 25, 75 25, 75 75, 25 75, 25 25))"

    elif spatial_func == "ST_WITHIN":
        # Find many geometries that could be within a large query polygon
        points_and_small_polys = []
        for item in base_data:
            if "POINT" in item[geo_field_name]:
                x, y = parse_point(item[geo_field_name])
                if x is not None and y is not None:
                    points_and_small_polys.append(
                        (x, y, 0, 0)
                    )  # point as 0-size polygon
            elif "POLYGON" in item[geo_field_name]:
                min_x, max_x, min_y, max_y = parse_polygon_bounds(item[geo_field_name])
                if (
                    min_x is not None and (max_x - min_x) < 30 and (max_y - min_y) < 30
                ):  # small polygons only
                    points_and_small_polys.append(
                        (min_x, min_y, max_x - min_x, max_y - min_y)
                    )

        if points_and_small_polys and len(points_and_small_polys) >= 10:
            # Select many small geometries and create a large containing polygon
            target_geoms = points_and_small_polys[
                : min(30, len(points_and_small_polys))
            ]
            min_x = min(geom[0] for geom in target_geoms) - 10
            max_x = max(geom[0] + geom[2] for geom in target_geoms) + 10
            min_y = min(geom[1] for geom in target_geoms) - 10
            max_y = max(geom[1] + geom[3] for geom in target_geoms) + 10

            query_geom = f"POLYGON (({min_x} {min_y}, {max_x} {min_y}, {max_x} {max_y}, {min_x} {max_y}, {min_x} {min_y}))"
        else:
            # Fallback: large polygon covering most of the space
            query_geom = "POLYGON ((5 5, 95 5, 95 95, 5 95, 5 5))"

    elif spatial_func == "ST_EQUALS":
        # Find a point in base data and create query with same point
        for item in base_data:
            if "POINT" in item[geo_field_name]:
                query_geom = item[geo_field_name]  # Use the same point
                break
        else:
            # If no points found, create a default case
            query_geom = "POINT (25 25)"

    elif spatial_func == "ST_TOUCHES":
        # Create a polygon that touches some base geometries
        points = []
        for item in base_data:
            if "POINT" in item[geo_field_name]:
                x, y = parse_point(item[geo_field_name])
                if x is not None and y is not None:
                    points.append((x, y))

        if points:
            # Use first point to create a touching polygon (point on edge)
            target_point = points[0]
            x, y = target_point[0], target_point[1]
            size = 20
            # Create polygon where the point touches the edge
            query_geom = f"POLYGON (({x} {y - size}, {x + size} {y - size}, {x + size} {y}, {x} {y}, {x} {y - size}))"
        else:
            query_geom = "POLYGON ((0 0, 20 0, 20 20, 0 20, 0 0))"

    elif spatial_func == "ST_OVERLAPS":
        # Find polygons in base data and create overlapping query polygon
        polygons = []
        for item in base_data:
            if "POLYGON" in item[geo_field_name]:
                min_x, max_x, min_y, max_y = parse_polygon_bounds(item[geo_field_name])
                if min_x is not None:
                    polygons.append((min_x, max_x, min_y, max_y))

        if polygons:
            # Create overlapping polygon with first polygon
            target_poly = polygons[0]
            min_x, max_x, min_y, max_y = (
                target_poly[0],
                target_poly[1],
                target_poly[2],
                target_poly[3],
            )
            # Create polygon that overlaps by shifting slightly
            shift = (max_x - min_x) * 0.3
            query_geom = f"POLYGON (({min_x + shift} {min_y + shift}, {max_x + shift} {min_y + shift}, {max_x + shift} {max_y + shift}, {min_x + shift} {max_y + shift}, {min_x + shift} {min_y + shift}))"
        else:
            query_geom = "POLYGON ((10 10, 30 10, 30 30, 10 30, 10 10))"

    elif spatial_func == "ST_CROSSES":
        # Find polygons and create crossing linestrings
        polygons = []
        for item in base_data:
            if "POLYGON" in item[geo_field_name]:
                min_x, max_x, min_y, max_y = parse_polygon_bounds(item[geo_field_name])
                if min_x is not None:
                    polygons.append((min_x, max_x, min_y, max_y))

        if polygons:
            # Create a line that crosses the first polygon
            target_poly = polygons[0]
            min_x, max_x, min_y, max_y = (
                target_poly[0],
                target_poly[1],
                target_poly[2],
                target_poly[3],
            )
            center_x = (min_x + max_x) / 2
            center_y = (min_y + max_y) / 2
            # Create vertical crossing line
            query_geom = (
                f"LINESTRING ({center_x} {min_y - 10}, {center_x} {max_y + 10})"
            )
        else:
            query_geom = "LINESTRING (15 -5, 15 25)"

    else:
        # Default case
        query_geom = "POLYGON ((0 0, 50 0, 50 50, 0 50, 0 0))"

    # Validate and log the generated query geometry
    try:
        geom = wkt.loads(query_geom)
        log.debug(f"Generated {spatial_func} query geometry: {query_geom}")
        log.debug(f"Query geometry validation - Type: {geom.geom_type}, Valid: {geom.is_valid}")
        if not geom.is_valid:
            log.warning(f"Generated invalid query geometry for {spatial_func}: {query_geom}, Reason: {geom.is_valid_reason if hasattr(geom, 'is_valid_reason') else 'Unknown'}")
    except Exception as e:
        log.error(f"Failed to parse query WKT for {spatial_func}: {query_geom}, Error: {e}")

    return query_geom


def generate_diverse_base_data(
    count=9, bounds=(0, 100, 0, 100), pk_field_name="id", geo_field_name="geo"
):
    """
    Generate diverse base geometry data for testing

    Args:
        count: Number of geometries to generate (default: 9)
        bounds: Coordinate bounds as (min_x, max_x, min_y, max_y)
        pk_field_name: Name of the primary key field (default: "id")
        geo_field_name: Name of the geometry field (default: "geo")

    Returns:
        List of geometry data with format [{pk_field_name: int, geo_field_name: "WKT_STRING"}, ...]
    """
    import random

    base_data = []
    min_x, max_x, min_y, max_y = bounds

    def validate_and_log_base_wkt(wkt_string: str, geom_type: str, index: int) -> str:
        """Validate base data WKT using shapely and log debug info"""
        try:
            geom = wkt.loads(wkt_string)
            log.debug(f"Generated base {geom_type} geometry [{index}]: {wkt_string}")
            log.debug(f"Base geometry validation [{index}] - Type: {geom.geom_type}, Valid: {geom.is_valid}")
            if not geom.is_valid:
                log.warning(f"Generated invalid base geometry [{index}]: {wkt_string}, Reason: {geom.is_valid_reason if hasattr(geom, 'is_valid_reason') else 'Unknown'}")
            return wkt_string
        except Exception as e:
            log.error(f"Failed to parse base WKT {geom_type} [{index}]: {wkt_string}, Error: {e}")
            raise ValueError(f"Invalid WKT generated for base {geom_type} [{index}]: {wkt_string}")

    # Generate points (30% of data)
    point_count = int(count * 0.3)
    for _ in range(point_count):
        x = random.uniform(min_x, max_x)
        y = random.uniform(min_y, max_y)
        wkt_string = f"POINT ({x:.2f} {y:.2f})"
        validated_wkt = validate_and_log_base_wkt(wkt_string, "POINT", len(base_data))
        base_data.append(
            {pk_field_name: len(base_data), geo_field_name: validated_wkt}
        )

    # Generate polygons (40% of data)
    polygon_count = int(count * 0.4)
    for _ in range(polygon_count):
        # Generate rectangular polygons with various sizes
        size = random.uniform(5, 20)
        x = random.uniform(min_x, max_x - size)
        y = random.uniform(min_y, max_y - size)

        wkt_string = f"POLYGON (({x:.2f} {y:.2f}, {x + size:.2f} {y:.2f}, {x + size:.2f} {y + size:.2f}, {x:.2f} {y + size:.2f}, {x:.2f} {y:.2f}))"
        validated_wkt = validate_and_log_base_wkt(wkt_string, "POLYGON", len(base_data))
        base_data.append(
            {
                pk_field_name: len(base_data),
                geo_field_name: validated_wkt,
            }
        )

    # Generate linestrings (25% of data)
    line_count = int(count * 0.25)
    for _ in range(line_count):
        # Generate lines with 2-4 points
        point_count = random.randint(2, 4)
        coords = []
        for _ in range(point_count):
            x = random.uniform(min_x, max_x)
            y = random.uniform(min_y, max_y)
            coords.append(f"{x:.2f} {y:.2f}")

        wkt_string = f"LINESTRING ({', '.join(coords)})"
        validated_wkt = validate_and_log_base_wkt(wkt_string, "LINESTRING", len(base_data))
        base_data.append(
            {
                pk_field_name: len(base_data),
                geo_field_name: validated_wkt,
            }
        )

    # Add some specific geometries for edge cases
    remaining = count - len(base_data)
    if remaining > 0:
        # Add duplicate points for ST_EQUALS testing
        if len(base_data) > 0 and "POINT" in base_data[0][geo_field_name]:
            base_data.append(
                {
                    pk_field_name: len(base_data),
                    geo_field_name: base_data[0][
                        geo_field_name
                    ],  # Duplicate first point
                }
            )
            remaining -= 1

        # Fill remaining with random points
        for _ in range(remaining):
            x = random.uniform(min_x, max_x)
            y = random.uniform(min_y, max_y)
            wkt_string = f"POINT ({x:.2f} {y:.2f})"
            validated_wkt = validate_and_log_base_wkt(wkt_string, "POINT", len(base_data))
            base_data.append(
                {
                    pk_field_name: len(base_data),
                    geo_field_name: validated_wkt,
                }
            )

    return base_data


def generate_latlon_data_for_dwithin(count: int = 10, center_lat: float = 40.7128, center_lon: float = -74.0060):
    """
    Generate latitude/longitude based geometry data for ST_DWITHIN testing

    Args:
        count: Number of geometries to generate
        center_lat: Center latitude (default: NYC latitude)
        center_lon: Center longitude (default: NYC longitude)

    Returns:
        List of WKT POINT strings with latitude/longitude coordinates
    """
    def validate_and_log_latlon_wkt(wkt_string: str, point_type: str, index: int) -> str:
        """Validate lat/lon WKT using shapely and log debug info"""
        try:
            geom = wkt.loads(wkt_string)
            log.debug(f"Generated {point_type} lat/lon point [{index}]: {wkt_string}")
            log.debug(f"Lat/lon point validation [{index}] - Type: {geom.geom_type}, Valid: {geom.is_valid}")
            if not geom.is_valid:
                log.warning(f"Generated invalid lat/lon point [{index}]: {wkt_string}, Reason: {geom.is_valid_reason if hasattr(geom, 'is_valid_reason') else 'Unknown'}")
            return wkt_string
        except Exception as e:
            log.error(f"Failed to parse lat/lon WKT [{index}]: {wkt_string}, Error: {e}")
            raise ValueError(f"Invalid lat/lon WKT generated [{index}]: {wkt_string}")

    points = []

    # Add center point
    center_wkt = f"POINT ({center_lon:.6f} {center_lat:.6f})"
    points.append(validate_and_log_latlon_wkt(center_wkt, "center", 0))

    # Add points at various distances from center
    # Using approximate degree-to-meter conversions at NYC latitude
    # 1 degree latitude ≈ 111,320 meters
    # 1 degree longitude ≈ 111,320 * cos(latitude) meters
    lat_degree_per_meter = 1.0 / 111320.0
    lon_degree_per_meter = 1.0 / (111320.0 * np.cos(np.radians(center_lat)))

    distances_meters = [500, 1000, 2000, 5000, 10000, 20000, 50000]
    directions = [0, 45, 90, 135, 180, 225, 270, 315]  # degrees

    point_id = 1
    for distance in distances_meters:
        if point_id >= count:
            break
        for direction in directions:
            if point_id >= count:
                break

            # Convert direction to radians
            direction_rad = np.radians(direction)

            # Calculate lat/lon offset
            lat_offset = distance * lat_degree_per_meter * np.cos(direction_rad)
            lon_offset = distance * lon_degree_per_meter * np.sin(direction_rad)

            new_lat = center_lat + lat_offset
            new_lon = center_lon + lon_offset

            point_wkt = f"POINT ({new_lon:.6f} {new_lat:.6f})"
            points.append(validate_and_log_latlon_wkt(point_wkt, f"distance_{distance}m", point_id))
            point_id += 1

    # Fill remaining slots with random points in nearby area
    while len(points) < count:
        # Random points within 100km radius with varied distribution
        angle = random.uniform(0, 2 * np.pi)

        # Use exponential distribution to have more points closer to center
        # but still cover the full range
        if random.random() < 0.7:  # 70% within 20km
            distance = random.uniform(100, 20000)
        else:  # 30% between 20-100km
            distance = random.uniform(20000, 100000)

        lat_offset = distance * lat_degree_per_meter * np.cos(angle)
        lon_offset = distance * lon_degree_per_meter * np.sin(angle)

        new_lat = center_lat + lat_offset
        new_lon = center_lon + lon_offset

        point_wkt = f"POINT ({new_lon:.6f} {new_lat:.6f})"
        points.append(validate_and_log_latlon_wkt(point_wkt, "random", len(points)))

    return points[:count]


def generate_dwithin_query_point(center_lat: float = 40.7128, center_lon: float = -74.0060):
    """
    Generate a query point for ST_DWITHIN testing

    Args:
        center_lat: Center latitude (default: NYC latitude)
        center_lon: Center longitude (default: NYC longitude)

    Returns:
        WKT POINT string for query
    """
    query_wkt = f"POINT ({center_lon:.6f} {center_lat:.6f})"

    # Validate and log the query point
    try:
        geom = wkt.loads(query_wkt)
        log.debug(f"Generated DWITHIN query point: {query_wkt}")
        log.debug(f"DWITHIN query point validation - Type: {geom.geom_type}, Valid: {geom.is_valid}")
        if not geom.is_valid:
            log.warning(f"Generated invalid DWITHIN query point: {query_wkt}, Reason: {geom.is_valid_reason if hasattr(geom, 'is_valid_reason') else 'Unknown'}")
    except Exception as e:
        log.error(f"Failed to parse DWITHIN query WKT: {query_wkt}, Error: {e}")

    return query_wkt


def calculate_expected_ids_for_dwithin(base_data, query_point, distance_meters, geo_field_name="geo", pk_field_name="id"):
    """
    Calculate expected IDs for ST_DWITHIN using Haversine distance formula

    Args:
        base_data: List of base geometry data
        query_point: WKT POINT string for query
        distance_meters: Distance threshold in meters
        geo_field_name: Name of geometry field
        pk_field_name: Name of primary key field

    Returns:
        Set of expected IDs within the distance
    """
    import re

    def parse_point(wkt):
        """Extract lon, lat from POINT WKT"""
        match = re.search(r"POINT \(([0-9.-]+) ([0-9.-]+)\)", wkt)
        if match:
            return float(match.group(1)), float(match.group(2))  # lon, lat
        return None, None

    def haversine_distance(lat1, lon1, lat2, lon2):
        """Calculate great-circle distance between two points on Earth using Haversine formula"""
        R = 6371000  # Earth's radius in meters

        lat1_rad = np.radians(lat1)
        lon1_rad = np.radians(lon1)
        lat2_rad = np.radians(lat2)
        lon2_rad = np.radians(lon2)

        dlat = lat2_rad - lat1_rad
        dlon = lon2_rad - lon1_rad

        a = (np.sin(dlat / 2) ** 2 +
             np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(dlon / 2) ** 2)
        c = 2 * np.arcsin(np.sqrt(a))

        return R * c

    # Parse query point
    query_lon, query_lat = parse_point(query_point)
    if query_lon is None:
        return set()

    expected_ids = set()

    for item in base_data:
        # Parse base point
        base_lon, base_lat = parse_point(item[geo_field_name])
        if base_lon is None:
            continue

        # Calculate distance
        actual_distance = haversine_distance(query_lat, query_lon, base_lat, base_lon)

        # Check if within threshold
        if actual_distance <= distance_meters:
            expected_ids.add(item[pk_field_name])

    return expected_ids


def generate_gt(
    spatial_func, base_data, query_geom, geo_field_name="geo", pk_field_name="id"
):
    """
    Generate ground truth (expected IDs) using shapely

    Args:
        spatial_func: The spatial function name (e.g., "ST_INTERSECTS", "ST_CONTAINS")
        base_data: List of base geometry data with format [{pk_field_name: int, geo_field_name: "WKT_STRING"}, ...]
        query_geom: WKT string of the query geometry
        geo_field_name: Name of the geometry field in base_data (default: "geo")
        pk_field_name: Name of the primary key field in base_data (default: "id")

    Returns:
        expected_ids: List of primary key values that should match the spatial function
    """
    from shapely import wkt
    import shapely

    # Spatial function mapping
    spatial_function_mapping = {
        "ST_EQUALS": shapely.equals,
        "ST_TOUCHES": shapely.touches,
        "ST_OVERLAPS": shapely.overlaps,
        "ST_CROSSES": shapely.crosses,
        "ST_CONTAINS": shapely.contains,
        "ST_INTERSECTS": shapely.intersects,
        "ST_WITHIN": shapely.within,
    }

    if spatial_func not in spatial_function_mapping:
        print(
            f"Warning: Unsupported spatial function {spatial_func}, returning empty expected_ids"
        )
        return []

    try:
        import numpy as np

        # Parse query geometry
        query_geometry = wkt.loads(query_geom)
        shapely_func = spatial_function_mapping[spatial_func]

        # Parse all base geometries
        base_geometries = []
        base_ids = []
        for item in base_data:
            try:
                base_geometry = wkt.loads(item[geo_field_name])
                base_geometries.append(base_geometry)
                base_ids.append(item[pk_field_name])
            except Exception as e:
                print(f"Warning: Failed to parse geometry {item[geo_field_name]}: {e}")
                continue

        if not base_geometries:
            return []

        # Convert to numpy array for vectorized operation
        base_geoms_array = np.array(base_geometries)
        base_ids_array = np.array(base_ids)

        # Apply vectorized spatial function
        results = shapely_func(base_geoms_array, query_geometry)

        # Get matching IDs
        expected_ids = base_ids_array[results].tolist()

        return expected_ids

    except Exception as e:
        print(f"Warning: Failed to compute ground truth for {spatial_func}: {e}")
        return []


class TestMilvusClientGeometryBasic(TestMilvusClientV2Base):
    """Test case of geometry operations"""

    def test_create_collection_with_geometry_field(self):
        """
        target: test create collection with geometry field
        method: create collection with geometry field using schema
        expected: create collection successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create schema with geometry field
        schema, _ = self.create_schema(client,
            auto_id=False, description="test geometry collection"
        )
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("geo", DataType.GEOMETRY)

        # Create collection
        self.create_collection(client, collection_name, schema=schema)

        # Verify collection has geometry field
        collections, _ = self.list_collections(client)
        assert collection_name in collections

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "wkt_type",
        [
            "POINT",
            "LINESTRING",
            "POLYGON",
            "MULTIPOINT",
            "MULTILINESTRING",
            "MULTIPOLYGON",
            "GEOMETRYCOLLECTION",
        ],
    )
    def test_insert_wkt_data(self, wkt_type):
        """
        target: test insert various WKT geometry types
        method: dynamically generate and insert different WKT geometry data
        expected: insert successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create collection with geometry field
        schema, _ = self.create_schema(client,
            auto_id=False, description=f"test {wkt_type} data"
        )
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("geo", DataType.GEOMETRY)

        self.create_collection(client, collection_name, schema=schema)

        # Generate WKT data based on type
        bounds = (
            0,
            1000,
            0,
            1000,
        )  # Expanded coordinate bounds for better distribution
        base_count = 3000  # 3000+ records for comprehensive testing
        wkt_data = generate_wkt_by_type(wkt_type, bounds=bounds, count=base_count)

        # Prepare data with generated WKT examples
        data = []
        for i, wkt in enumerate(wkt_data):
            data.append(
                {
                    "id": i,
                    "vector": [random.random() for _ in range(default_dim)],
                    "geo": wkt,
                }
            )

        # Insert data
        self.insert(client, collection_name, data)

        # Flush to ensure data is persisted
        self.flush(client,collection_name)

        # Create index before loading
        index_params, _ = self.prepare_index_params(client)
        index_params.add_index(
            field_name="vector", index_type="IVF_FLAT", metric_type="L2", nlist=128
        )
        self.create_index(client,collection_name, index_params=index_params)

        # Verify insert
        self.load_collection(client,collection_name)

        # Use query to verify data insertion
        results, _ = self.query(client,
            collection_name=collection_name,
            filter="id >= 0",
            output_fields=["id", "geo"],
        )
        assert len(results) == len(wkt_data)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "empty_wkt",
        [   
            "POINT EMPTY",
            "LINESTRING EMPTY",
            "POLYGON EMPTY",
            "MULTIPOINT EMPTY",
            "MULTILINESTRING EMPTY",
            "MULTIPOLYGON EMPTY",
            "GEOMETRYCOLLECTION EMPTY",
        ],
    )
    def test_insert_valid_empty_geometry(self, empty_wkt):
        """
        target: test inserting valid empty geometry formats according to WKT standard
        method: insert valid empty geometry WKT that should be accepted per OGC standard
        expected: should insert successfully as these are valid WKT representations
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create collection
        schema, _ = self.create_schema(client,
            auto_id=False, description="test valid empty geometry"
        )
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("geo", DataType.GEOMETRY)

        self.create_collection(client, collection_name, schema=schema)

        # Test valid empty geometry WKT formats
        data = [
            {
                "id": 0,
                "vector": [random.random() for _ in range(default_dim)],
                "geo": empty_wkt,
            }
        ]

        self.insert(client, collection_name, data)
        self.flush(client,collection_name)

        # Build indexes and load collection before querying
        index_params, _ = self.prepare_index_params(client)
        index_params.add_index(
            field_name="vector", index_type="IVF_FLAT", metric_type="L2", nlist=128
        )
        index_params.add_index(field_name="geo", index_type="RTREE")

        self.create_index(client,collection_name, index_params=index_params)
        self.load_collection(client,collection_name)

        # Verify we can query the empty geometry
        results, _ = self.query(client,
            collection_name=collection_name,
            filter="id == 0",
            output_fields=["id", "geo"],
        )
        assert len(results) == 1, f"Should be able to query {empty_wkt}"
        if empty_wkt != "POINT EMPTY":
            assert results[0]["geo"] == empty_wkt, (
                f"Retrieved geometry should match inserted {empty_wkt}"
            )
        else:
            assert results[0]["geo"] == "POINT (NaN NaN)", (
                f"Retrieved geometry should match inserted {empty_wkt}"
            )

    @pytest.mark.tags(CaseLabel.L1)
    def test_build_rtree_index(self):
        """
        target: test build RTREE index on geometry field
        method: create RTREE index on geometry field
        expected: build index successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create collection with geometry field
        schema, _ = self.create_schema(client,auto_id=False, description="test rtree index")
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("geo", DataType.GEOMETRY)

        self.create_collection(client, collection_name, schema=schema)

        # Insert diverse geometry data using various types
        geometry_types = [
            "POINT",
            "LINESTRING",
            "POLYGON",
            "MULTIPOINT",
            "MULTILINESTRING",
            "MULTIPOLYGON",
        ]
        data = []

        # Generate data for each geometry type
        for i, geom_type in enumerate(geometry_types):
            # Generate multiple geometries of each type
            geometries = generate_wkt_by_type(
                geom_type, bounds=(0, 100, 0, 100), count=15
            )
            for j, wkt in enumerate(geometries):
                data.append(
                    {
                        "id": i * 15 + j,
                        "vector": [random.random() for _ in range(default_dim)],
                        "geo": wkt,
                    }
                )

        # Add some additional mixed geometry collections
        geometry_collections = generate_wkt_by_type(
            "GEOMETRYCOLLECTION", bounds=(0, 100, 0, 100), count=10
        )
        for j, wkt in enumerate(geometry_collections):
            data.append(
                {
                    "id": len(data) + j,
                    "vector": [random.random() for _ in range(default_dim)],
                    "geo": wkt,
                }
            )

        self.insert(client, collection_name, data)
        import pandas as pd
        df = pd.DataFrame(data)
        print(f"Data: {data}")
        import os
        os.makedirs("/tmp/ci_logs/test", exist_ok=True)
        df.to_csv("/tmp/ci_logs/test/test_build_rtree_index.csv", index=False)

        # Prepare index params
        index_params, _ = self.prepare_index_params(client)
        index_params.add_index(
            field_name="vector", index_type="IVF_FLAT", metric_type="L2", nlist=128
        )
        index_params.add_index(field_name="geo", index_type="RTREE")

        # Create index
        self.create_index(client,collection_name, index_params=index_params)

        # Load collection
        self.load_collection(client,collection_name)

        # Verify index creation
        indexes, _ = self.list_indexes(client,collection_name)
        geo_index_found = any("geo" in str(idx) for idx in indexes)
        assert geo_index_found, "RTREE index on geometry field not found"

        # Verify inserted geometry data can be queried
        results, _ = self.query(client,
            collection_name=collection_name,
            filter="id >= 0",
            output_fields=["id", "geo"],
        )

        # Verify all inserted data is queryable
        expected_count = len(data)
        assert len(results) == expected_count, (
            f"Expected {expected_count} records, got {len(results)}"
        )

        # Verify geometry data integrity by checking some sample records
        result_ids = {r["id"] for r in results}
        expected_ids = {d["id"] for d in data}
        assert result_ids == expected_ids, "Query results don't match inserted data IDs"

        # Test spatial query with ST_WITHIN to verify RTREE index functionality
        # Create a large polygon that contains all geometries
        query_polygon = "POLYGON ((-10 -10, 110 -10, 110 110, -10 110, -10 -10))"
        spatial_results, _ = self.query(client,
            collection_name=collection_name,
            filter=f"ST_WITHIN(geo, '{query_polygon}')",
            output_fields=["id", "geo"],
        )

        # Should return all results since all geometries are within the expanded bounds
        assert len(spatial_results) == expected_count, (
            f"ST_WITHIN query should return all {expected_count} records, got {len(spatial_results)}"
        )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "spatial_func",
        [
            "ST_INTERSECTS",
            "ST_CONTAINS",
            "ST_WITHIN",
            "ST_EQUALS",
            "ST_TOUCHES",
            "ST_OVERLAPS",
            "ST_CROSSES",
        ],
    )
    @pytest.mark.parametrize("with_geo_index", [True, False])
    def test_spatial_query_operators_correctness(self, spatial_func, with_geo_index):
        """
        target: test various spatial query operators
        method: query geometry data using different spatial operators
        expected: return correct results based on spatial relationships
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Generate test data dynamically
        base_data = generate_diverse_base_data(
            count=3000,
            bounds=(0, 100, 0, 100),
            pk_field_name="id",
            geo_field_name="geo",
        )

        query_geom = generate_spatial_query_data_for_function(
            spatial_func, base_data, "geo"
        )
        expected_ids = generate_gt(spatial_func, base_data, query_geom, "geo", "id")

        print(
            f"Generated query for {spatial_func}: {len(expected_ids)} expected matches"
        )
        assert len(expected_ids) >= 1, (
            f"{spatial_func} query should return at least 1 result, got {len(expected_ids)}"
        )

        # Create collection
        schema, _ = self.create_schema(client,
            auto_id=False, description=f"test {spatial_func} query"
        )
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("geo", DataType.GEOMETRY)

        self.create_collection(client, collection_name, schema=schema)

        # Prepare data with vectors
        data = []
        for item in base_data:
            data.append(
                {
                    "id": item["id"],
                    "vector": [random.random() for _ in range(default_dim)],
                    "geo": item["geo"],
                }
            )

        self.insert(client, collection_name, data)
        self.flush(client,collection_name)

        # Build index based on parameter
        index_params, _ = self.prepare_index_params(client)
        index_params.add_index(
            field_name="vector", index_type="IVF_FLAT", metric_type="L2", nlist=128
        )
        if with_geo_index:
            index_params.add_index(field_name="geo", index_type="RTREE")

        self.create_index(client,collection_name, index_params=index_params)
        self.load_collection(client,collection_name)

        # Query with spatial operator
        filter_expr = f"{spatial_func}(geo, '{query_geom}')"

        results, _ = self.query(client,
            collection_name=collection_name,
            filter=filter_expr,
            output_fields=["id", "geo"],
        )

        # Verify results
        result_ids = {r["id"] for r in results}
        expected_ids_set = set(expected_ids)

        assert result_ids == expected_ids_set, (
            f"{spatial_func} query should return IDs {expected_ids}, got {list(result_ids)}"
        )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "spatial_func", ["ST_INTERSECTS", "ST_CONTAINS", "ST_WITHIN"]
    )
    def test_search_with_geo_filter(self, spatial_func):
        """
        target: test search with geo spatial filter
        method: search vectors with geo filter expressions
        expected: return correct results based on both vector similarity and spatial relationships
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Generate test data
        base_data = generate_diverse_base_data(
            count=1000,
            bounds=(0, 100, 0, 100),
            pk_field_name="id",
            geo_field_name="geo",
        )
        query_geom = generate_spatial_query_data_for_function(
            spatial_func, base_data, "geo"
        )
        expected_ids = generate_gt(spatial_func, base_data, query_geom, "geo", "id")

        print(
            f"Generated search filter for {spatial_func}: {len(expected_ids)} expected matches"
        )
        assert len(expected_ids) >= 1, (
            f"{spatial_func} filter should match at least 1 result"
        )

        # Create collection
        schema, _ = self.create_schema(client,
            auto_id=False, description=f"test search with {spatial_func} geo filter"
        )
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("geo", DataType.GEOMETRY)

        self.create_collection(client, collection_name, schema=schema)

        # Prepare data with vectors
        data = []
        for item in base_data:
            data.append(
                {
                    "id": item["id"],
                    "vector": [random.random() for _ in range(default_dim)],
                    "geo": item["geo"],
                }
            )

        self.insert(client, collection_name, data)
        self.flush(client,collection_name)

        # Build index
        index_params, _ = self.prepare_index_params(client)
        index_params.add_index(
            field_name="vector", index_type="IVF_FLAT", metric_type="L2", nlist=128
        )
        index_params.add_index(field_name="geo", index_type="RTREE")

        self.create_index(client,collection_name, index_params=index_params)
        self.load_collection(client,collection_name)

        # Search with geo filter
        search_params = {"metric_type": "L2", "params": {"nprobe": 10}}
        query_vector = [random.random() for _ in range(default_dim)]
        filter_expr = f"{spatial_func}(geo, '{query_geom}')"

        results, _ = self.search(client,
            collection_name=collection_name,
            data=[query_vector],
            anns_field="vector",
            search_params=search_params,
            limit=len(expected_ids),
            filter=filter_expr,
            output_fields=["id", "geo"],
        )

        # Verify results
        search_result_ids = {hit["id"] for hit in results[0]}
        expected_ids_set = set(expected_ids)

        assert search_result_ids.issubset(expected_ids_set), (
            f"Search results should be subset of expected IDs for {spatial_func}"
        )
        assert len(search_result_ids) > 0, (
            f"Search with {spatial_func} filter should return at least 1 result"
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_collection_with_multiple_geometry_fields(self):
        """
        target: test create collection with multiple geometry fields
        method: create collection with 2+ geometry fields and test cross-field queries
        expected: create collection successfully and queries work correctly
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create schema with multiple geometry fields
        schema, _ = self.create_schema(client,
            auto_id=False, description="test multiple geometry fields"
        )
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("geo1", DataType.GEOMETRY)
        schema.add_field("geo2", DataType.GEOMETRY)

        self.create_collection(client, collection_name, schema=schema)

        # Generate test data with different geometry types for each field
        data = []
        for i in range(100):
            # geo1: points
            point_x = random.uniform(0, 100)
            point_y = random.uniform(0, 100)
            geo1_wkt = f"POINT ({point_x:.2f} {point_y:.2f})"

            # geo2: polygons around the points
            size = 10
            geo2_wkt = f"POLYGON (({point_x - size:.2f} {point_y - size:.2f}, {point_x + size:.2f} {point_y - size:.2f}, {point_x + size:.2f} {point_y + size:.2f}, {point_x - size:.2f} {point_y + size:.2f}, {point_x - size:.2f} {point_y - size:.2f}))"

            data.append(
                {
                    "id": i,
                    "vector": [random.random() for _ in range(default_dim)],
                    "geo1": geo1_wkt,
                    "geo2": geo2_wkt,
                }
            )

        self.insert(client, collection_name, data)
        self.flush(client,collection_name)

        # Build indexes
        index_params, _ = self.prepare_index_params(client)
        index_params.add_index(
            field_name="vector", index_type="IVF_FLAT", metric_type="L2", nlist=128
        )
        index_params.add_index(field_name="geo1", index_type="RTREE")
        index_params.add_index(field_name="geo2", index_type="RTREE")

        self.create_index(client,collection_name, index_params=index_params)
        self.load_collection(client,collection_name)

        # Test query on specific geometry field
        query_polygon = "POLYGON ((-10 -10, 110 -10, 110 110, -10 110, -10 -10))"
        results_geo1, _ = self.query(client,
            collection_name=collection_name,
            filter=f"ST_WITHIN(geo1, '{query_polygon}')",
            output_fields=["id", "geo1"],
        )

        results_geo2, _ = self.query(client,
            collection_name=collection_name,
            filter=f"ST_INTERSECTS(geo2, '{query_polygon}')",
            output_fields=["id", "geo2"],
        )

        assert len(results_geo1) > 0, "Should find points within query polygon"
        assert len(results_geo2) > 0, (
            "Should find polygons intersecting with query polygon"
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_collection_with_nullable_geometry_field(self):
        """
        target: test create collection with nullable geometry field
        method: create collection with nullable geometry field and insert data with NULL values
        expected: create collection successfully and handle NULL values correctly
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create schema with nullable geometry field
        schema, _ = self.create_schema(client,
            auto_id=False, description="test nullable geometry field"
        )
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("geo", DataType.GEOMETRY, nullable=True)

        self.create_collection(client, collection_name, schema=schema)

        # Generate test data with mix of valid geometries and NULL values
        data = []
        for i in range(100):
            if i % 3 == 0:  # Every 3rd record has NULL geometry
                geo_value = None
            else:
                # Generate valid geometry
                x = random.uniform(0, 100)
                y = random.uniform(0, 100)
                geo_value = f"POINT ({x:.2f} {y:.2f})"

            data.append(
                {
                    "id": i,
                    "vector": [random.random() for _ in range(default_dim)],
                    "geo": geo_value,
                }
            )

        self.insert(client, collection_name, data)
        self.flush(client,collection_name)

        # Build indexes
        index_params, _ = self.prepare_index_params(client)
        index_params.add_index(
            field_name="vector", index_type="IVF_FLAT", metric_type="L2", nlist=128
        )
        index_params.add_index(field_name="geo", index_type="RTREE")

        self.create_index(client,collection_name, index_params=index_params)
        self.load_collection(client,collection_name)

        # Test query excluding NULL values
        results_non_null, _ = self.query(client,
            collection_name=collection_name,
            filter="geo IS NOT NULL",
            output_fields=["id", "geo"],
        )

        expected_non_null_count = len([d for d in data if d["geo"] is not None])
        assert len(results_non_null) == expected_non_null_count, (
            f"Expected {expected_non_null_count} non-null geometries, got {len(results_non_null)}"
        )

        # Test query including NULL values
        results_null, _ = self.query(client,
            collection_name=collection_name,
            filter="geo IS NULL",
            output_fields=["id", "geo"],
        )

        expected_null_count = len([d for d in data if d["geo"] is None])
        assert len(results_null) == expected_null_count, (
            f"Expected {expected_null_count} null geometries, got {len(results_null)}"
        )

        # Test spatial query (should only return non-null geometries)
        query_polygon = "POLYGON ((40 40, 60 40, 60 60, 40 60, 40 40))"
        spatial_results, _ = self.query(client,
            collection_name=collection_name,
            filter=f"ST_WITHIN(geo, '{query_polygon}')",
            output_fields=["id", "geo"],
        )

        # Spatial query should not include NULL values
        for result in spatial_results:
            assert result["geo"] is not None, (
                "Spatial query should not return NULL geometry values"
            )

        # Test search with filter excluding nulls
        search_params = {"metric_type": "L2", "params": {"nprobe": 10}}
        query_vector = [random.random() for _ in range(default_dim)]

        search_results, _ = self.search(client,
            collection_name=collection_name,
            data=[query_vector],
            anns_field="vector",
            search_params=search_params,
            limit=10,
            filter="geo IS NOT NULL",
            output_fields=["id", "geo"],
        )

        # All search results should have non-null geometry
        for hit in search_results[0]:
            assert hit["geo"] is not None, (
                "Search results should not include NULL geometry when filtered"
            )

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_collection_with_geometry_default_value(self):
        """
        target: test create collection with geometry field having default value
        method: create collection with geometry field with default value and insert data
        expected: create collection successfully and handle default values correctly
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create schema with geometry field having default value
        schema, _ = self.create_schema(client,
            auto_id=False, description="test geometry field with default value"
        )
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("geo", DataType.GEOMETRY, default_value="POINT (0 0)")

        self.create_collection(client, collection_name, schema=schema)

        # Generate test data - some with explicit geometry, some without (to use default)
        data = []
        for i in range(100):
            if i % 3 == 0:  # Every 3rd record uses default geometry (omit geo field)
                data.append(
                    {
                        "id": i,
                        "vector": [random.random() for _ in range(default_dim)],
                        # geo field omitted - should use default value
                    }
                )
            elif i % 3 == 1:  # Every 3rd record sets geo to None (should use default)
                data.append(
                    {
                        "id": i,
                        "vector": [random.random() for _ in range(default_dim)],
                        "geo": None,  # explicitly set to None - should use default value
                    }
                )
            else:  # Remaining records have explicit geometry values
                x = random.uniform(10, 90)
                y = random.uniform(10, 90)
                data.append(
                    {
                        "id": i,
                        "vector": [random.random() for _ in range(default_dim)],
                        "geo": f"POINT ({x:.2f} {y:.2f})",
                    }
                )

        self.insert(client, collection_name, data)
        self.flush(client,collection_name)

        # Build indexes
        index_params, _ = self.prepare_index_params(client)
        index_params.add_index(
            field_name="vector", index_type="IVF_FLAT", metric_type="L2", nlist=128
        )
        index_params.add_index(field_name="geo", index_type="RTREE")

        self.create_index(client,collection_name, index_params=index_params)
        self.load_collection(client,collection_name)

        # Verify all data was inserted
        all_results, _ = self.query(client,
            collection_name=collection_name,
            filter="id >= 0",
            output_fields=["id", "geo"],
        )
        assert len(all_results) == 100, f"Expected 100 records, got {len(all_results)}"

        # Check that records using default value have the expected geometry
        default_results, _ = self.query(client,
            collection_name=collection_name,
            filter="ST_EQUALS(geo, 'POINT (0 0)')",
            output_fields=["id", "geo"],
        )

        # Should have records where default value was used
        expected_default_count = len(
            [d for i, d in enumerate(data) if i % 3 == 0 or i % 3 == 1]
        )
        assert len(default_results) == expected_default_count, (
            f"Expected {expected_default_count} records with default geometry, got {len(default_results)}"
        )

        # Verify default geometry values
        for result in default_results:
            assert result["geo"] == "POINT (0 0)", (
                f"Default geometry should be 'POINT (0 0)', got {result['geo']}"
            )

        # Test spatial query to find non-default geometries
        non_default_results, _ = self.query(client,
            collection_name=collection_name,
            filter="NOT ST_EQUALS(geo, 'POINT (0 0)')",
            output_fields=["id", "geo"],
        )

        expected_non_default_count = len([d for i, d in enumerate(data) if i % 3 == 2])
        assert len(non_default_results) == expected_non_default_count, (
            f"Expected {expected_non_default_count} records with non-default geometry, got {len(non_default_results)}"
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_collection_with_nullable_geometry_default_value(self):
        """
        target: test create collection with nullable geometry field having default value
        method: create collection with nullable geometry field with default value and test behavior
        expected: create collection successfully and handle nullable + default values correctly
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create schema with nullable geometry field having default value
        schema, _ = self.create_schema(client,
            auto_id=False, description="test nullable geometry field with default value"
        )
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(
            "geo", DataType.GEOMETRY, nullable=True, default_value="POINT (50 50)"
        )

        self.create_collection(client, collection_name, schema=schema)

        # Generate test data with different scenarios
        data = []
        for i in range(120):
            if i % 4 == 0:  # Omit geo field - should use default value
                data.append(
                    {"id": i, "vector": [random.random() for _ in range(default_dim)]}
                )
            elif i % 4 == 1:  # Set geo to None - should use default value
                data.append(
                    {
                        "id": i,
                        "vector": [random.random() for _ in range(default_dim)],
                        "geo": None,
                    }
                )
            elif i % 4 == 2:  # Explicit geometry value
                x = random.uniform(0, 100)
                y = random.uniform(0, 100)
                data.append(
                    {
                        "id": i,
                        "vector": [random.random() for _ in range(default_dim)],
                        "geo": f"POINT ({x:.2f} {y:.2f})",
                    }
                )
            else:  # Different geometry type with explicit value
                data.append(
                    {
                        "id": i,
                        "vector": [random.random() for _ in range(default_dim)],
                        "geo": "POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))",
                    }
                )

        self.insert(client, collection_name, data)
        self.flush(client,collection_name)

        # Build indexes
        index_params, _ = self.prepare_index_params(client)
        index_params.add_index(
            field_name="vector", index_type="IVF_FLAT", metric_type="L2", nlist=128
        )
        index_params.add_index(field_name="geo", index_type="RTREE")

        self.create_index(client,collection_name, index_params=index_params)
        self.load_collection(client,collection_name)

        # Verify all data was inserted
        all_results, _ = self.query(client,
            collection_name=collection_name,
            filter="id >= 0",
            output_fields=["id", "geo"],
        )
        assert len(all_results) == 120, f"Expected 120 records, got {len(all_results)}"

        # Check records using default value
        default_results, _ = self.query(client,
            collection_name=collection_name,
            filter="ST_EQUALS(geo, 'POINT (50 50)')",
            output_fields=["id", "geo"],
        )

        # Should have records where default value was used (i % 4 == 0 or i % 4 == 1)
        expected_default_count = len(
            [d for i, d in enumerate(data) if i % 4 == 0 or i % 4 == 1]
        )
        assert len(default_results) == expected_default_count, (
            f"Expected {expected_default_count} records with default geometry, got {len(default_results)}"
        )

        # Verify default geometry values
        for result in default_results:
            assert result["geo"] == "POINT (50 50)", (
                f"Default geometry should be 'POINT (50 50)', got {result['geo']}"
            )

        # Test that no records have NULL values (since default is used when None is provided)
        null_results, _ = self.query(client,
            collection_name=collection_name,
            filter="geo IS NULL",
            output_fields=["id", "geo"],
        )
        assert len(null_results) == 0, (
            "Should have no NULL geometry values when default is provided"
        )

        # Check non-default geometries
        non_default_results, _ = self.query(client,
            collection_name=collection_name,
            filter="NOT ST_EQUALS(geo, 'POINT (50 50)')",
            output_fields=["id", "geo"],
        )

        # Should include both explicit points (i % 4 == 2) and polygons (i % 4 == 3)
        expected_non_default_count = len(
            [d for i, d in enumerate(data) if i % 4 == 2 or i % 4 == 3]
        )
        assert len(non_default_results) == expected_non_default_count, (
            f"Expected {expected_non_default_count} non-default geometries"
        )

        # Check polygon geometries specifically
        polygon_results, _ = self.query(client,
            collection_name=collection_name,
            filter="ST_EQUALS(geo, 'POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))')",
            output_fields=["id", "geo"],
        )

        expected_polygon_count = len([d for i, d in enumerate(data) if i % 4 == 3])
        assert len(polygon_results) == expected_polygon_count, (
            f"Expected {expected_polygon_count} polygon geometries"
        )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "default_geom_type,default_wkt",
        [
            ("POINT", "POINT (100 100)"),
            ("LINESTRING", "LINESTRING (0 0, 1 1, 2 2)"),
            ("POLYGON", "POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))"),
            ("MULTIPOINT", "MULTIPOINT ((0 0), (1 1), (2 2))"),
            ("MULTILINESTRING", "MULTILINESTRING ((0 0, 1 1), (2 2, 3 3))"),
            (
                "MULTIPOLYGON",
                "MULTIPOLYGON (((0 0, 2 0, 2 2, 0 2, 0 0)), ((3 3, 5 3, 5 5, 3 5, 3 3)))",
            ),
            (
                "GEOMETRYCOLLECTION",
                "GEOMETRYCOLLECTION (POINT (1 1), LINESTRING (2 2, 3 3))",
            ),
        ],
    )
    def test_geometry_default_value_mixed_types(self, default_geom_type, default_wkt):
        """
        target: test geometry fields with different default value types
        method: create collections with different geometry types as default values
        expected: handle various geometry types as default values correctly
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create schema with specific geometry type as default value
        schema, _ = self.create_schema(client,
            auto_id=False, description=f"test {default_geom_type} as default geometry"
        )
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("geo", DataType.GEOMETRY, default_value=default_wkt)

        self.create_collection(client, collection_name, schema=schema)

        # Generate test data
        data = []
        for i in range(60):
            if i % 2 == 0:  # Use default value (omit geo field)
                data.append(
                    {"id": i, "vector": [random.random() for _ in range(default_dim)]}
                )
            else:  # Provide explicit value (different from default)
                if default_geom_type == "POINT":
                    explicit_wkt = f"POINT ({random.uniform(200, 300):.2f} {random.uniform(200, 300):.2f})"
                elif default_geom_type == "LINESTRING":
                    explicit_wkt = "LINESTRING (10 10, 20 20, 30 30)"
                elif default_geom_type == "POLYGON":
                    explicit_wkt = "POLYGON ((10 10, 15 10, 15 15, 10 15, 10 10))"
                elif default_geom_type == "MULTIPOINT":
                    explicit_wkt = "MULTIPOINT ((10 10), (11 11), (12 12))"
                elif default_geom_type == "MULTILINESTRING":
                    explicit_wkt = "MULTILINESTRING ((10 10, 11 11), (12 12, 13 13))"
                elif default_geom_type == "MULTIPOLYGON":
                    explicit_wkt = "MULTIPOLYGON (((10 10, 12 10, 12 12, 10 12, 10 10)), ((13 13, 15 13, 15 15, 13 15, 13 13)))"
                else:  # GEOMETRYCOLLECTION
                    explicit_wkt = (
                        "GEOMETRYCOLLECTION (POINT (10 10), LINESTRING (12 12, 13 13))"
                    )

                data.append(
                    {
                        "id": i,
                        "vector": [random.random() for _ in range(default_dim)],
                        "geo": explicit_wkt,
                    }
                )

        self.insert(client, collection_name, data)
        self.flush(client,collection_name)

        # Build indexes
        index_params, _ = self.prepare_index_params(client)
        index_params.add_index(
            field_name="vector", index_type="IVF_FLAT", metric_type="L2", nlist=128
        )
        index_params.add_index(field_name="geo", index_type="RTREE")

        self.create_index(client,collection_name, index_params=index_params)
        self.load_collection(client,collection_name)

        # Verify all data was inserted
        all_results, _ = self.query(client,
            collection_name=collection_name,
            filter="id >= 0",
            output_fields=["id", "geo"],
        )
        assert len(all_results) == 60, f"Expected 60 records, got {len(all_results)}"

        # Check records using default value
        default_results, _ = self.query(client,
            collection_name=collection_name,
            filter=f"ST_EQUALS(geo, '{default_wkt}')",
            output_fields=["id", "geo"],
        )

        expected_default_count = 30  # Half the records use default (i % 2 == 0)
        assert len(default_results) == expected_default_count, (
            f"Expected {expected_default_count} records with default {default_geom_type} geometry, got {len(default_results)}"
        )

        # Verify default geometry values
        for result in default_results:
            # Handle both MULTIPOINT formats: ((x y), (x y)) and (x y, x y)
            if default_geom_type == "MULTIPOINT":
                # Normalize both formats to the same representation for comparison
                def normalize_multipoint(wkt):
                    if wkt.startswith("MULTIPOINT"):
                        # Remove MULTIPOINT prefix and normalize parentheses
                        coords = wkt[len("MULTIPOINT") :].strip()
                        # Handle both ((x y), (x y)) and (x y, x y) formats
                        coords = coords.replace("((", "(").replace("))", ")")
                        coords = coords.replace("), (", ", ")
                        return f"MULTIPOINT {coords}"
                    return wkt

                normalized_result = normalize_multipoint(result["geo"])
                normalized_expected = normalize_multipoint(default_wkt)
                assert normalized_result == normalized_expected, (
                    f"Default geometry should be '{default_wkt}', got {result['geo']}"
                )
            else:
                assert result["geo"] == default_wkt, (
                    f"Default geometry should be '{default_wkt}', got {result['geo']}"
                )

        # Test spatial queries work with default geometry types
        if default_geom_type in ["POINT", "LINESTRING", "POLYGON"]:
            # Create a large containing polygon to test spatial relationships
            large_polygon = (
                "POLYGON ((-500 -500, 500 -500, 500 500, -500 500, -500 -500))"
            )
            spatial_results, _ = self.query(client,
                collection_name=collection_name,
                filter=f"ST_WITHIN(geo, '{large_polygon}')",
                output_fields=["id", "geo"],
            )

            # Should find geometries within the large polygon
            assert len(spatial_results) > 0, (
                f"Should find geometries within large polygon for {default_geom_type}"
            )

        # Check non-default geometries
        non_default_results, _ = self.query(client,
            collection_name=collection_name,
            filter=f"NOT ST_EQUALS(geo, '{default_wkt}')",
            output_fields=["id", "geo"],
        )

        expected_non_default_count = (
            30  # Half the records have explicit values (i % 2 == 1)
        )
        assert len(non_default_results) == expected_non_default_count, (
            f"Expected {expected_non_default_count} records with non-default geometry, got {len(non_default_results)}"
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_and_query_with_geometry_default_values(self):
        """
        target: test search and query operations with geometry default values
        method: create collection with geometry default values, perform search and query operations
        expected: search and query work correctly with default geometry values
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create schema with geometry field having default value
        schema, _ = self.create_schema(client,
            auto_id=False,
            description="test search and query with geometry default values",
        )
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("geo", DataType.GEOMETRY, default_value="POINT (25 25)")
        schema.add_field("category", DataType.VARCHAR, max_length=50)

        self.create_collection(client, collection_name, schema=schema)

        # Generate test data with mix of default and explicit geometries
        data = []
        for i in range(200):
            if i % 3 == 0:  # Use default geometry value
                data.append(
                    {
                        "id": i,
                        "vector": [random.random() for _ in range(default_dim)],
                        "category": "default_geo",
                        # geo field omitted - will use default "POINT (25 25)"
                    }
                )
            elif i % 3 == 1:  # Explicit geometry near default
                x = random.uniform(20, 30)
                y = random.uniform(20, 30)
                data.append(
                    {
                        "id": i,
                        "vector": [random.random() for _ in range(default_dim)],
                        "geo": f"POINT ({x:.2f} {y:.2f})",
                        "category": "near_default",
                    }
                )
            else:  # Explicit geometry far from default
                x = random.uniform(80, 90)
                y = random.uniform(80, 90)
                data.append(
                    {
                        "id": i,
                        "vector": [random.random() for _ in range(default_dim)],
                        "geo": f"POINT ({x:.2f} {y:.2f})",
                        "category": "far_from_default",
                    }
                )

        self.insert(client, collection_name, data)
        self.flush(client,collection_name)

        # Build indexes
        index_params, _ = self.prepare_index_params(client)
        index_params.add_index(
            field_name="vector", index_type="IVF_FLAT", metric_type="L2", nlist=128
        )
        index_params.add_index(field_name="geo", index_type="RTREE")

        self.create_index(client,collection_name, index_params=index_params)
        self.load_collection(client,collection_name)

        # Query for records with default geometry value
        default_geo_query, _ = self.query(client,
            collection_name=collection_name,
            filter="ST_EQUALS(geo, 'POINT (25 25)')",
            output_fields=["id", "geo", "category"],
        )

        expected_default_count = len([d for i, d in enumerate(data) if i % 3 == 0])
        assert len(default_geo_query) == expected_default_count, (
            f"Expected {expected_default_count} records with default geometry"
        )

        # Verify all returned records have default geometry and category
        for result in default_geo_query:
            assert result["geo"] == "POINT (25 25)", (
                "Should return records with default geometry"
            )
            assert result["category"] == "default_geo", (
                "Should return records with default_geo category"
            )

        # Spatial query that includes default geometry area
        near_default_area = "POLYGON ((15 15, 35 15, 35 35, 15 35, 15 15))"
        spatial_query, _ = self.query(client,
            collection_name=collection_name,
            filter=f"ST_WITHIN(geo, '{near_default_area}')",
            output_fields=["id", "geo", "category"],
        )

        # Should include both default geometry records and near_default records
        expected_categories = {"default_geo", "near_default"}
        found_categories = {result["category"] for result in spatial_query}
        assert expected_categories.issubset(found_categories), (
            "Should find both default_geo and near_default categories"
        )
        assert len(spatial_query) > expected_default_count, (
            "Should find more than just default geometry records"
        )

        # Search with geometry filter including default values
        search_params = {"metric_type": "L2", "params": {"nprobe": 10}}
        query_vector = [random.random() for _ in range(default_dim)]

        search_with_geo_filter, _ = self.search(client,
            collection_name=collection_name,
            data=[query_vector],
            anns_field="vector",
            search_params=search_params,
            limit=50,
            filter=f"ST_WITHIN(geo, '{near_default_area}')",
            output_fields=["id", "geo", "category"],
        )

        # Verify search results include records with default geometry
        search_categories = {hit["category"] for hit in search_with_geo_filter[0]}
        assert "default_geo" in search_categories, (
            "Search results should include records with default geometry"
        )
        assert len(search_with_geo_filter[0]) > 0, (
            "Search with geo filter should return results"
        )

        # Range query excluding default geometry area
        far_area = "POLYGON ((75 75, 95 75, 95 95, 75 95, 75 75))"
        far_query, _ = self.query(client,
            collection_name=collection_name,
            filter=f"ST_WITHIN(geo, '{far_area}')",
            output_fields=["id", "geo", "category"],
        )

        # Should only find far_from_default records, no default geometry
        for result in far_query:
            assert result["category"] == "far_from_default", (
                "Far area query should not include default geometry records"
            )
            assert result["geo"] != "POINT (25 25)", (
                "Far area should not contain default geometry"
            )

        expected_far_count = len([d for i, d in enumerate(data) if i % 3 == 2])
        assert len(far_query) == expected_far_count, (
            f"Expected {expected_far_count} far_from_default records"
        )

        # Search excluding default geometry area
        search_far_area, _ = self.search(client,
            collection_name=collection_name,
            data=[query_vector],
            anns_field="vector",
            search_params=search_params,
            limit=30,
            filter=f"ST_WITHIN(geo, '{far_area}')",
            output_fields=["id", "geo", "category"],
        )

        # Should not include any default geometry records
        for hit in search_far_area[0]:
            assert hit["category"] == "far_from_default", (
                "Search in far area should exclude default geometry"
            )
            assert hit["geo"] != "POINT (25 25)", (
                "Search results should not contain default geometry"
            )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "spatial_func",
        [
            "ST_INTERSECTS",
            "ST_CONTAINS",
            "ST_WITHIN",
            "ST_EQUALS",
            "ST_TOUCHES",
            "ST_OVERLAPS",
            "ST_CROSSES",
        ],
    )
    def test_case_insensitive_spatial_functions(self, spatial_func):
        """
        target: test case-insensitive spatial functions
        method: test spatial functions with different case variations
        expected: all case variations should work correctly
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create collection
        schema, _ = self.create_schema(client,
            auto_id=False, description="test case insensitive spatial functions"
        )
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("geo", DataType.GEOMETRY)

        self.create_collection(client, collection_name, schema=schema)

        # Generate test data
        base_data = generate_diverse_base_data(
            count=100, bounds=(0, 100, 0, 100), pk_field_name="id", geo_field_name="geo"
        )
        query_geom = generate_spatial_query_data_for_function(
            spatial_func, base_data, "geo"
        )
        expected_ids = generate_gt(spatial_func, base_data, query_geom, "geo", "id")

        assert len(expected_ids) >= 1, (
            f"{spatial_func} query should return at least 1 result"
        )

        # Prepare data with vectors
        data = []
        for item in base_data:
            data.append(
                {
                    "id": item["id"],
                    "vector": [random.random() for _ in range(default_dim)],
                    "geo": item["geo"],
                }
            )

        self.insert(client, collection_name, data)
        self.flush(client,collection_name)

        # Build indexes
        index_params, _ = self.prepare_index_params(client)
        index_params.add_index(
            field_name="vector", index_type="IVF_FLAT", metric_type="L2", nlist=128
        )
        index_params.add_index(field_name="geo", index_type="RTREE")

        self.create_index(client,collection_name, index_params=index_params)
        self.load_collection(client,collection_name)

        # Test different case variations of the spatial function
        case_variations = [
            spatial_func.upper(),  # e.g., ST_INTERSECTS
            spatial_func.lower(),  # e.g., st_intersects
        ]

        expected_ids_set = set(expected_ids)

        for case_func in case_variations:
            filter_expr = f"{case_func}(geo, '{query_geom}')"

            results, _ = self.query(client,
                collection_name=collection_name,
                filter=filter_expr,
                output_fields=["id", "geo"],
            )

            result_ids = {r["id"] for r in results}
            assert result_ids == expected_ids_set, (
                f"Case variation '{case_func}' should return same results as '{spatial_func}'"
            )

    @pytest.mark.tags(CaseLabel.L1)
    def test_boundary_conditions_geometry_positive(self):
        """
        target: test positive boundary conditions for geometry data
        method: test valid edge cases like large coordinates, high precision
        expected: handle valid boundary conditions successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create collection
        schema, _ = self.create_schema(client,
            auto_id=False, description="test positive geometry boundary conditions"
        )
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("geo", DataType.GEOMETRY)

        self.create_collection(client, collection_name, schema=schema)

        # Test valid boundary conditions
        boundary_test_data = [
            # High precision coordinates
            {"id": 0, "geo": "POINT (1.23456789012345 9.87654321098765)"},
            # Zero coordinates
            {"id": 1, "geo": "POINT (0 0)"},
            # Negative coordinates
            {"id": 2, "geo": "POINT (-123.456 -789.012)"},
            # Large but reasonable coordinates
            {"id": 3, "geo": "POINT (1000000 1000000)"},
            # Very small polygon
            {"id": 4, "geo": "POLYGON ((0 0, 0.001 0, 0.001 0.001, 0 0.001, 0 0))"},
            # Large polygon
            {"id": 5, "geo": "POLYGON ((0 0, 1000 0, 1000 1000, 0 1000, 0 0))"},
        ]

        # Prepare data with vectors
        data = []
        for item in boundary_test_data:
            data.append(
                {
                    "id": item["id"],
                    "vector": [random.random() for _ in range(default_dim)],
                    "geo": item["geo"],
                }
            )

        # Insert boundary test data
        self.insert(client, collection_name, data)
        self.flush(client,collection_name)

        # Build indexes
        index_params, _ = self.prepare_index_params(client)
        index_params.add_index(
            field_name="vector", index_type="IVF_FLAT", metric_type="L2", nlist=128
        )
        index_params.add_index(field_name="geo", index_type="RTREE")

        self.create_index(client,collection_name, index_params=index_params)
        self.load_collection(client,collection_name)

        # Verify all boundary data was inserted correctly
        results, _ = self.query(client,
            collection_name=collection_name,
            filter="id >= 0",
            output_fields=["id", "geo"],
        )

        assert len(results) == len(boundary_test_data), (
            f"Expected {len(boundary_test_data)} boundary test records, got {len(results)}"
        )

        # Test spatial queries with boundary geometries
        # Query for zero point
        zero_results, _ = self.query(client,
            collection_name=collection_name,
            filter="ST_EQUALS(geo, 'POINT (0 0)')",
            output_fields=["id", "geo"],
        )

        assert len(zero_results) == 1 and zero_results[0]["id"] == 1, (
            "Should find zero coordinate point"
        )

        # Test precision - query for high precision point
        precision_results, _ = self.query(client,
            collection_name=collection_name,
            filter="ST_EQUALS(geo, 'POINT (1.23456789012345 9.87654321098765)')",
            output_fields=["id", "geo"],
        )

        assert len(precision_results) == 1 and precision_results[0]["id"] == 0, (
            "Should find high precision point"
        )

        # Test large coordinate range query
        large_polygon = "POLYGON ((-2000000 -2000000, 2000000 -2000000, 2000000 2000000, -2000000 2000000, -2000000 -2000000))"
        large_results, _ = self.query(client,
            collection_name=collection_name,
            filter=f"ST_WITHIN(geo, '{large_polygon}')",
            output_fields=["id", "geo"],
        )

        # Should include all points within reasonable large bounds
        assert len(large_results) >= 4, "Large polygon should contain multiple points"

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "filter_type,logical_op",
        [
            ("multi_geo", "AND"),
            ("multi_geo", "OR"),
            ("geo_int", "AND"),
            ("geo_int", "OR"),
            ("geo_varchar", "AND"),
            ("geo_varchar", "OR"),
        ],
    )
    def test_complex_filter_combinations(self, filter_type, logical_op):
        """
        target: test complex filter combinations with multiple geo functions and mixed data types
        method: parameterized test covering geo+geo, geo+int, geo+varchar filters with AND/OR operators
        expected: all complex filter combinations should work correctly
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create collection with multiple field types
        schema, _ = self.create_schema(client,
            auto_id=False,
            description=f"test complex {filter_type} filter with {logical_op}",
        )
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("geo", DataType.GEOMETRY)
        schema.add_field("age", DataType.INT64)
        schema.add_field("category", DataType.VARCHAR, max_length=50)

        self.create_collection(client, collection_name, schema=schema)

        # Generate test data with specific patterns for complex filtering
        data = []

        for i in range(150):
            # Generate geometry based on patterns
            if i % 5 == 0:  # Premium points in northeast quadrant
                x = random.uniform(60, 80)
                y = random.uniform(60, 80)
                geo_value = f"POINT ({x:.2f} {y:.2f})"
                age_value = random.randint(25, 40)
                category_value = "premium"
            elif i % 5 == 1:  # Standard points in northwest quadrant
                x = random.uniform(20, 40)
                y = random.uniform(60, 80)
                geo_value = f"POINT ({x:.2f} {y:.2f})"
                age_value = random.randint(30, 50)
                category_value = "standard"
            elif i % 5 == 2:  # Basic polygons in center
                x = random.uniform(45, 55)
                y = random.uniform(45, 55)
                size = 3
                geo_value = f"POLYGON (({x:.2f} {y:.2f}, {x + size:.2f} {y:.2f}, {x + size:.2f} {y + size:.2f}, {x:.2f} {y + size:.2f}, {x:.2f} {y:.2f}))"
                age_value = random.randint(40, 60)
                category_value = "basic"
            elif i % 5 == 3:  # Enterprise linestrings
                x1 = random.uniform(10, 30)
                y1 = random.uniform(10, 30)
                x2 = random.uniform(70, 90)
                y2 = random.uniform(70, 90)
                geo_value = f"LINESTRING ({x1:.2f} {y1:.2f}, {x2:.2f} {y2:.2f})"
                age_value = random.randint(35, 55)
                category_value = "enterprise"
            else:  # Trial points in southeast
                x = random.uniform(60, 80)
                y = random.uniform(20, 40)
                geo_value = f"POINT ({x:.2f} {y:.2f})"
                age_value = random.randint(20, 35)
                category_value = "trial"

            data.append(
                {
                    "id": i,
                    "vector": [random.random() for _ in range(default_dim)],
                    "geo": geo_value,
                    "age": age_value,
                    "category": category_value,
                }
            )

        self.insert(client, collection_name, data)
        self.flush(client,collection_name)

        # Build indexes
        index_params, _ = self.prepare_index_params(client)
        index_params.add_index(
            field_name="vector", index_type="IVF_FLAT", metric_type="L2", nlist=128
        )
        index_params.add_index(field_name="geo", index_type="RTREE")

        self.create_index(client,collection_name, index_params=index_params)
        self.load_collection(client,collection_name)

        # Define spatial regions
        northeast_region = "POLYGON ((55 55, 85 55, 85 85, 55 85, 55 55))"
        northwest_region = "POLYGON ((15 55, 45 55, 45 85, 15 85, 15 55))"
        center_region = "POLYGON ((40 40, 60 40, 60 60, 40 60, 40 40))"
        southeast_region = "POLYGON ((55 15, 85 15, 85 45, 55 45, 55 15))"
        large_region = "POLYGON ((0 0, 100 0, 100 100, 0 100, 0 0))"

        # Create filter expressions based on test type and logical operator
        if filter_type == "multi_geo":
            if logical_op == "AND":
                # Find geometries that are both within large region AND intersect with center
                filter_expr = f"ST_WITHIN(geo, '{large_region}') AND ST_INTERSECTS(geo, '{center_region}')"
                validation_queries = [
                    f"ST_WITHIN(geo, '{large_region}')",
                    f"ST_INTERSECTS(geo, '{center_region}')",
                ]
            else:  # OR
                # Find geometries that are either in northeast OR northwest regions
                filter_expr = f"ST_WITHIN(geo, '{northeast_region}') OR ST_WITHIN(geo, '{northwest_region}')"
                validation_queries = [
                    f"ST_WITHIN(geo, '{northeast_region}')",
                    f"ST_WITHIN(geo, '{northwest_region}')",
                ]

        elif filter_type == "geo_int":
            if logical_op == "AND":
                # Find geometries in northeast region AND age between 25-40
                filter_expr = (
                    f"ST_WITHIN(geo, '{northeast_region}') AND age >= 25 AND age <= 40"
                )
            else:  # OR
                # Find geometries either in center OR age > 50 OR age < 30
                filter_expr = (
                    f"ST_INTERSECTS(geo, '{center_region}') OR age > 50 OR age < 30"
                )

        else:  # geo_varchar
            if logical_op == "AND":
                # Find geometries in northeast region AND premium category
                filter_expr = (
                    f"ST_WITHIN(geo, '{northeast_region}') AND category == 'premium'"
                )
            else:  # OR
                # Find geometries either in southeast OR enterprise category OR standard category
                filter_expr = f"ST_WITHIN(geo, '{southeast_region}') OR category == 'enterprise' OR category == 'standard'"

        # Execute complex filter query
        results, _ = self.query(client,
            collection_name=collection_name,
            filter=filter_expr,
            output_fields=["id", "geo", "age", "category"],
        )

        assert len(results) > 0, (
            f"Complex {filter_type} filter with {logical_op} should find matching records"
        )

        # Validation based on filter type and logical operator
        if filter_type == "multi_geo":
            if logical_op == "AND":
                # All results should satisfy both spatial conditions
                for result in results:
                    # Check each spatial condition individually
                    for validation_query in validation_queries:
                        check_result, _ = self.query(client,
                            collection_name=collection_name,
                            filter=f"id == {result['id']} AND {validation_query}",
                            output_fields=["id"],
                        )
                        assert len(check_result) == 1, (
                            f"Record {result['id']} should satisfy: {validation_query}"
                        )
            else:  # OR
                # Results should satisfy at least one spatial condition
                individual_results = []
                for validation_query in validation_queries:
                    individual_result, _ = self.query(client,
                        collection_name=collection_name,
                        filter=validation_query,
                        output_fields=["id"],
                    )
                    individual_results.append(len(individual_result))

                expected_min = max(individual_results)
                assert len(results) >= expected_min, (
                    f"OR filter should find at least {expected_min} results"
                )

        elif filter_type == "geo_int":
            if logical_op == "AND":
                # All results should satisfy both geo and int conditions
                for result in results:
                    # Check spatial condition
                    spatial_check, _ = self.query(client,
                        collection_name=collection_name,
                        filter=f"id == {result['id']} AND ST_WITHIN(geo, '{northeast_region}')",
                        output_fields=["id"],
                    )
                    assert len(spatial_check) == 1, (
                        f"Record {result['id']} should be in northeast region"
                    )

                    # Check age condition
                    assert 25 <= result["age"] <= 40, (
                        f"Record {result['id']} age should be 25-40, got {result['age']}"
                    )
            else:  # OR
                # Check individual conditions
                center_results, _ = self.query(client,
                    collection_name=collection_name,
                    filter=f"ST_INTERSECTS(geo, '{center_region}')",
                    output_fields=["id"],
                )
                older_results, _ = self.query(client,
                    collection_name=collection_name,
                    filter="age > 50",
                    output_fields=["id"],
                )
                younger_results, _ = self.query(client,
                    collection_name=collection_name,
                    filter="age < 30",
                    output_fields=["id"],
                )

                individual_counts = [
                    len(center_results),
                    len(older_results),
                    len(younger_results),
                ]
                expected_min = max(individual_counts)
                assert len(results) >= expected_min, (
                    f"OR filter should find at least {expected_min} results"
                )

        else:  # geo_varchar
            if logical_op == "AND":
                # All results should satisfy both geo and varchar conditions
                for result in results:
                    # Check spatial condition
                    spatial_check, _ = self.query(client,
                        collection_name=collection_name,
                        filter=f"id == {result['id']} AND ST_WITHIN(geo, '{northeast_region}')",
                        output_fields=["id"],
                    )
                    assert len(spatial_check) == 1, (
                        f"Record {result['id']} should be in northeast region"
                    )

                    # Check category condition
                    assert result["category"] == "premium", (
                        f"Record {result['id']} should be premium category, got {result['category']}"
                    )
            else:  # OR
                # Check individual conditions
                southeast_results, _ = self.query(client,
                    collection_name=collection_name,
                    filter=f"ST_WITHIN(geo, '{southeast_region}')",
                    output_fields=["id"],
                )
                enterprise_results, _ = self.query(client,
                    collection_name=collection_name,
                    filter="category == 'enterprise'",
                    output_fields=["id"],
                )
                standard_results, _ = self.query(client,
                    collection_name=collection_name,
                    filter="category == 'standard'",
                    output_fields=["id"],
                )

                individual_counts = [
                    len(southeast_results),
                    len(enterprise_results),
                    len(standard_results),
                ]
                expected_min = max(individual_counts)
                assert len(results) >= expected_min, (
                    f"OR filter should find at least {expected_min} results"
                )

        # Test search with complex filter
        search_params = {"metric_type": "L2", "params": {"nprobe": 10}}
        query_vector = [random.random() for _ in range(default_dim)]

        search_results, _ = self.search(client,
            collection_name=collection_name,
            data=[query_vector],
            anns_field="vector",
            search_params=search_params,
            limit=25,
            filter=filter_expr,
            output_fields=["id", "geo", "age", "category"],
        )

        # Search results should be subset of query results
        search_ids = {hit["id"] for hit in search_results[0]}
        query_ids = {r["id"] for r in results}
        assert search_ids.issubset(query_ids), (
            f"Search results should be subset of query results for {filter_type} {logical_op} filter"
        )

        # Test additional complex combinations
        if filter_type == "multi_geo" and logical_op == "AND":
            # Test three geo functions with AND
            triple_geo_filter = f"ST_WITHIN(geo, '{large_region}') AND ST_INTERSECTS(geo, '{center_region}') AND NOT ST_EQUALS(geo, 'POINT (0 0)')"
            triple_results, _ = self.query(client,
                collection_name=collection_name,
                filter=triple_geo_filter,
                output_fields=["id", "geo"],
            )
            assert len(triple_results) <= len(results), (
                "Triple AND condition should return fewer or equal results"
            )

        elif filter_type == "geo_varchar" and logical_op == "OR":
            # Test mixed geo and multiple varchar conditions
            mixed_filter = f"ST_WITHIN(geo, '{center_region}') OR category IN ['premium', 'enterprise'] OR category == 'trial'"
            mixed_results, _ = self.query(client,
                collection_name=collection_name,
                filter=mixed_filter,
                output_fields=["id", "geo", "category"],
            )
            assert len(mixed_results) > 0, (
                "Mixed geo and varchar OR filter should find results"
            )

    @pytest.mark.tags(CaseLabel.L1)
    def test_upsert_geometry_data(self):
        """
        target: test upsert geometry data
        method: create collection with geometry field, upsert data and search
        expected: upsert successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create schema with geometry field
        schema, _ = self.create_schema(client,
            auto_id=False, description="test geometry collection"
        )
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("geo", DataType.GEOMETRY)

        # Create collection
        index_params, _ = self.prepare_index_params(client)
        index_params.add_index("vector")
        index_params.add_index("geo", index_type="RTREE")
        self.create_collection(
            client, collection_name, schema=schema, index_params=index_params
        )

        # Initial insert
        rng = np.random.default_rng(seed=19530)
        initial_data = []
        for i in range(10):
            x, y = rng.uniform(-180, 180), rng.uniform(-90, 90)
            point_wkt = f"POINT ({x} {y})"
            log.debug(f"Point WKT: {point_wkt}")
            initial_data.append(
                {"id": i, "vector": rng.random(default_dim).tolist(), "geo": point_wkt}
            )

        insert_res, _ = self.insert(client,collection_name, initial_data)
        assert insert_res["insert_count"] == 10

        # Store original geometry values before upsert for comparison
        original_records, _ = self.query(client,
            collection_name, filter="", output_fields=["id", "geo"], limit=100
        )
        original_geometries = {
            record["id"]: record["geo"] for record in original_records
        }
        log.debug(f"Original geometries: {original_geometries}")

        # Upsert data - update some existing records and add new ones
        upsert_data = []
        expected_upserted_geometries = {}  # Store expected values for verification
        for i in range(5, 15):  # Update ids 5-9, add new ids 10-14
            x, y = rng.uniform(-180, 180), rng.uniform(-90, 90)
            point_wkt = f"POINT ({x} {y})"
            upsert_data.append(
                {"id": i, "vector": rng.random(default_dim).tolist(), "geo": point_wkt}
            )
            expected_upserted_geometries[i] = point_wkt

        upsert_res, _ = self.upsert(client,collection_name, upsert_data)
        assert upsert_res["upsert_count"] == 10
        # Query to verify total count after upsert
        results, _ = self.query(client,
            collection_name, filter="", output_fields=["id", "geo"], limit=100
        )
        assert len(results) == 15  # 10 original + 5 new - 0 duplicates = 15 total
        log.debug(f"Results: {results}")

        # Verify that updated records (5-9) have the exact geometry values we upserted
        updated_ids = [5, 6, 7, 8, 9]
        for updated_id in updated_ids:
            updated_record = [r for r in results if r["id"] == updated_id][0]
            updated_geo = updated_record["geo"]
            expected_geo = expected_upserted_geometries[updated_id]

            assert updated_geo is not None
            assert "POINT" in updated_geo
            # Verify it matches exactly what we upserted
            assert updated_geo == expected_geo, (
                f"Record {updated_id} geometry should match upserted value: expected {expected_geo}, got {updated_geo}"
            )

        # Verify that records 0-4 remain unchanged (not upserted)
        unchanged_ids = [0, 1, 2, 3, 4]
        for unchanged_id in unchanged_ids:
            unchanged_record = next(r for r in results if r["id"] == unchanged_id)
            assert unchanged_record["geo"] == original_geometries[unchanged_id], (
                f"Record {unchanged_id} should remain unchanged"
            )

        # Verify that new records 10-14 exist with exact upserted values
        new_ids = [10, 11, 12, 13, 14]
        for new_id in new_ids:
            new_record = next(r for r in results if r["id"] == new_id)
            expected_geo = expected_upserted_geometries[new_id]

            assert new_record["geo"] is not None
            assert "POINT" in new_record["geo"]
            # This should be a new record, not in original data
            assert new_id not in original_geometries
            # Verify it matches exactly what we upserted
            assert new_record["geo"] == expected_geo, (
                f"New record {new_id} geometry should match upserted value: expected {expected_geo}, got {new_record['geo']}"
            )

    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_geometry_with_ids(self):
        """
        target: test delete geometry data with ids
        method: create collection with geometry field, insert data, delete by ids and verify
        expected: delete successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create schema with geometry field
        schema, _ = self.create_schema(client,
            auto_id=False, description="test geometry collection"
        )
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("geo", DataType.GEOMETRY)

        # Create collection
        index_params, _ = self.prepare_index_params(client)
        index_params.add_index("vector")
        index_params.add_index("geo", index_type="RTREE")
        self.create_collection(
            client, collection_name, schema=schema, index_params=index_params
        )
        res = self.describe_collection(client, collection_name)
        print(res)

        # Insert data
        rng = np.random.default_rng(seed=19530)
        data = []
        for i in range(20):
            x, y = rng.uniform(-180, 180), rng.uniform(-90, 90)
            point_wkt = f"POINT ({x} {y})"
            data.append(
                {"id": i, "vector": rng.random(default_dim).tolist(), "geo": point_wkt}
            )

        insert_res, _ = self.insert(client,collection_name, data)
        assert insert_res["insert_count"] == 20

        # Store original data before deletion for verification
        original_results, _ = self.query(client,
            collection_name, filter="", output_fields=["id", "geo"], limit=100
        )
        original_ids = {r["id"] for r in original_results}
        original_geometries = {r["id"]: r["geo"] for r in original_results}
        print(original_geometries)

        # Delete some records by IDs
        delete_ids = [1, 3, 5, 7, 9]
        self.delete(client,collection_name, ids=delete_ids)

        # Verify deletion - records should be completely gone
        results, _ = self.query(client,
            collection_name, filter="", output_fields=["id", "geo"], limit=100
        )
        print(results)
        remaining_ids = {r["id"] for r in results}

        assert len(results) == 15  # 20 - 5 deleted = 15

        # Verify deleted records are not in results
        for deleted_id in delete_ids:
            assert deleted_id not in remaining_ids, (
                f"Deleted record {deleted_id} should not exist"
            )
            # Also verify by trying to query the specific deleted record
            specific_query, _ = self.query(client,
                collection_name, filter=f"id == {deleted_id}", output_fields=["id"]
            )
            assert len(specific_query) == 0, (
                f"Deleted record {deleted_id} should return no results when queried directly"
            )

        # Verify remaining records are exactly the expected ones
        expected_remaining_ids = original_ids - set(delete_ids)
        assert remaining_ids == expected_remaining_ids, (
            "Remaining IDs should match expected"
        )

        # Verify remaining records have their original geometry values unchanged
        for remaining_id in remaining_ids:
            remaining_record = next(r for r in results if r["id"] == remaining_id)
            assert remaining_record["geo"] == original_geometries[remaining_id], (
                f"Remaining record {remaining_id} geometry should be unchanged"
            )

    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_geometry_with_spatial_filters(self):
        """
        target: test delete geometry data with spatial filters
        method: create collection with geometry field, insert data, delete with spatial filter and verify
        expected: delete successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create schema with geometry field
        schema, _ = self.create_schema(client,
            auto_id=False, description="test geometry collection"
        )
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("geo", DataType.GEOMETRY)

        # Create collection
        index_params, _ = self.prepare_index_params(client)
        index_params.add_index("vector")
        index_params.add_index("geo", index_type="RTREE")
        self.create_collection(
            client, collection_name, schema=schema, index_params=index_params
        )

        # Insert data with specific geometry layout
        data = []
        rng = np.random.default_rng(seed=19530)

        # Insert points inside a specific region (will be deleted)
        region_to_delete = "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"
        for i in range(10):
            # Points inside the deletion region
            x, y = rng.uniform(1, 9), rng.uniform(1, 9)
            point_wkt = f"POINT ({x} {y})"
            data.append(
                {"id": i, "vector": rng.random(default_dim).tolist(), "geo": point_wkt}
            )

        # Insert points outside the region (will remain)
        for i in range(10, 20):
            # Points outside the deletion region
            x, y = rng.uniform(20, 30), rng.uniform(20, 30)
            point_wkt = f"POINT ({x} {y})"
            data.append(
                {"id": i, "vector": rng.random(default_dim).tolist(), "geo": point_wkt}
            )

        insert_res, _ = self.insert(client,collection_name, data)
        assert insert_res["insert_count"] == 20

        # Store original data before deletion for verification
        before_results, _ = self.query(client,
            collection_name, filter="", output_fields=["id", "geo"], limit=100
        )
        assert len(before_results) == 20

        # Identify which records should be deleted by the spatial filter
        spatial_filter = f"ST_WITHIN(geo, '{region_to_delete}')"
        records_to_delete, _ = self.query(client,
            collection_name,
            filter=spatial_filter,
            output_fields=["id", "geo"],
            limit=100,
        )
        delete_ids = {r["id"] for r in records_to_delete}

        # Verify we found the expected records to delete (should be IDs 0-9)
        expected_delete_ids = {i for i in range(10)}
        assert delete_ids == expected_delete_ids, (
            f"Expected to delete {expected_delete_ids}, found {delete_ids}"
        )

        # Delete with spatial filter
        self.delete(client,collection_name, filter=spatial_filter)

        # Verify deletion
        after_results, _ = self.query(client,
            collection_name, filter="", output_fields=["id", "geo"], limit=100
        )
        remaining_ids = {r["id"] for r in after_results}

        assert len(after_results) == 10  # Only points outside the region should remain

        # Verify that deleted records are completely gone
        for deleted_id in delete_ids:
            assert deleted_id not in remaining_ids, (
                f"Spatially deleted record {deleted_id} should not exist"
            )
            # Also verify by trying to query the specific deleted record
            specific_query, _ = self.query(client,
                collection_name,
                filter=f"id == {deleted_id}",
                output_fields=["id"],
                limit=100,
            )
            assert len(specific_query) == 0, (
                f"Spatially deleted record {deleted_id} should return no results when queried directly"
            )

        # Verify that remaining points are exactly the expected ones (IDs 10-19)
        expected_remaining_ids = {i for i in range(10, 20)}
        assert remaining_ids == expected_remaining_ids, (
            f"Remaining IDs should be {expected_remaining_ids}, got {remaining_ids}"
        )

        # Verify that remaining points are outside the deletion region
        for record in after_results:
            assert record["id"] >= 10, (
                f"Record {record['id']} should not have been deleted"
            )

        # Double-check: verify no remaining records are within the deletion region
        remaining_in_region, _ = self.query(client,
            collection_name, filter=spatial_filter, output_fields=["id"], limit=100
        )
        assert len(remaining_in_region) == 0, (
            "No records should remain within the deletion region"
        )

        # Verify that spatial queries still work on remaining data
        remaining_region = "POLYGON ((15 15, 35 15, 35 35, 15 35, 15 15))"
        spatial_query_results, _ = self.query(client,
            collection_name,
            filter=f"ST_WITHIN(geo, '{remaining_region}')",
            output_fields=["id", "geo"],
            limit=100,
        )
        assert (
            len(spatial_query_results) == 10
        )  # All remaining points should be in this region

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("distance_meters", [1000, 2000, 20000])
    @pytest.mark.parametrize("with_geo_index", [True, False])
    def test_st_dwithin_functionality_and_index(self, distance_meters, with_geo_index):
        """
        target: test ST_DWITHIN operator with various distances and index configurations using latitude/longitude coordinates
        method: insert thousands of POINT geometries, test with/without RTREE index, verify results using ground truth
        expected: correct results matching Haversine distance calculations, consistent with and without index
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create collection with geometry field
        schema, _ = self.create_schema(client, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("geo", DataType.GEOMETRY)

        self.create_collection(client, collection_name, schema=schema)

        # Generate latitude/longitude based test data with thousands of points
        center_lat, center_lon = 40.7128, -74.0060  # NYC coordinates
        geo_points = generate_latlon_data_for_dwithin(count=3000, center_lat=center_lat, center_lon=center_lon)

        # Create test data with generated points
        data = []
        base_data = []
        for i, geo_point in enumerate(geo_points):
            item = {
                "id": i,
                "vector": [random.random() for _ in range(default_dim)],
                "geo": geo_point
            }
            data.append(item)
            base_data.append({"id": i, "geo": geo_point})

        self.insert(client, collection_name, data)
        self.flush(client, collection_name)

        # Create indexes based on parameter
        index_params, _ = self.prepare_index_params(client)
        index_params.add_index(
            field_name="vector", index_type="IVF_FLAT", metric_type="L2", nlist=128
        )
        if with_geo_index:
            index_params.add_index(field_name="geo", index_type="RTREE")

        self.create_index(client, collection_name, index_params=index_params)

        # Load collection
        self.load_collection(client, collection_name)

        # Generate query point and calculate ground truth using Haversine distance
        query_point = generate_dwithin_query_point(center_lat=center_lat, center_lon=center_lon)
        expected_within = calculate_expected_ids_for_dwithin(
            base_data, query_point, distance_meters, geo_field_name="geo", pk_field_name="id"
        )

        # Query using ST_DWITHIN
        results, _ = self.query(
            client,
            collection_name=collection_name,
            filter=f"ST_DWITHIN(geo, '{query_point}', {distance_meters})",
            output_fields=["id", "geo"],
        )

        # Verify results match ground truth
        result_ids = {result["id"] for result in results}
        assert result_ids == expected_within, (
            f"ST_DWITHIN({distance_meters}m, index={with_geo_index}) should return expected IDs "
            f"(count: {len(expected_within)}), but got different IDs (count: {len(result_ids)})"
        )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("with_geo_index", [True, False])
    def test_st_dwithin_edge_cases(self, with_geo_index):
        """
        target: test ST_DWITHIN with edge cases and boundary conditions, with/without RTREE index
        method: test with zero distance, small distances, and boundary points using ground truth verification
        expected: correct behavior for edge cases, consistent results with and without index
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create collection with geometry field
        schema, _ = self.create_schema(client, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("geo", DataType.GEOMETRY)

        self.create_collection(client, collection_name, schema=schema)

        # Generate test data using lat/lon coordinates for edge case testing
        center_lat, center_lon = 40.7128, -74.0060  # NYC coordinates

        # Generate edge case test points using the data generation function
        geo_points = [
            generate_dwithin_query_point(center_lat=center_lat, center_lon=center_lon),  # Exact match point
            f"POINT ({center_lon + 0.0001:.6f} {center_lat:.6f})",  # Very close point (~10m)
            f"POINT ({center_lon + 0.001:.6f} {center_lat:.6f})",   # ~100m away
            f"POINT ({center_lon + 0.1:.6f} {center_lat:.6f})",     # Far point (~10km)
        ]

        data = []
        for i, geo_point in enumerate(geo_points):
            data.append({
                "id": i + 1,
                "vector": [random.random() for _ in range(default_dim)],
                "geo": geo_point
            })

        self.insert(client, collection_name, data)
        self.flush(client, collection_name)

        # Create indexes based on parameter
        index_params, _ = self.prepare_index_params(client)
        index_params.add_index(
            field_name="vector", index_type="IVF_FLAT", metric_type="L2", nlist=128
        )
        if with_geo_index:
            index_params.add_index(field_name="geo", index_type="RTREE")

        self.create_index(client, collection_name, index_params=index_params)

        # Load collection
        self.load_collection(client, collection_name)

        # Generate query point and base data for ground truth calculation
        query_point = generate_dwithin_query_point(center_lat=center_lat, center_lon=center_lon)
        base_data = [{"id": i + 1, "geo": geo_point} for i, geo_point in enumerate(geo_points)]

        # Test zero distance using ground truth
        expected_zero = calculate_expected_ids_for_dwithin(
            base_data, query_point, 0, geo_field_name="geo", pk_field_name="id"
        )
        results_zero, _ = self.query(
            client,
            collection_name=collection_name,
            filter=f"ST_DWITHIN(geo, '{query_point}', 0)",
            output_fields=["id", "geo"],
        )

        result_ids_zero = {result["id"] for result in results_zero}
        assert result_ids_zero == expected_zero, (
            f"Zero distance (index={with_geo_index}) should return expected IDs {expected_zero}, got {result_ids_zero}"
        )

        # Test small distance (50m) using ground truth
        expected_50m = calculate_expected_ids_for_dwithin(
            base_data, query_point, 50, geo_field_name="geo", pk_field_name="id"
        )
        results_small, _ = self.query(
            client,
            collection_name=collection_name,
            filter=f"ST_DWITHIN(geo, '{query_point}', 50)",
            output_fields=["id", "geo"],
        )

        result_ids_small = {result["id"] for result in results_small}
        assert result_ids_small == expected_50m, (
            f"50m distance (index={with_geo_index}) should return expected IDs {expected_50m}, got {result_ids_small}"
        )

        # Test medium distance (200m) using ground truth
        expected_200m = calculate_expected_ids_for_dwithin(
            base_data, query_point, 200, geo_field_name="geo", pk_field_name="id"
        )
        results_medium, _ = self.query(
            client,
            collection_name=collection_name,
            filter=f"ST_DWITHIN(geo, '{query_point}', 200)",
            output_fields=["id", "geo"],
        )

        result_ids_medium = {result["id"] for result in results_medium}
        assert result_ids_medium == expected_200m, (
            f"200m distance (index={with_geo_index}) should return expected IDs {expected_200m}, got {result_ids_medium}"
        )

        # Test large distance (20km) using ground truth
        expected_20km = calculate_expected_ids_for_dwithin(
            base_data, query_point, 20000, geo_field_name="geo", pk_field_name="id"
        )
        results_large, _ = self.query(
            client,
            collection_name=collection_name,
            filter=f"ST_DWITHIN(geo, '{query_point}', 20000)",
            output_fields=["id", "geo"],
        )

        result_ids_large = {result["id"] for result in results_large}
        assert result_ids_large == expected_20km, (
            f"20km distance (index={with_geo_index}) should return expected IDs {expected_20km}, got {result_ids_large}"
        )

        # Verify distance ordering: larger distances should return same or more results
        assert len(result_ids_zero) <= len(result_ids_small) <= len(result_ids_medium) <= len(result_ids_large), (
            "Larger distances should return same or more results"
        )




class TestMilvusClientGeometryNegative(TestMilvusClientV2Base):
    """Test case of geometry operations - negative cases"""

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "invalid_wkt",
        [
            "INVALID WKT STRING",  # Completely invalid WKT
            "POINT ()",  # Empty coordinates
            "POINT (abc def)",  # Non-numeric coordinates
            "POLYGON ((0 0, 1 1))",  # Unclosed polygon (less than 4 points)
            "LINESTRING (0 0)",  # Single point linestring
            "POINT (1 2 3 4 5)",  # Too many coordinates
            "POLYGON ((0 0, 1 0, 1 1, 0 1))",  # Unclosed polygon ring (missing closing point)
            "",  # Empty string
        ],
    )
    def test_insert_invalid_wkt_data(self, invalid_wkt):
        """
        target: test error handling for invalid WKT formats
        method: insert invalid WKT geometry data and expect appropriate errors
        expected: should raise appropriate errors for invalid WKT
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create collection
        schema, _ = self.create_schema(client,
            auto_id=False, description="test invalid WKT error handling"
        )
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("geo", DataType.GEOMETRY)

        self.create_collection(client, collection_name, schema=schema)

        # Prepare data with invalid WKT
        data = [
            {
                "id": 0,
                "vector": [random.random() for _ in range(default_dim)],
                "geo": invalid_wkt,
            }
        ]

        error = {
            ct.err_code: 1001,
            ct.err_msg: "syntax error",
        }  # We expect some error related to invalid geometry
        self.insert(
            client,
            collection_name,
            data,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_spatial_query_on_non_geometry_fields(self):
        """
        target: test spatial query on non-geometry field should fail
        method: try spatial functions on non-geometry fields
        expected: should raise appropriate errors
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create collection with non-geometry field
        schema, _ = self.create_schema(client,
            auto_id=False, description="test spatial query on non-geometry field"
        )
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("text_field", DataType.VARCHAR, max_length=100)
        schema.add_field("int_field", DataType.INT64)

        self.create_collection(client, collection_name, schema=schema)

        # Insert some data
        data = []
        for i in range(10):
            data.append(
                {
                    "id": i,
                    "vector": [random.random() for _ in range(default_dim)],
                    "text_field": f"text_{i}",
                    "int_field": i * 10,
                }
            )

        self.insert(client, collection_name, data)
        self.flush(client,collection_name)

        # Build indexes
        index_params, _ = self.prepare_index_params(client)
        index_params.add_index(
            field_name="vector", index_type="IVF_FLAT", metric_type="L2", nlist=128
        )

        self.create_index(client, collection_name, index_params=index_params)
        self.load_collection(client,collection_name)

        # Try spatial query on non-geometry fields - should fail
        query_polygon = "POLYGON ((40 40, 60 40, 60 60, 40 60, 40 40))"

        # Test spatial query on varchar field - should fail
        error = {
            ct.err_code: 1100,
            ct.err_msg: "failed to create query plan: cannot parse expression",
        }  # We expect error for spatial function on varchar field
        self.query(
            client,
            collection_name,
            filter=f"ST_WITHIN(text_field, '{query_polygon}')",
            output_fields=["id", "text_field"],
            check_task=CheckTasks.err_res,
            check_items=error,
        )

        # Test spatial query on int field - should fail
        error = {
            ct.err_code: 1100,
            ct.err_msg: "failed to create query plan: cannot parse expression",
        }  # We expect error for spatial function on int field
        self.query(
            client,
            collection_name,
            filter=f"ST_CONTAINS(int_field, '{query_polygon}')",
            output_fields=["id", "int_field"],
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "invalid_spatial_func",
        [
            "ST_INVALID_FUNCTION",
            "INVALID_ST_CONTAINS",
            "ST_INTERSECT",  # Missing 'S' at the end
            "ST_CONTAIN",  # Missing 'S' at the end
        ],
    )
    def test_invalid_spatial_function_names(self, invalid_spatial_func):
        """
        target: test invalid spatial function names
        method: use invalid spatial function names in queries
        expected: should raise appropriate errors
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create collection with valid data
        schema, _ = self.create_schema(client,
            auto_id=False, description="test invalid spatial functions"
        )
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("geo", DataType.GEOMETRY)

        self.create_collection(client, collection_name, schema=schema)

        # Insert valid data
        data = [
            {
                "id": 0,
                "vector": [random.random() for _ in range(default_dim)],
                "geo": "POINT (50 50)",
            }
        ]

        self.insert(client, collection_name, data)
        self.flush(client,collection_name)

        # Build indexes
        index_params, _ = self.prepare_index_params(client)
        index_params.add_index(
            field_name="vector", index_type="IVF_FLAT", metric_type="L2", nlist=128
        )
        index_params.add_index(field_name="geo", index_type="RTREE")

        self.create_index(client, collection_name, index_params=index_params)
        self.load_collection(client,collection_name)

        # Try query with invalid spatial function - should fail
        query_polygon = "POLYGON ((40 40, 60 40, 60 60, 40 60, 40 40))"
        filter_expr = f"{invalid_spatial_func}(geo, '{query_polygon}')"

        error = {
            ct.err_code: 65535,
            ct.err_msg: "Create retrieve plan by expr failed",
        }  # We expect error for invalid spatial function
        self.query(
            client,
            collection_name,
            filter=filter_expr,
            output_fields=["id", "geo"],
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_spatial_query_with_wrong_parameters(self):
        """
        target: test spatial functions with wrong number of parameters
        method: call spatial functions with incorrect parameter count
        expected: should raise appropriate errors
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create collection with valid data
        schema, _ = self.create_schema(client,
            auto_id=False, description="test wrong parameter count"
        )
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("geo", DataType.GEOMETRY)

        self.create_collection(client, collection_name, schema=schema)

        # Insert valid data
        data = [
            {
                "id": 0,
                "vector": [random.random() for _ in range(default_dim)],
                "geo": "POINT (50 50)",
            }
        ]

        self.insert(client, collection_name, data)
        self.flush(client,collection_name)

        # Build indexes
        index_params, _ = self.prepare_index_params(client)
        index_params.add_index(
            field_name="vector", index_type="IVF_FLAT", metric_type="L2", nlist=128
        )
        index_params.add_index(field_name="geo", index_type="RTREE")

        self.create_index(client, collection_name, index_params=index_params)
        self.load_collection(client,collection_name)

        # Test cases with wrong parameter counts
        invalid_filters = [
            "ST_CONTAINS(geo)",  # Missing second parameter
            "ST_WITHIN()",  # Missing both parameters
            "ST_INTERSECTS(geo, 'POINT (1 1)', 'POINT (2 2)')",  # Too many parameters
        ]

        for invalid_filter in invalid_filters:
            error = {
                ct.err_code: 1100,
                ct.err_msg: "failed to create query plan: cannot parse expression",
            }  # We expect error for wrong parameters
            self.query(
                client,
                collection_name,
                filter=invalid_filter,
                output_fields=["id", "geo"],
                check_task=CheckTasks.err_res,
                check_items=error,
            )

    @pytest.mark.tags(CaseLabel.L1)
    def test_geometry_field_as_partition_key(self):
        """
        target: test create collection with geometry field as partition key
        method: create collection with geometry field set as partition key
        expected: should raise PartitionKeyException
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create schema with geometry field as partition key
        schema, _ = self.create_schema(client,
            auto_id=False, description="test geometry partition key error"
        )
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("geo", DataType.GEOMETRY, is_partition_key=True)

        # This should fail with PartitionKeyException
        error = {
            ct.err_code: 1,
            ct.err_msg: "Partition key field type must be DataType.INT64 or DataType.VARCHAR",
        }
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_inverted_index_on_geo_field(self):
        """
        target: test error handling for creating INVERTED index on geometry field
        method: create INVERTED index on geometry field and expect appropriate error
        expected: should raise error as INVERTED index is not supported for geometry fields
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create collection with geometry field
        schema, _ = self.create_schema(client,
            auto_id=False, description="test invalid index on geo field"
        )
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("geo", DataType.GEOMETRY)

        self.create_collection(client, collection_name, schema=schema)

        # Try to create INVERTED index on geometry field
        index_params, _ = self.prepare_index_params(client)
        index_params.add_index(
            field_name="vector", index_type="IVF_FLAT", metric_type="L2", nlist=128
        )
        index_params.add_index(field_name="geo", index_type="INVERTED")

        # This should fail
        error = {
            ct.err_code: 1100,
            ct.err_msg: "INVERTED are not supported on Geometry field",
        }
        self.create_index(
            client,
            collection_name,
            index_params=index_params,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_rtree_index_on_int_field(self):
        """
        target: test error handling for creating RTREE index on non-geometry field
        method: create RTREE index on int field and expect appropriate error
        expected: should raise error as RTREE index is only supported for geometry fields
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create collection with int field
        schema, _ = self.create_schema(client,
            auto_id=False, description="test invalid index on int field"
        )
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("int_field", DataType.INT64)

        self.create_collection(client, collection_name, schema=schema)

        # Try to create RTREE index on int field
        index_params, _ = self.prepare_index_params(client)
        index_params.add_index(
            field_name="vector", index_type="IVF_FLAT", metric_type="L2", nlist=128
        )
        index_params.add_index(field_name="int_field", index_type="RTREE")

        # This should fail
        error = {
            ct.err_code: 1100,
            ct.err_msg: "RTREE index can only be built on geometry field",
        }
        self.create_index(
            client,
            collection_name,
            index_params=index_params,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("with_geo_index", [True, False])
    def test_st_dwithin_with_invalid_filters(self, with_geo_index):
        """
        target: test ST_DWITHIN error handling for invalid parameters with/without RTREE index
        method: test with invalid geometries, negative distances, and wrong parameter types
        expected: appropriate error messages for invalid inputs, consistent behavior with and without index
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create collection with geometry field
        schema, _ = self.create_schema(client, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("geo", DataType.GEOMETRY)

        self.create_collection(client, collection_name, schema=schema)

        # Generate test data using lat/lon coordinates
        center_lat, center_lon = 40.7128, -74.0060  # NYC coordinates
        test_point = generate_dwithin_query_point(center_lat=center_lat, center_lon=center_lon)

        data = [
            {
                "id": 1,
                "vector": [random.random() for _ in range(default_dim)],
                "geo": test_point
            }
        ]
        self.insert(client, collection_name, data)
        self.flush(client, collection_name)

        # Create indexes based on parameter
        index_params, _ = self.prepare_index_params(client)
        index_params.add_index(
            field_name="vector", index_type="IVF_FLAT", metric_type="L2", nlist=128
        )
        if with_geo_index:
            index_params.add_index(field_name="geo", index_type="RTREE")

        self.create_index(client, collection_name, index_params=index_params)

        # Load collection
        self.load_collection(client, collection_name)

        # Test invalid WKT geometry
        error_invalid_wkt = {
            ct.err_code: 1100,
            ct.err_msg: "failed to create query plan: cannot parse expression",
        }
        self.query(
            client,
            collection_name=collection_name,
            filter="ST_DWITHIN(geo, 'INVALID_WKT', 1000)",
            output_fields=["id", "geo"],
            check_task=CheckTasks.err_res,
            check_items=error_invalid_wkt,
        )

        # Test negative distance
        error_negative_distance = {
            ct.err_code: 1100,
            ct.err_msg: "failed to create query plan: cannot parse expression",
        }
        self.query(
            client,
            collection_name=collection_name,
            filter=f"ST_DWITHIN(geo, '{test_point}', -100)",
            output_fields=["id", "geo"],
            check_task=CheckTasks.err_res,
            check_items=error_negative_distance,
        )

        # Test missing parameters
        error_missing_params = {
            ct.err_code: 1100,
            ct.err_msg: "failed to create query plan: cannot parse expression",
        }
        self.query(
            client,
            collection_name=collection_name,
            filter=f"ST_DWITHIN(geo, '{test_point}')",  # Missing distance
            output_fields=["id", "geo"],
            check_task=CheckTasks.err_res,
            check_items=error_missing_params,
        )

        # Test too many parameters
        self.query(
            client,
            collection_name=collection_name,
            filter=f"ST_DWITHIN(geo, '{test_point}', 1000, 'extra')",
            output_fields=["id", "geo"],
            check_task=CheckTasks.err_res,
            check_items=error_missing_params,
        )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("with_geo_index", [True, False])
    @pytest.mark.skip(reason="not implemented for verifcation")
    def test_st_dwithin_invalid_query_coordinates(self, with_geo_index):
        """
        target: test ST_DWITHIN with invalid query latitude/longitude coordinates
        method: test with coordinates outside valid ranges (lat: -90 to 90, lon: -180 to 180)
        expected: appropriate error handling for invalid coordinate values
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create collection with geometry field
        schema, _ = self.create_schema(client, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("geo", DataType.GEOMETRY)

        self.create_collection(client, collection_name, schema=schema)

        # Insert valid test data first
        center_lat, center_lon = 40.7128, -74.0060  # NYC coordinates
        valid_point = generate_dwithin_query_point(center_lat=center_lat, center_lon=center_lon)

        data = [
            {
                "id": 1,
                "vector": [random.random() for _ in range(default_dim)],
                "geo": valid_point
            }
        ]
        self.insert(client, collection_name, data)
        self.flush(client, collection_name)

        # Create indexes based on parameter
        index_params, _ = self.prepare_index_params(client)
        index_params.add_index(
            field_name="vector", index_type="IVF_FLAT", metric_type="L2", nlist=128
        )
        if with_geo_index:
            index_params.add_index(field_name="geo", index_type="RTREE")

        self.create_index(client, collection_name, index_params=index_params)

        # Load collection
        self.load_collection(client, collection_name)

        # Test cases with invalid coordinates
        invalid_coordinate_cases = [
            # Invalid latitude (> 90)
            ("POINT (-74.0060 95.0)", "latitude > 90"),
            # Invalid latitude (< -90)
            ("POINT (-74.0060 -95.0)", "latitude < -90"),
            # Invalid longitude (> 180)
            ("POINT (185.0 40.7128)", "longitude > 180"),
            # Invalid longitude (< -180)
            ("POINT (-185.0 40.7128)", "longitude < -180"),
            # Both coordinates invalid
            ("POINT (200.0 100.0)", "both coordinates invalid"),
            # Edge case: exactly at boundary but invalid
            ("POINT (180.1 90.1)", "slightly over boundary"),
        ]

        error_invalid_coords = {
            ct.err_code: 1,
            ct.err_msg: "invalid coordinate",  # May need to adjust based on actual error message
        }

        for invalid_point, description in invalid_coordinate_cases:
            # Test query with invalid coordinates
            self.query(
                client,
                collection_name=collection_name,
                filter=f"ST_DWITHIN(geo, '{invalid_point}', 1000)",
                output_fields=["id", "geo"],
                check_task=CheckTasks.err_res,
                check_items=error_invalid_coords,
            )


    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="not implemented for verifcation")
    def test_st_dwithin_invalid_base_data_coordinates(self):
        """
        target: test ST_DWITHIN with invalid coordinates in base data
        method: attempt to insert POINT data with coordinates outside valid lat/lon ranges
        expected: appropriate error handling during data insertion or querying
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create collection with geometry field
        schema, _ = self.create_schema(client, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("geo", DataType.GEOMETRY)

        self.create_collection(client, collection_name, schema=schema)

        # Test cases with invalid base data coordinates
        invalid_base_data_cases = [
            {
                "id": 1,
                "vector": [random.random() for _ in range(default_dim)],
                "geo": "POINT (-74.0060 95.0)"  # Invalid latitude > 90
            },
            {
                "id": 2,
                "vector": [random.random() for _ in range(default_dim)],
                "geo": "POINT (-74.0060 -95.0)"  # Invalid latitude < -90
            },
            {
                "id": 3,
                "vector": [random.random() for _ in range(default_dim)],
                "geo": "POINT (185.0 40.7128)"  # Invalid longitude > 180
            },
            {
                "id": 4,
                "vector": [random.random() for _ in range(default_dim)],
                "geo": "POINT (-185.0 40.7128)"  # Invalid longitude < -180
            }
        ]

        # Test each invalid coordinate case individually
        for invalid_data in invalid_base_data_cases:
            # Create a new collection for each test to avoid state pollution
            test_collection_name = f"{collection_name}_{invalid_data['id']}"

            # Create collection
            self.create_collection(client, test_collection_name, schema=schema)

            # Try to insert invalid coordinate data
            error_invalid_insert = {
                ct.err_code: 1100,
                ct.err_msg: "failed to insert invalid geometry",
            }
            self.insert(
                client,
                test_collection_name,
                [invalid_data],
                check_task=CheckTasks.err_res,
                check_items=error_invalid_insert,
            )