# -*- coding: utf-8 -*-
"""
Created on Sat Apr 16 20:03:28 2022

@author: labpc2
"""

from os import path
import osmnx as ox
import geopandas as gpd
import numpy as np
from shapely.geometry import LineString, Point, MultiPoint
from shapely.ops import linemerge
from rtree import index

from pyproj import  Transformer


class BaselineRoutes:
    
    def __init__(self):
        # import airspace polygon with geopandas
        airspace_path = path.join(path.dirname(__file__), "gis/airspace/total_polygon.gpkg")
        self.airspace = gpd.read_file(airspace_path, driver="GPKG", layer="total_polygon")
    
        # import airspace polygon with geopandas
        con_airspace_path = path.join(
            path.dirname(__file__), "gis/airspace/updated_constrained_airspace.gpkg"
        )
        self.con_airspace = gpd.read_file(con_airspace_path, driver="GPKG")
    
        # import common elements graph with osmnx
        graph_path = path.join(
            path.dirname(__file__),
            "gis/road_network/crs_4326_cleaned_simplified_network/cleaned_simplified.graphml",
        )
        self.G = ox.load_graphml(graph_path)
    
        # convert to undirected graph
        self.G_undirected = ox.get_undirected(self.G)

        # get the border nodes and rtree of constrained airspace
        self.border_node_gdf, self.border_node_rtree = find_border_nodes(self.con_airspace, self.G_undirected )
        self.node_gdf, self.node_rtree =build_rtree_nodes(self.con_airspace, self.G_undirected )
        
    def compute_len(self,origin_dest_tuple):
        transformer = Transformer.from_crs('epsg:4326','epsg:32633')

        start=transformer.transform(origin_dest_tuple[1],origin_dest_tuple[0])
        dest=transformer.transform(origin_dest_tuple[3],origin_dest_tuple[2])
        path=LineString([(start[0],start[1]),(dest[0],dest[1])])
        length=gen_path_through_constrained(path,self.con_airspace,self.G_undirected,self.border_node_gdf, self.border_node_rtree,self.node_gdf, self.node_rtree)
        return length
        
        





def gen_path_through_constrained(random_path, con_airspace, G,border_node_gdf, border_node_rtree,node_gdf, node_rtree):
    """
    Generate a path that does not violate the constrained airspace.

    First, it checks if there are any intersections with constrained
    airspace. If there are none, then it returns the original path.

    If there are intersections, then the function finds the entry
    and exit points and divides the path into a front path and a
    back path.

    The front path is from the origin to the entry point
    of constrained airspace. The back path is from the exit point
    of constrained airspace to the destination.

    The entry and exit points are nodes of the constrained airspace.

    With these nodes, osmnx finds the shortest path through the constrained
    airspace.

    The last thing is to merge the front path, path through constrained airspace,
    and back path into one path.

    Parameters
    ----------
    random_path : shapely.geometry.LineString
        The random path that does not respect the constrained airspace.
    con_airspace : geopandas.GeoDataFrame
        The constrained airspace.
    G : networkx.MultiGraph
        The graph of the constrained airspace


    Returns
    -------
    merged_path : shapely.geometry.LineString
        The full path that does not violate the constrained airspace.

    """
    # get the airspace polygon
    con_airspace_polygon = con_airspace.geometry.values[0]
    start_point=Point(list(random_path.coords)[0][0],list(random_path.coords)[0][1])
    dest_point=Point(list(random_path.coords)[1][0],list(random_path.coords)[1][1])

    # check if the path intersects the airspace. If not, return the path
    if not con_airspace_polygon.intersects(random_path):
        #print("open")
        return random_path.length



    if con_airspace_polygon.contains(random_path):
        #print("constar")
 
        first_node=list(node_rtree.nearest(list(random_path.coords)[0], 1))[0]
        last_node=list(node_rtree.nearest(list(random_path.coords)[1], 1))[0]
        # find a path from the front node to the back node in constrained airspace
        const_route = ox.shortest_path(G, first_node, last_node)

        # get lat lon from osm route
        lats_c, lons_c, line_gdf = get_lat_lon_from_osm_route(G, const_route)


        # convert to epsg 32633
        line_gdf = line_gdf.to_crs(epsg=32633)
        const_path = line_gdf.geometry.values[0]
        const_path = round_geometry(const_path)

        const_path_list=list(const_path.coords)

        const_path_list.insert(0,tuple(start_point.coords)[0])
        const_path_list.append(tuple(dest_point.coords)[0])

        const_path=LineString(const_path_list)

        return const_path.length


    if con_airspace_polygon.contains(start_point):
        #print("start_cosnst")
 
        first_node=list(node_rtree.nearest(list(random_path.coords)[0], 1))[0]

        back_path, last_node = split_path(random_path,border_node_gdf, border_node_rtree,con_airspace_polygon, "back")

        back_path = round_geometry(back_path)
        

        # find a path from the front node to the back node in constrained airspace
        const_route = ox.shortest_path(G, first_node, last_node)
        if len(const_route)==1:
            lon = np.array(G.nodes[const_route[0]]["x"])
            lat = np.array(G.nodes[const_route[0]]["y"])
            transformer = Transformer.from_crs('epsg:4326','epsg:32633')
            p=transformer.transform(lat,lon)
            back_path_list=list(back_path.coords)
            back_path_list.insert(0,(p[0],p[1]))
            back_path_list.insert(0,tuple(start_point.coords)[0])
            back_path=LineString(back_path_list)
            return back_path.length
        # get lat lon from osm route
        lats_c, lons_c, line_gdf = get_lat_lon_from_osm_route(G, const_route)

        # convert to epsg 32633
        line_gdf = line_gdf.to_crs(epsg=32633)
        const_path = line_gdf.geometry.values[0]
        const_path = round_geometry(const_path)

        const_path_list=list(const_path.coords)
        const_path_list.insert(0,tuple(start_point.coords)[0])
        const_path=LineString(const_path_list)

        back_path_list=list(back_path.coords)
        merged_list=const_path_list+back_path_list
        merged_path=LineString(merged_list)
        return merged_path.length

    if con_airspace_polygon.contains(dest_point):
        #print("Dest_const")
 
        last_node=list(node_rtree.nearest(list(random_path.coords)[1], 1))[0]

        front_path, first_node = split_path(
        random_path,
        border_node_gdf,
        border_node_rtree,
        con_airspace_polygon,
        "front",
        )

        front_path = round_geometry(front_path)       
        

        # find a path from the front node to the back node in constrained airspace
        const_route = ox.shortest_path(G, first_node, last_node)
        
        if len(const_route)==1:
            #print("only front")
            lon = np.array(G.nodes[const_route[0]]["x"])
            lat = np.array(G.nodes[const_route[0]]["y"])
            transformer = Transformer.from_crs('epsg:4326','epsg:32633')
            p=transformer.transform(lat,lon)
            front_path_list=list(front_path.coords)
            front_path_list.append((p[0],p[1]))
            front_path_list.append(tuple(dest_point.coords)[0])
            front_path=LineString(front_path_list)

            return front_path.length
        # get lat lon from osm route
        lats_c, lons_c, line_gdf = get_lat_lon_from_osm_route(G, const_route)

        # convert to epsg 32633
        line_gdf = line_gdf.to_crs(epsg=32633)
        const_path = line_gdf.geometry.values[0]
        const_path = round_geometry(const_path)
        const_path_list=list(const_path.coords)
        const_path_list.append(tuple(dest_point.coords)[0])
        front_path_list = round_geometry(front_path)
        merged_list=front_path_list+const_path_list
        merged_path=LineString(merged_list)
        return merged_path.length

    # split the path into a front and back segments that connect to a constrained airspace polygon
    #print("open-const")
    front_path, first_node = split_path(
        random_path,
        border_node_gdf,
        border_node_rtree,
        con_airspace_polygon,
        "front",
    )
    back_path, last_node = split_path(
        random_path,
        border_node_gdf,
        border_node_rtree,
        con_airspace_polygon,
        "back",
    )

    # round geometry to avoid floating point errors
    front_path = round_geometry(front_path)
    back_path = round_geometry(back_path)

    # find a path from the front node to the back node in constrained airspace
    const_route = ox.shortest_path(G, first_node, last_node)
    
    if len(const_route)==1:
        #print("ccccccc")
        lon = np.array(G.nodes[const_route[0]]["x"])
        lat = np.array(G.nodes[const_route[0]]["y"])
        transformer = Transformer.from_crs('epsg:4326','epsg:32633')
        p=transformer.transform(lat,lon)

        front_path_list=list(front_path.coords)
        back_path_list=list(back_path.coords)
        front_path_list.append((p[0],p[1]))
        merged_list=front_path_list+back_path_list
        merged_path=LineString(merged_list)
 

        return merged_path.length  

    # get lat lon from osm route
    lats_c, lons_c, line_gdf = get_lat_lon_from_osm_route(G, const_route)

    # convert to epsg 32633
    line_gdf = line_gdf.to_crs(epsg=32633)
    const_path = line_gdf.geometry.values[0]
    const_path = round_geometry(const_path)

    # merge the front and back paths
    merged_path = linemerge([front_path, const_path, back_path])

    return merged_path.length    
        
"""HELPER FUNCTIONS BELOW"""
def get_lat_lon_from_osm_route(G, route):
    """
    Get lat and lon from an osmnx route (list of nodes) and nx.MultGraph.
    The function returns two numpy arrays with the lat and lon of route.
    Also return a GeoDataFrame with the lat and lon of the route as a
    linestring.

    Parameters
    ----------
    G : nx.MultiGraph
        Graph to get lat and lon from. Graph should be built
        with osmnx.get_undirected.

    route : list
        List of nodes to build edge and to get lat lon from

    Returns
    -------
    lat : numpy.ndarray
        Array with latitudes of route

    lon : numpy.ndarray
        Array with longitudes of route

    route_gdf : geopandas.GeoDataFrame
        GeoDataFrame with lat and lon of route as a linestring.
    """
    # add first node to route
    lons = np.array(G.nodes[route[0]]["x"])
    lats = np.array(G.nodes[route[0]]["y"])

    # loop through the rest for loop only adds from second point of edge
    for u, v in zip(route[:-1], route[1:]):
        # if there are parallel edges, select the shortest in length
        data = list(G.get_edge_data(u, v).values())[0]

        # extract coords from linestring
        xs, ys = data["geometry"].xy

        # Check if geometry of edge is in correct order
        if G.nodes[u]["x"] != data["geometry"].coords[0][0]:

            # flip if in wrong order
            xs = xs[::-1]
            ys = ys[::-1]

        # only add from the second point of linestring
        lons = np.append(lons, xs[1:])
        lats = np.append(lats, ys[1:])

    # make a linestring from the coords
    linestring = LineString(zip(lons, lats))

    # make into a gdf
    line_gdf = gpd.GeoDataFrame(geometry=[linestring], crs="epsg:4326")

    return lats, lons, line_gdf

 

def find_border_nodes(airspace_gdf, G):
    """
    Finds the border nodes of the the constrained airspace polygon.
    The funciton needs a geopandas dataframe with the airspace polygon and
    a networkx graph. The function returns gdf with the border nodes. Also
    returns an rtree with the border nodes.

    Parameters
    ----------
    airspace_gdf : geopandas.GeoDataFrame
        GeoDataFrame with airspace polygon.

    G : nx.MultiGraph
        Graph to find border nodes of. Graph should be built
        with osmnx.get_undirected.

    Returns
    -------
    border_nodes_gdf : geopandas.GeoDataFrame
        GeoDataFrame with border nodes.

    border_nodes_rtree : rtree.index.Index
        Rtree with border nodes.
    """

    # Convert the crs gdf to a projected crs
    airspace_gdf = airspace_gdf.to_crs(epsg=32633)

    # get node_gdf and project cs
    nodes = ox.graph_to_gdfs(G, edges=False)
    nodes = nodes.to_crs(epsg=32633)

    # buffer the airspace polygon inside -0.5 meters
    airspace_buff = airspace_gdf.buffer(-0.5)

    # see which nodes are inside the airspace border
    nodes_inside = nodes["geometry"].apply(lambda x: x.within(airspace_buff.values[0]))

    # select border nodes based on when nodes_inside is False
    border_nodes_gdf = nodes[nodes_inside == False]

    # crete an rtree
    border_nodes_rtree = index.Index()

    # loop through the border nodes and add them to the rtree
    for idx, node in border_nodes_gdf.iterrows():
        border_nodes_rtree.insert(idx, node.geometry.bounds)
    return border_nodes_gdf, border_nodes_rtree

def build_rtree_nodes(airspace_gdf, G):
    """
    Finds the border nodes of the the constrained airspace polygon.
    The funciton needs a geopandas dataframe with the airspace polygon and
    a networkx graph. The function returns gdf with the border nodes. Also
    returns an rtree with the border nodes.

    Parameters
    ----------
    airspace_gdf : geopandas.GeoDataFrame
        GeoDataFrame with airspace polygon.

    G : nx.MultiGraph
        Graph to find border nodes of. Graph should be built
        with osmnx.get_undirected.

    Returns
    -------

    border_nodes_gdf : geopandas.GeoDataFrame
        GeoDataFrame with border nodes.

    border_nodes_rtree : rtree.index.Index
        Rtree with border nodes.
    """

    # Convert the crs gdf to a projected crs
    airspace_gdf = airspace_gdf.to_crs(epsg=32633)

    # get node_gdf and project cs
    nodes = ox.graph_to_gdfs(G, edges=False)
    nodes = nodes.to_crs(epsg=32633)


    # crete an rtree
    border_nodes_rtree = index.Index()

    # loop through the border nodes and add them to the rtree
    for idx, node in nodes.iterrows():
        border_nodes_rtree.insert(idx, node.geometry.bounds)
    return nodes, border_nodes_rtree

def round_geometry(geometry, round_to=1):
    """
    Round the coordinates of a shapely geometry to the given precision.

    Parameters
    ----------
    geometry : shapely.geometry.base.BaseGeometry
        The geometry to round.
    round_to : float, optional
        The precision to round to.

    Returns
    -------
    shapely.geometry.base.BaseGeometry
        The rounded geometry.
    """
    if isinstance(geometry, Point):
        return Point(round(geometry.x, round_to), round(geometry.y, round_to))
    elif isinstance(geometry, LineString):
        return LineString(
            [
                (round(coord[0], round_to), round(coord[1], round_to))
                for coord in geometry.coords
            ]
        )
    else:
        raise ValueError("Unsupported geometry type: {}".format(type(geometry)))
def split_path(
    segment,
    border_node_gdf,
    node_rtree,
    con_airspace_polygon,
    loc="front",
):
    """
    Split the path into a front and back segments that connect to a constrained airspace polygon.
    If there are more than two intersections in a given segment, then the function
    finds the closest point to the point to connect to decide which point to use.

    Parameters
    ----------
    segments : list of shapely.geometry.LineString
        The individual segments of the path.
    intersecting_idx : int
        The index of the segment that intersects with the constrained airspace.
    border_node_gdf : geopandas.GeoDataFrame
        The border nodes of the constrained airspace.
    node_rtree : rtree.index.Index
        The rtree of the border nodes of the constrained airspace.
    con_airspace_polygon : shapely.geometry.Polygon
        The constrained airspace polygon.
    loc : str
        The location of the split. Either 'front' or 'back' path.

    Returns
    -------
    shapely.geometry.LineString
        The front or back path.
    int
        The node id of border_nodes_gdf that the front or back path connects to.
    """

    if loc == "front":

        point_to_connect = Point(segment.coords[0])

        # find intersecting point of first intersection idx
        intersecting_line = segment

        intersecting_point = con_airspace_polygon.intersection(
            intersecting_line
        )

        # check if intersecting points are Point or MultiPoint
        intersecting_point = filter_multipoint_geometry(
            intersecting_point, point_to_connect
        )

        # find the nearest node to the intersecting points
        intersecting_node = list(node_rtree.nearest(intersecting_point.bounds, 1))[0]

        # extract geometry from border_node_gdf
        intersecting_node_geom = border_node_gdf.loc[intersecting_node]["geometry"]

        # create a line segment from the first_point_to_connect to the first intersecting node
        new_segment_line = LineString([point_to_connect, intersecting_node_geom])

        # extend the first legs with the first segment line
        connected_path = new_segment_line#linemerge([remain_path, new_segment_line])

    if loc == "back":
        
        point_to_connect = Point(segment.coords[-1])

        # find intersecting point of first intersection idx
        intersecting_line = segment
        intersecting_point = con_airspace_polygon.boundary.intersection(
            intersecting_line
        )

        # check if intersecting points are Point or MultiPoint
        intersecting_point = filter_multipoint_geometry(
            intersecting_point, point_to_connect
        )

        # find the nearest node to the intersecting points
        intersecting_node = list(node_rtree.nearest(intersecting_point.bounds, 1))[0]

        # extract geometry from border_node_gdf
        intersecting_node_geom = border_node_gdf.loc[intersecting_node]["geometry"]

        # create a line segment from the first_point_to_connect to the first intersecting node
        new_segment_line = LineString([intersecting_node_geom, point_to_connect])
        # extend the first legs with the first segment line
        connected_path = new_segment_line#linemerge([new_segment_line, remain_path])

    return connected_path, intersecting_node       



def filter_multipoint_geometry(intersecting_point, points_to_connect):
    """
    Filter the multipoint geometry to only keep the point that is closest to the points_to_connect
    Parameters
    ----------
    intersecting_point : shapely.geometry.multipoint.MultiPoint
        The multipoint geometry that intersects with the airspace
    points_to_connect : shapely.geometry.point.Point
        The point that is being connected to the intersecting_point
    Returns
    -------
    shapely.geometry.point.Point
        The point that is closest to the points_to_connect
    """
    # filter out MultiPoint geometries
    if isinstance(intersecting_point, MultiPoint):

        # extract single points
        list_points = list(intersecting_point)

        # check which point in the list is closest to first_point_to_connect
        intersecting_point = min(
            list_points, key=lambda x: x.distance(points_to_connect)
        )

    return intersecting_point 
