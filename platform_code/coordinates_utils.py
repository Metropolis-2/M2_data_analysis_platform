import dill
import shapely
from pyproj import Transformer


class Utils():

    def __init__(self):
        input_file = open("../../data/constrained_poly.dill", 'rb')
        constrained_poly = dill.load(input_file)
        self.constrained_poly = shapely.geometry.Polygon(constrained_poly)

    # Function to check if a point (lon,lat) is in constrained airspace
    # returns true if point is in contsrained
    # returns false if poitn is in open
    def inConstrained(self, point):
        transformer = Transformer.from_crs('epsg:4326', 'epsg:32633')
        p = transformer.transform(point[1], point[0])
        p = shapely.geometry.Point(p[0], p[1])
        return self.constrained_poly.contains(p)

    # Function to check if the linesegment connecting point1 (lon1,lat1) and point2(lon2,lat2) intersects with the constrained airspace
    # returns true if it intersects, otehrwise it returns false
    def lineIntersects_Constarined(self, point1, point2):
        transformer = Transformer.from_crs('epsg:4326', 'epsg:32633')
        p1 = transformer.transform(point1[1], point1[0])
        p2 = transformer.transform(point2[1], point2[0])
        lineSegment = shapely.geometry.LineString(
            [shapely.geometry.Point(p1[0], p1[1]), shapely.geometry.Point(p2[0], p2[1])])
        return lineSegment.intersects(self.constrained_poly)
