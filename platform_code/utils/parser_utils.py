import json
from pathlib import Path
from typing import Tuple, List, Union, Dict

import geopy.distance
from loguru import logger
from pyproj import Transformer
from pyspark.sql import DataFrame
from pyspark.sql.functions import monotonically_increasing_id, col, udf
from pyspark.sql.types import StructType, StructField, DoubleType

from parse.parser_constants import CONCEPTS, LINE_COUNT, FEET_TO_METERS_SCALE, UNCERTAINTIES
from schemas.tables_attributes import LATITUDE, LONGITUDE, VERTICAL_SPEED, CRUISING_SPEED
from utils.config import settings


def get_density_from_fp_int_string(fp_intention: List[str]) -> Tuple[str, List[str]]:
    """ Gets the scenario density and returns it together with the rest
    of string yet to be parsed.

    :param fp_intention: flight intention split string.
    :return: density and the rest of the elements of the split to be parsed.
    """
    if fp_intention[0] == 'very':
        logger.trace('The density is very low. Check: {}_{}.', *fp_intention[:2])
        density = "very_low"
        fp_intention = fp_intention[2:]
    else:
        density = fp_intention[0]
        fp_intention = fp_intention[1:]

    return density, fp_intention


def parse_fp_int_string(fp_intention: List[str]) -> str:
    """ Parses the flight intention split string present in the flight
    intention and log files in order to be used as key in the
    dataframes.

    :param fp_intention: flight intention string part of the file name.
     For example: ['very', 'low', '40', '8']
    :return: string with the elements joint by underscores.
    """
    # First check the density
    density, fp_intention = get_density_from_fp_int_string(fp_intention)
    return f'{density}_{"_".join(fp_intention)}'


def get_fp_int_key_from_scenario_name(scenario_name: str) -> str:
    """ Method to get the flight intention file name from the scenario name.

    :param scenario_name: scenario name.
    :return: the name of the flight intention related with the scenario.
    """
    # 1: discard the concept
    scenario_name_split = scenario_name.split('_')[1:]

    # The flight intentions do not have the uncertainty info in the name
    # Therefore, the uncertainty part is removed.
    if scenario_name_split[-1] in UNCERTAINTIES:
        scenario_name_split = scenario_name_split[:-1]

    fp_int_string = parse_fp_int_string(scenario_name_split)
    logger.debug('Matched scenario name: {} with flight intention: {}.',
                 scenario_name, fp_int_string)
    return fp_int_string


def get_scenario_data_from_fp_int(file_name: Path) -> str:
    """ Generates the scenario name for the file to process.

    :param file_name: Path to the log file to parse.
    :return: the scenario name generated from the file.
    """
    logger.trace('Obtaining the scenario data for file: {}.', file_name)

    fp_intention = file_name.stem.split("_")[2:]
    scenario_data = parse_fp_int_string(fp_intention)

    logger.debug('Scenario data string obtained: {}.', scenario_data)
    return scenario_data


def build_scenario_name(file_name: Path) -> str:
    """ Generates the scenario name for the file to process.

    :param file_name: Path to the log file to parse.
    :return: the scenario name generated from the file.
    """
    logger.trace('Obtaining the scenario name for file: {}.', file_name)
    concept = file_name.parent.name
    concept_index = str(CONCEPTS.index(concept) + 1)

    # 3: removes the log name and the 'Flight_intention' string
    # -2: removes the timestamp
    fp_intention = file_name.stem.split("_")[3:-2]
    scenario_data = parse_fp_int_string(fp_intention)

    scenario_name = f'{concept_index}_{scenario_data}'
    logger.debug('Scenario name obtained: {}.', scenario_name)
    return scenario_name


def add_dataframe_counter(dataframe: DataFrame, counter_name: str) -> DataFrame:
    """ Adds a counter for each of the rows in the dataframe.

    :param dataframe: dataframe to add the counter.
    :param counter_name: attribute name of the counter.
    :return: dataframe with the counter added.
    """
    return dataframe.withColumn(counter_name, monotonically_increasing_id())


def remove_commented_log_lines(dataframe: DataFrame) -> DataFrame:
    """ Function that removes from the dataframe the 9 lines of
    comments that exist in each log file.

    :param dataframe: loaded dataframe from the log file.
    :return: filtered dataframe without the comment lines.
    """
    dataframe = add_dataframe_counter(dataframe, LINE_COUNT)
    dataframe = dataframe.filter(col(LINE_COUNT) >= 9)
    return dataframe.drop(col(LINE_COUNT))


def convert_feet_to_meters(dataframe: DataFrame, column_name: str) -> DataFrame:
    """ Converts a given column that contains the altitude in feets to meters.

    :param dataframe: dataframe to perform the transformation.
    :param column_name: attribute name of the column.
    :return: dataframe with the altitude column in meters.
    """
    return dataframe.withColumn(column_name, col(column_name) * FEET_TO_METERS_SCALE)


def get_drone_avg_speed(drone_model: str) -> float:
    """ Checks in the configuration the average speed of the drone model.
    If the drone is not found, 10 is returned.

    :param drone_model: name of the drone model. For example, MP20.
    :return: speed in meters per second.
    """
    if drone_model:
        speed = settings.get(f'{drone_model}.avg_speed', 10.)

    else:
        logger.warning('No drone model value was retrieved. Returning speed 0.')
        speed = 0

    return float(speed)


def get_drone_vertical_speed(drone_model: str) -> float:
    """ Checks in the configuration the vertical speed of the drone model.
    If the drone is not found, 5 is returned.

    :param drone_model: name of the drone model. For example, MP20.
    :return: speed in meters per second.
    """
    if drone_model:
        speed = settings.get(f'{drone_model}.vertical_speed', 5.)

    else:
        logger.warning('No drone model value was retrieved. Returning speed 0.')
        speed = 0

    return float(speed)


@udf(returnType=StructType([
    StructField(CRUISING_SPEED, DoubleType(), False),
    StructField(VERTICAL_SPEED, DoubleType(), False)
]))
def get_drone_speed(drone_model: str) -> Tuple[float, float]:
    """ Checks in the configuration the average and vertical
    speeds of the drone model.
    If the drone is not found, the speeds are set to 0.

    :param drone_model: name of the drone model. For example, MP20.
    :return: avg_speed in meters per second.
    """
    avg_speed = get_drone_avg_speed(drone_model)
    vertical_speed = get_drone_vertical_speed(drone_model)
    return avg_speed, vertical_speed


@udf(returnType=StructType([
    StructField(LATITUDE, DoubleType(), False),
    StructField(LONGITUDE, DoubleType(), False)
]))
def transform_location(latitude: float, longitude: float) -> Tuple[float, float]:
    """ Transform the coordinates from the original coordinate reference system
    to another one. The coordinates systems to used can be set in the
    setting files.

    :param latitude: latitude in the origin coordinate system.
    :param longitude: longitude in the origin coordinate system.
    :return: struct with two columns with the latitude and longitude
     in the desired coordinate system.
    """
    # Change the crs
    transformer = Transformer.from_crs(settings.crs.origin, settings.crs.desired)
    p = transformer.transform(latitude, longitude)
    transformed_latitude = p[0]
    transformed_longitude = p[1]
    return transformed_latitude, transformed_longitude


@udf
def get_coordinates_distance(origin_latitude: float, origin_longitude: float,
                             destination_latitude: float, destination_longitude: float) -> float:
    """ Calculates the distance in meters between two world coordinates.

    :param origin_latitude: origin latitude point.
    :param origin_longitude: origin longitude point.
    :param destination_latitude: destination latitude point.
    :param destination_longitude: destination longitude point.
    :return: distance in meters.
    """
    origin_tuple = (origin_latitude, origin_longitude)
    destination_tuple = (destination_latitude, destination_longitude)
    return geopy.distance.distance(origin_tuple, destination_tuple).m


def load_interest_points(file_path: Union[str, Path]) -> Dict[str, List]:
    """ Parses the JSON file with the interest points. The returned
    object is a dictionary with the name of the point as the key and
    a list as value with the latitude, longitude and the type of point.

    The point could be of type:
     - 'constained': if it is inside the city area.
     - 'border': if it is the border between the city area and the open area.
     - 'open': if it is in the open area.

    :param file_path: path to the interest points JSON file.
    :return: dictionary with the point name as key and
     [latitude, longitude, type] as value.
    """
    with open(file_path) as f:
        interest_points = json.load(f)
    return interest_points
