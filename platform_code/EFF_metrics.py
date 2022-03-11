# -*- coding: utf-8 -*-
"""
Created on Thu Feb 24 10:49:23 2022

@author: jpedrero
"""

import utils
from platform_code.ctes import Ctes


class EFF_metrics():
    
    def __init__(self, flst_log_dataframe):
        self.ctes = Ctes()
        self.utils = utils.Utils()
        self.flst_log_dataframe = flst_log_dataframe

        self.rec_snd_points = dict([(str(row["ACID"]), [(row["Origin_LAT"], row["Origin_LON"]), (row["Dest_LAT"], row["Dest_LON"])]) for row in self.flst_log_dataframe.select("ACID", "Origin_LAT", "Origin_LON", "Dest_LAT", "Dest_LON").collect()])
        self.vehicle_types = dict([(str(row["ACID"]), str(row["Aircraft_type"])) for row in self.flst_log_dataframe.select("ACID", "Aircraft_type").collect()])
        return
        
    def evaluate_EFF_metric(self, metric_id):
        if(metric_id == 1):
            return self.compute_EFF1_metric()
        elif(metric_id == 2):
            return self.compute_EFF2_metric()
        elif(metric_id == 3):
            return self.compute_EFF3_metric()
        elif(metric_id == 4):
            return self.compute_EFF4_metric()
        elif(metric_id == 5):
            return self.compute_EFF5_metric()
        elif(metric_id == 6):
            return self.compute_EFF6_metric()
        # elif(metric_id == 7):
        #     return self.compute_EFF7_metric()
        # elif(metric_id == 8):
        #     return self.compute_EFF8_metric()
        else:
            print("No valid metric")
    
    def compute_EFF1_metric(self):
        '''
        EFF-1: Horizontal distance route efficiency
        (Ratio representing the length of the ideal horizontal route to the actual horizontal route)
        '''
        # Calculate length_ideal_horizontal_route
        ideal_route_dist = {} #route_path 2D distance between sending and receiving points for each vehicle ACID
        for key, value in self.rec_snd_points.items():
            snd_point = value[0]
            rcv_point = value[1]
            route_dist = self.utils.distCoords(snd_point, rcv_point)
            ideal_route_dist[key] = route_dist

        # Calculate length_actual_horizontal_route
        route_dist = dict([(str(row["ACID"]), float(row["2D_dist"])) for row in self.flst_log_dataframe.select("ACID", "2D_dist").collect()])

        #Calculate ratio between ideal and actual lenght route
        lenght_ratios = {}
        for key, value in route_dist.items():
            ratio = float(ideal_route_dist[key] / route_dist[key])
            lenght_ratios[key] = ratio

        result = lenght_ratios
        return result
    
    def compute_EFF2_metric(self):
        '''
        EFF-2: Vertical distance route efficiency
        (Ratio representing the length of the ideal vertical route to the actual vertical route)
        '''

        # Calculate lenght_ideal_vertical_route
        ideal_vertical_dist = self.utils.feetToMeter(2*self.ctes.HEIGHT_LOW_LAYER)

        #Calculate lenght_actual_vertical_route (include ascensing_distances and descending_distances)
        alt_dist = dict([(str(row["ACID"]), float(row["ALT_dist"])) for row in self.flst_log_dataframe.select("ACID", "ALT_dist").collect()])

        alt_dist_ratios = {}
        for key, value in alt_dist.items():
            if(alt_dist[key] >0):
                alt_dist_ratios[key] = ideal_vertical_dist/alt_dist[key]
            else: #TODO: Are there cases where the ALT-dist of the dataframe is zero?
                alt_dist_ratios[key] = -1

        result = alt_dist_ratios
        return result
    
    def compute_EFF3_metric(self):
        '''
        EFF-3: Ascending route efficiency
        (Ratio representing the length of the ascending distance in the ideal route to the length of the ascending distance of the actual route)
        '''

        heigh_lowest_layer = self.utils.feetToMeter(self.ctes.HEIGHT_LOW_LAYER)
        # TODO: ascensing_distance is is half of ALT in the dataframe?
        alt_dist_ascending = dict([(str(row["ACID"]), float(row["ALT_dist"])/2) for row in self.flst_log_dataframe.select("ACID", "ALT_dist").collect()])

        alt_dist_ascending_ratios = {}
        for key, value in alt_dist_ascending.items():
            if(alt_dist_ascending[key] >0):
                alt_dist_ascending_ratios[key] = heigh_lowest_layer/alt_dist_ascending[key]
            else: #TODO: Are there cases where the ALT-dist of the dataframe is zero?
                alt_dist_ascending_ratios[key] = -1

        result = alt_dist_ascending_ratios
        return result
    
    def compute_EFF4_metric(self):
        '''
        EFF-4: 3D distance route efficiency
        (Ratio representing the 3D length of the ideal route to the 3D length of the actual route)
        '''

        ideal_route_dist = {} #route_path 2D distance between sending and receiving points for each vehicle ACID
        length_ideal_3D_route = {} #ideal_route_dist + vertical_distances (takeoff and landing events)
        for key, value in self.rec_snd_points.items():
            snd_point = value[0]
            rcv_point = value[1]
            route_dist = self.utils.distCoords(snd_point, rcv_point)
            ideal_route_dist[key] = route_dist
            length_ideal_3D_route[key] = route_dist + 2*self.ctes.HEIGHT_LOW_LAYER

        length_actual_3D_route = dict([(str(row["ACID"]), float(row["3D_dist"])) for row in self.flst_log_dataframe.select("ACID", "3D_dist").collect()])

        distance_route_efficiency = {}
        for key, value in length_actual_3D_route.items():
            if(length_actual_3D_route[key] > 0):
                distance_route_efficiency[key] = length_ideal_3D_route[key]/length_actual_3D_route[key]
            else: #TODO: Are there cases where the 3D_dist of the dataframe is zero?
                distance_route_efficiency[key] = -1

        result = distance_route_efficiency
        return result
    
    def compute_EFF5_metric(self):
        '''
        EFF-5: Route duration efficiency
        (Ratio representing the time duration of the ideal route to the time duration of the actual route)
        '''

        #TODO: Clarify parameters SPAWN_time, DEL_time and FLIGHT_time of dataframe FLST_LOG

        time_duration_ideal_route = {}  #ideal flight_time based on ideal route_dist and vehicle_speed
        for key, value in self.rec_snd_points.items():
            snd_point = value[0]
            rcv_point = value[1]
            route_dist = self.utils.distCoords(snd_point, rcv_point)
            vehicle_speed = self.utils.getVehicleMaxSpeed(self.vehicle_types[key])
            time_duration_ideal_route[key] = route_dist/vehicle_speed

        #TODO: in time_vertiport_arrival how estimate when it reaches its destination vertiport? DEL_time when it hits the ground?
        time_vertiport_arrival = dict([(str(row["ACID"]), float(row["DEL_time"])) for row in self.flst_log_dataframe.select("ACID", "DEL_time").collect()])
        #TODO: SPAWN_time is equivalent to take_off time?
        time_vertiport_departure = dict([(str(row["ACID"]), float(row["SPAWN_time"])) for row in self.flst_log_dataframe.select("ACID", "SPAWN_time").collect()])
        time_duration_actual_route = {}

        for key, value in time_vertiport_arrival.items():
            time_duration_actual_route[key] = time_vertiport_arrival[key] - time_vertiport_departure[key]

        route_duration_efficiency = {}
        for key, value in time_duration_actual_route.items():
            if(time_duration_actual_route[key] >0):
                route_duration_efficiency[key] = time_duration_ideal_route[key]/time_duration_actual_route[key]
            else: #TODO: Are there cases where the route_duration_efficiency is zero?
                route_duration_efficiency[key] = -1

        result = route_duration_efficiency
        return result

    def compute_EFF6_metric(self):
        '''
        EFF-6: Departure delay
        (Time duration from the planned departure time until the actual departure time of the aircraft)
        '''
        time_planned_departure = dict([(str(row["ACID"]), str(row["Baseline_deparure_time"])) for row in self.flst_log_dataframe.select("ACID", "Baseline_deparure_time").collect()])
        #TODO: time_actual_departure == SPAWN_time == take_off time??
        time_actual_departure = dict([(str(row["ACID"]), float(row["SPAWN_time"])) for row in self.flst_log_dataframe.select("ACID", "SPAWN_time").collect()])
        departure_delay = {}
        for key, value in time_actual_departure.items():
            departure_delay[key] = float(time_actual_departure[key]) - float(time_planned_departure[key])
        result = departure_delay
        return result
    
    # def compute_EFF7_metric(self):
    #     '''
    #     EFF-7: Departure sequence delay
    #     (Time duration from the time that the aircraft starts its flight to the time that the aircraft takes off from the origin point)
    #     '''
    #     #TODO: How to estimate when the drone starts to fly? FLIGHT_time is the time between takeoff and landing directly? (takeoff == SPAWN_time) and (landing == DEL_time)??
    #     time_actual_departure = dict([(str(row["ACID"]), float(row["SPAWN_time"])) for row in self.flst_log_dataframe.select("ACID", "SPAWN_time").collect()])
    #     time_takeoff = dict([(str(row["ACID"]), abs(float(row["SPAWN_time"])-float(row["FLIGHT_time"]))) for row in self.flst_log_dataframe.select("ACID", "SPAWN_time", "FLIGHT_time").collect()])
    #     departure_sequence_delay = {}
    #     for key, value in time_actual_departure.items():
    #         departure_sequence_delay[key] = abs(time_actual_departure[key] - time_takeoff[key])
    #     result = departure_sequence_delay
    #     return result
    #
    # def compute_EFF8_metric(self):
    #     '''
    #     EFF-8: Arrival sequence delay
    #     (Time duration from the time that the aircraft arrived at the destination vertiport to the time that the aircraft landed at the destination point)
    #     '''
    #     #TODO: How to know the point of reached_landing_vertiport?? (takeoff == SPAWN_time) and (landing == DEL_time)??
    #
    #     time_reached_landing_vertiport = dict([(str(row["ACID"]), float(row["DEL_time"])) for row in self.flst_log_dataframe.select("ACID", "DEL_time").collect()])
    #     time_landing = dict([(str(row["ACID"]), abs(float(row["DEL_time"])-float(row["FLIGHT_time"]))) for row in self.flst_log_dataframe.select("ACID", "DEL_time", "FLIGHT_time").collect()])
    #     departure_sequence_delay = {}
    #     for key, value in time_reached_landing_vertiport.items():
    #         departure_sequence_delay[key] = abs(time_reached_landing_vertiport[key] - time_landing[key])
    #     result = departure_sequence_delay
    #     return result