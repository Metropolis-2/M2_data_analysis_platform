# -*- coding: utf-8 -*-
"""
Created on Thu Feb 24 10:49:23 2022

@author: jpedrero
"""
import utils

class CAP_metrics():
    
    def __init__(self, flst_log_dataframe, loslog_dataframe):
        self.utils = utils.Utils()
        self.flst_log_dataframe = flst_log_dataframe
        self.loslog_dataframe = loslog_dataframe

        self.delay_times = list()
        self.rec_snd_points = dict([(str(row["ACID"]), [(row["Origin_LAT"],row["Origin_LON"]), (row["Dest_LAT"],row["Dest_LON"])]) for row in self.flst_log_dataframe.select("ACID", "Origin_LAT","Origin_LON", "Dest_LAT","Dest_LON").collect()])
        self.vehicle_types = dict([(str(row["ACID"]), str(row["Aircraft_type"])) for row in self.flst_log_dataframe.select("ACID", "Aircraft_type").collect()])
        self.flight_times = dict([(str(row["ACID"]), float(row["FLIGHT_time"])) for row in self.flst_log_dataframe.select("ACID", "FLIGHT_time").collect()])
        return
        
    def evaluate_CAP_metric(self, metric_id,):
        self.delay_times.clear() #CRITICAL POINT
        if(metric_id == 1):
            return self.compute_CAP1_metric()
        elif(metric_id == 2):
            return self.compute_CAP2_metric()
        elif(metric_id == 3):
            return self.compute_CAP3_metric()
        elif(metric_id == 4):
            return self.compute_CAP4_metric()
        else:
            print("No valid metric")
    
    def compute_CAP1_metric(self):
        '''
        CAP-1: Average demand delay
        (Measured as an arithmetic mean of delay of all flight intentions,
        where delay for each flight intention is calculated as the difference between realized arrival time
        and ideal expected arrival time.
        Ideal expected arrival time is computed as arrival time of the fastest trajectory
        from origin to destination departing at the requested time as if a user were alone in the system,
        respecting all concept airspace rules. Realized arrival time comes directly from the simulation)
        '''
        #Calculate path 2D distance between sending and receiving points for each ACID
        ideal_route_dist = {} #route_path 2D distance between sending and receiving points for each vehicle ACID
        ideal_flight_time = {} #ideal flight_time based on ideal route_dist and vehicle_speed
        for key, value in self.rec_snd_points.items():
            snd_point = value[0]
            rcv_point = value[1]
            route_dist = self.utils.distCoords(snd_point, rcv_point)
            ideal_route_dist[key] = route_dist
            vehicle_speed = self.utils.getVehicleMaxSpeed(self.vehicle_types[key])
            ideal_flight_time[key] = route_dist/vehicle_speed

        for key, value in self.flight_times.items():
            delay = abs(self.flight_times[key] - ideal_flight_time[key])
            self.delay_times.append(delay) #Add to delay list

        result = self.utils.average_statistics(self.delay_times)
        return result
    
    def compute_CAP2_metric(self):
        '''
        CAP-2: Average number of intrusions
        (Number of intrusions per flight intention I.e., a ration between total number of intrusions (SAF-2 indicator)
        and number of flight intentions.Intrusions are situations in which the distance between two aircraft is smaller
        than separation norm of 32 metres horizontally and 25 feet vertically, and is directly computed during the simulation)
        '''

        number_intrusions = self.loslog_dataframe.select("ACID1", "ACID2").count()
        number_fp_intentions = len(self.vehicle_types)
        result = number_intrusions/number_fp_intentions
        return result
    
    def compute_CAP3_metric(self): #TODO: PENDING
        '''
        CAP-3: Additional demand delay
        (Calculated as an increase of the CAP-1 indicator with the introduction of rogue aircraft)
        :return:
        '''
        #TODO: Rogue aircraft??
        return
    
    def compute_CAP4_metric(self): #TODO: PENDING
        '''
        CAP-4: Additional number of intrusions
        (Calculated as an increase of the CAP-2 indicator with the introduction of rogue aircraft)
        '''
        #TODO: Rogue aircraft??
        return
    