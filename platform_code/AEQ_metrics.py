# -*- coding: utf-8 -*-
"""
Created on Thu Feb 24 10:49:23 2022

@author: jpedrero
"""
import utils

class AEQ_metrics():

    # Ctes
    CANCELLATION_DELAY_LIMIT = 60  # 1min
    AUTONOMY_MP20 = 600  # 10min
    AUTONOMY_MP30 = 1200  # 20min
    THRESHOLD_DELAY = 120  # 2min

    def __init__(self, fp_intention_dataframe, flst_log_dataframe):
        self.utils = utils.Utils()
        self.fp_intention_dataframe = fp_intention_dataframe
        self.flst_log_dataframe = flst_log_dataframe

        self.delay_times = list()
        self.rec_snd_points = dict([(str(row["FPLAN_ID_INDEX"]), [eval(row["INITIAL_LOCATION_INDEX"]), eval(row["FINAL_LOCATION_INDEX"])]) for row in self.fp_intention_dataframe.select("FPLAN_ID_INDEX", "INITIAL_LOCATION_INDEX", "FINAL_LOCATION_INDEX").collect()])
        self.vehicle_types = dict([(str(row["FPLAN_ID_INDEX"]), str(row["VEHICLE_INDEX"])) for row in self.fp_intention_dataframe.select("FPLAN_ID_INDEX", "VEHICLE_INDEX").collect()])
        self.flight_times = dict([(str(row["ACID"]), float(row["FLIGHT_time"])) for row in self.flst_log_dataframe.select("ACID", "FLIGHT_time").collect()])
        return
        
    def evaluate_AEQ_metric(self, metric_id):
        self.delay_times.clear()
        if(metric_id == 1):
            return self.compute_AEQ1_metric()
        elif(metric_id == 2):
            return self.compute_AEQ1_1_metric()
        elif(metric_id == 3):
            return self.compute_AEQ2_metric()
        elif(metric_id == 4):
            return self.compute_AEQ2_1_metric()
        elif(metric_id == 5):
            return self.compute_AEQ3_metric()
        elif(metric_id == 6):
            return self.compute_AEQ4_metric()
        elif(metric_id == 7):
            return self.compute_AEQ5_metric()
        elif(metric_id == 8):
            return self.compute_AEQ5_1_metric()
        else:
            print("No valid metric")
    
    def compute_AEQ1_metric(self):
        '''
        AEQ-1: Number of cancelled demands
        (Number of situations when realized arrival time of a given flight intention is greater than ideal expected arrival time
        by more or equal than some given cancellation delay limit that depends on mission type.
        Ideal expected arrival time is computed as arrival time of the fastest trajectory from origin to destination departing
        at the requested time as if a user were alone in the system, respecting all concept airspace rules.
        Realized arrival time comes directly from the simulations)
        '''
        #Calculate path 2D distance between sending and receiving points for each ACID
        ideal_route_dist = {}
        ideal_flight_time = {}
        for key, value in self.rec_snd_points.items():
            snd_point = value[0]
            rcv_point = value[1]
            route_dist = self.utils.distCoords(snd_point, rcv_point)
            ideal_route_dist[key] = route_dist
            vehicle_speed = self.utils.getVehicleSpeed(self.vehicle_types[key])
            ideal_flight_time[key] = route_dist/vehicle_speed

        list_cancelled_demands = list()
        for key, value in self.flight_times.items():
            delay = abs(self.flight_times[key] - ideal_flight_time[key])
            self.delay_times.append(delay)
            if(delay >= self.CANCELLATION_DELAY_LIMIT):
                list_cancelled_demands.append(key)

        self.number_cancelled_demands = len(list_cancelled_demands)
        result = self.number_cancelled_demands
        return result
    
    def compute_AEQ1_1_metric(self):
        '''
        AEQ-1.1 Percentage of cancelled demands
        (Calculated as the ratio of AEQ-1 and the total number of flight intentions in the given scenario)
        '''
        self.compute_AEQ1_metric()
        result = self.number_cancelled_demands / len(self.vehicle_types)
        return result
    
    def compute_AEQ2_metric(self):
        '''
        AEQ-2: Number of inoperative trajectories
        (Number of situations when realized total mission duration is greater than specific drone autonomy.
        Realized trajectories and hence realized total mission duration comes directly from a simulation)
        '''
        list_inoperative_trajectories = list()
        for key, value in self.flight_times.items():
            vehicle_type = self.vehicle_types[key]
            if(vehicle_type == 'MP20'):
                autonomy = self.AUTONOMY_MP20
            else: # vehicle_type == 'MP30'
                autonomy = self.AUTONOMY_MP30

            if(self.flight_times[key] >= autonomy):
                list_inoperative_trajectories.append(key)

        self.number_inoperative_trajectories = len(list_inoperative_trajectories)
        result = self.number_inoperative_trajectories
        return result
    
    def compute_AEQ2_1_metric(self):
        '''
        AEQ-2.1: Percentage of inoperative trajectories
        (Calculated as the ratio of AEQ-2 and the total number of flight intentions in the given scenario)
        '''
        self.compute_AEQ2_metric()
        result = self.number_inoperative_trajectories / len(self.vehicle_types)
        return result
    
    def compute_AEQ3_metric(self):
        '''
        AEQ-3: The demand delay dispersion
        (Measured as standard deviation of delay of all flight intentions, where delay for each flight intention is calculated
        as a difference between realized arrival time and ideal expected arrival time.
        Ideal expected arrival time is computed as arrival time of the fastest trajectory from origin to destination departing
        at the requested time as if a user were alone in the system, respecting all concept airspace rules.
        Realized arrival time comes directly from the simulations)
        '''
        self.compute_AEQ1_metric()
        result = self.utils.stdDeviation_statistics(self.delay_times)
        return result
    
    def compute_AEQ4_metric(self):
        '''
        AEQ-4: The worst demand delay
        (Computed as the maximal difference between any individual flight intention delay and the average delay;
        where delay for each flight intention is calculated as the difference between realized arrival time and
        ideal expected arrival time)
        '''
        self.compute_AEQ1_metric()
        average_delay = self.utils.average_statistics(self.delay_times)
        max_delay = self.delay_times[0]
        for delay in self.delay_times:
            if(delay > average_delay and delay > max_delay):
                max_delay = delay

        result = max_delay
        return result
    
    def compute_AEQ5_metric(self):
        '''
        AEQ-5: Number of inequitable delayed demands
        (Number of flight intentions whose delay is greater than a given threshold from the average delay in absolute sense,
        where delay for each flight intention is calculated as the difference between
        realized arrival time and ideal expected arrival time)
        '''
        self.compute_AEQ1_metric()
        list_inequitable_delayed_demands = list()
        for delay in self.delay_times:
            if(delay > self.THRESHOLD_DELAY):
                list_inequitable_delayed_demands.append(delay)

        self.number_inequitable_delayed_demands = len(list_inequitable_delayed_demands)
        result = self.number_inequitable_delayed_demands
        return result
    
    def compute_AEQ5_1_metric(self):
        '''
        AEQ-5-1: Percentage of inequitable delayed demands
        (Calculated as the ratio of AEQ-5 and the total number of flight intentions in the given scenario)
        '''
        self.compute_AEQ5_metric()
        result = self.number_inequitable_delayed_demands / len(self.vehicle_types)
        return result