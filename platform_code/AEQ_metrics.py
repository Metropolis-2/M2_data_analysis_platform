# -*- coding: utf-8 -*-
"""
Created on Thu Feb 24 10:49:23 2022

@author: jpedrero
"""
import utils

class AEQ_metrics():

    # Ctes
    #TODO: define this values
    '''AEQ-1 Cancellation rules'''
    CANCELLATION_DELAY_LIMIT_emergency_mission = 300
    CANCELLATION_DELAY_LIMIT_delivery_mission = 600 #TODO: delivery_mission is the rest of missions?
    CANCELLATION_DELAY_LIMIT_loitering_mission = 1200
    '''AEQ-2 Vehicle autonomy'''
    AUTONOMY_MP20 = 600
    AUTONOMY_MP30 = 1200
    '''AEQ-5 Threshold from the average delay in an absolute sense'''
    THRESHOLD_DELAY = 120

    def __init__(self, flst_log_dataframe):
        self.utils = utils.Utils()
        self.flst_log_dataframe = flst_log_dataframe

        self.delay_times = list()
        self.rec_snd_points = dict([(str(row["ACID"]), [(row["Origin_LAT"],row["Origin_LON"]), (row["Dest_LAT"], row["Dest_LON"])]) for row in self.flst_log_dataframe.select("ACID", "Origin_LAT","Origin_LON", "Dest_LAT","Dest_LON").collect()])
        self.vehicle_types = dict([(str(row["ACID"]), str(row["Aircraft_type"])) for row in self.flst_log_dataframe.select("ACID", "Aircraft_type").collect()])
        self.flight_times = dict([(str(row["ACID"]), float(row["FLIGHT_time"])) for row in self.flst_log_dataframe.select("ACID", "FLIGHT_time").collect()])
        self.loitering_missions = [(str(row["ACID"])) for row in self.flst_log_dataframe.select("ACID","Geofence_duration").filter("Geofence_duration != 'None'").collect()]
        self.emergency_missions = [(str(row["ACID"])) for row in self.flst_log_dataframe.select("ACID","Priority").filter("Priority == 4").collect()]
        return
        
    def evaluate_AEQ_metric(self, metric_id):
        self.delay_times.clear() #CRITICAL POINT
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
        ideal_route_dist = {}  #route_path 2D distance between sending and receiving points for each vehicle ACID
        ideal_flight_time = {} #ideal flight_time based on ideal route_dist and vehicle_speed
        for key, value in self.rec_snd_points.items():
            snd_point = value[0]
            rcv_point = value[1]
            route_dist = self.utils.distCoords(snd_point, rcv_point)
            ideal_route_dist[key] = route_dist
            vehicle_speed = self.utils.getVehicleMaxSpeed(self.vehicle_types[key])
            ideal_flight_time[key] = route_dist/vehicle_speed

        list_cancelled_demands = list()
        for key, value in self.flight_times.items():
            delay = abs(self.flight_times[key] - ideal_flight_time[key])
            self.delay_times.append(delay) #Add to delay list

            #Get mission type (emergency, loitering or delivery)
            if(key in self.emergency_missions):
                cancel_value = self.CANCELLATION_DELAY_LIMIT_emergency_mission
            elif(key in self.loitering_missions):
                cancel_value = self.CANCELLATION_DELAY_LIMIT_loitering_mission
            else:
                cancel_value = self.CANCELLATION_DELAY_LIMIT_delivery_mission

            if(delay >= cancel_value):
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
            if(delay > average_delay and delay > max_delay): #find the maximum value with respect to exceeding the mean
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