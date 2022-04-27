# -*- coding: utf-8 -*-
"""
Created on Fri Apr 15 16:44:11 2022

@author: labpc2
"""
import pandas as pd

aeq_1_threshold_emergency=300 # 5 minutes
aeq_1_threshold_delivery=600 # 10 minutes
aeq_1_threshold_loitering=1200 # 20 minutes

aeq_2_drone_autonomy=1800 #30 minutes

aeq_5_threshold=50

def compute_aeq1(df):
    """ AEQ-1: Number of cancelled demands

    Number of situations when realized arrival time of a given flight intention 
    is greater than ideal expected arrival time by more or equal than 
    some given cancellation delay limit that depends on mission type.

    :param input_dataframes:filtered by scenario flst_datframe.
    :return: the computed AEQ1 metric.
    """

    aeq1=0
    
    aeq1+=df[(df['Arrival_delay'] >aeq_1_threshold_loitering) & (df["loitering"]) & (df["Spawned"])  & (df["Mission_completed"])].shape[0]
    
    aeq1+=df[(df['Arrival_delay'] >aeq_1_threshold_emergency) & (df["Priority"]==4) & (df["Spawned"])  & (df["Mission_completed"])].shape[0]
    
    aeq1+=df[(df['Arrival_delay'] >aeq_1_threshold_delivery) & (df["Priority"]!=4)& (df["Spawned"])  & (df["Mission_completed"]) ].shape[0]
    
    aeq1+=df[  df["Spawned"]==False ].shape[0]
    aeq1+=df[ ( df["Mission_completed"]==False) & (df["Spawned"])   ].shape[0]

    return aeq1

def compute_aeq2(df):
    """ AEQ-2: Number of inoperative trajectories

     Number of situations when realized total 
     mission duration is greater than specific drone autonomy. 

    :param input_dataframes:filtered by scenario flst_datframe.
    :return: the computed AEQ2 metric.
    """
    aeq2=0
    
    aeq2+=df[(df["FLIGHT_time"] >aeq_2_drone_autonomy)].shape[0]

    return aeq2

def compute_aeq3(df):
    """ AEQ-3: The demand delay dispersion

    Measured as standard deviation of delay of all flight intentions,
    where delay for each flight intention is calculated as a difference between
    realized arrival time and ideal expected arrival time.

    Ideal expected arrival time is computed as arrival time of the fastest
    trajectory from origin to destination departing at the requested time as
    if a user were alone in the system, respecting all concept airspace rules.

    Realized arrival time comes directly from the simulations.
    The missions not completed are filtered from this metric.

    :param input_dataframes:filtered by scenario flst_datframe.
    :return: the computed AEQ3 metric.
    
    """
    df_filtered=df[(df['Spawned'])&(df['Mission_completed'])]
    aeq3=df_filtered["Arrival_delay"].std()
    
    return aeq3

def compute_aeq4(df):
    """ AEQ-4: The worst demand delay
    Computed as the maximal difference between any individual flight intention
    delay and the average delay, where delay for each flight intention is
    calculated as the difference between realized arrival time and
    ideal expected arrival time.

    The missions not started and completed are filtered from this metric.

    :param input_dataframes:filtered by scenario flst_datframe.
    :return: the computed AEQ4 metric.
    """
    
    df_filtered=df[(df['Spawned'])&(df['Mission_completed'])]
    average_delay=df_filtered["Arrival_delay"].mean()
    df_filtered["Delay_from_average"]=abs(df_filtered["Arrival_delay"]-average_delay) ##TODO: should that be abs?
    aeq4=df_filtered["Delay_from_average"].max()
    
    return aeq4

def compute_aeq5(df):
    """ AEQ-5: Number of inequitable delayed demands

    Number of flight intentions whose delay is greater than a given threshold
    from the average delay in absolute sense,
    where delay for each flight intention is calculated as the difference between
    realized arrival time and ideal expected arrival time.

    :param input_dataframes:filtered by scenario flst_datframe.
    :return: the computed AEQ5 metric.
    """
    df_filtered=df[(df['Spawned'])&(df['Mission_completed'])]
    average_delay=df_filtered["Arrival_delay"].mean()
   
    aeq5=0
    
    aeq5+=df[(abs(df['Arrival_delay']-average_delay )>aeq_5_threshold) & (df["Spawned"])  & (df["Mission_completed"])].shape[0]

    
    aeq5+=df[  df["Spawned"]==False ].shape[0]
    aeq5+=df[ ( df["Mission_completed"]==False) & (df["Spawned"])   ].shape[0] 
    
    
    return aeq5
