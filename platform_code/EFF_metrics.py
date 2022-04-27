# -*- coding: utf-8 -*-
"""
Created on Fri Apr 15 17:37:10 2022

@author: labpc2
"""

def compute_eff1(df):
    """ EFF-1: Horizontal distance route efficiency

    Ratio representing the length of the ideal horizontal route to the actual
    horizontal route.

    :param dataframe:filtered by sceanrio flst_dataframe.
    :return: the computed EFF1 metric.
    """
    
    df_filtered=df[(df['Spawned'])&(df['Mission_completed'])]
    sum_ideal_length=df_filtered["Baseline_2D_distance"].sum()
    sum_actual_length=df_filtered["2D_dist"].sum()
    eff1=sum_ideal_length/sum_actual_length
    return eff1


def compute_eff2(df):
    """ EFF-2: Vertical distance route efficiency

    Ratio representing the length of the ideal vertical route to the actual
    vertical route.

    :param dataframe:filtered by sceanrio flst_dataframe.
    :return: the computed EFF2 metric.
    """
    
    df_filtered=df[(df['Spawned'])&(df['Mission_completed'])]

    sum_ideal_length=df_filtered["Baseline_vertical_distance"].sum()
    sum_actual_length=df_filtered["ALT_dist"].sum()
    eff2=sum_ideal_length/sum_actual_length
    return eff2

def compute_eff3(df):
    """ EFF-3: Ascending route efficiency

    Ratio representing the length of the ascending distance in the ideal route
    to the length of the ascending distance of the actual route.

    :param dataframe:filtered by sceanrio flst_dataframe.
    :return: the computed EFF3 metric.
    """
    df_filtered=df[(df['Spawned'])&(df['Mission_completed'])]
    sum_ideal_length=df_filtered["Baseline_ascending_distance"].sum()
    sum_actual_length=df_filtered["Ascend_dist"].sum()
    eff3=sum_ideal_length/sum_actual_length
    return eff3

def compute_eff4(df):
    """ EFF-4: 3D distance route efficiency

    Ratio representing the 3D length of the ideal route to the 3D length
    of the actual route.

    :param dataframe:filtered by sceanrio flst_dataframe.
    :return: the computed EFF4 metric.
    """
    
    df_filtered=df[(df['Spawned'])&(df['Mission_completed'])]
    sum_ideal_length=df_filtered["Baseline_3D_distance"].sum()
    sum_actual_length=df_filtered["3D_dist"].sum()
    eff4=sum_ideal_length/sum_actual_length
    return eff4

def compute_eff5(df):
    """ EFF-5: Route duration efficiency

    Ratio representing the time duration of the ideal route to the time
    duration of the actual route.

    :param dataframe:filtered by sceanrio flst_dataframe.
    :return: the computed EFF5 metric.
    """
    df_filtered=df[(df['Spawned'])&(df['Mission_completed'])]
    sum_ideal_time=df_filtered["Baseline_flight_time"].sum()
    sum_actual_time=df_filtered["FLIGHT_time"].sum()
    eff5=sum_ideal_time/sum_actual_time
    return eff5


def compute_eff6(df):
    """ EFF-6: Departure delay

    Time duration from the planned departure time until the actual
    departure time of the aircraft.
    :param dataframe:filtered by sceanrio flst_dataframe.
    :return: the computed EFF6 metric.
    """
    
    df_filtered=df[(df['Spawned'])&(df['Mission_completed'])]
    eff6=df_filtered["Departure_delay"].mean()

    return eff6
