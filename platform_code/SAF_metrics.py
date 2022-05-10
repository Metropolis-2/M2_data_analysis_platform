# -*- coding: utf-8 -*-
"""
Created on Fri Apr 15 17:59:47 2022

@author: labpc2
"""

def compute_saf1(df):
    """ SAF-1: Number of conflicts

    Number of aircraft pairs that will experience a loss of separation
    within the look-ahead time.

    :param input_dataframes: the filtered by scenario conflog_datframe.
    :return: the computed SAF1 metric.
    """

    saf1=df[df['in_time']].shape[0]
    return saf1

def compute_saf2(df):
    """ SAF-2: Number of intrusions

    Number of aircraft pairs that experience loss of separation.
    
    :param input_dataframes: the filtered by scenario loslog_datframe.
    :return: the computed SAF2 metric.
    """
    saf2=df[df['in_time']].shape[0]
    return saf2


def compute_saf2_1(df):
    """ SAF-2-1: Count of crashes

    Count of crashes for each scenario (each aircraft logged in the FLSTlog has a boolean flag called crash)
    if that is true ir counts as a crash and the number of the times crash is true is the result of this metric.

    :param input_dataframes: the filtered by scenario loslog_datframe.
    :return: the computed SAF2-1 metric.
    """
    saf2=df[(df['in_time'])&(df['crash'])].shape[0]
    return saf2

def compute_saf4(df):
    """ SAF-4: Minimum separation

    The minimum separation between aircraft during conflicts.

    :param input_dataframes: the filtered by scenario loslog_datframe.
    :return: the computed SAF4 metric.
    """
    saf4=df[df['in_time']]['DIST'].min()
    return saf4

def compute_saf5(df):
    """ SAF-5: Time spent in LOS

    Total time spent in a state of intrusion.

    :param input_dataframes: the filtered by scenario loslog_datframe.
    :return: the computed SAF5 metric.
    """
    saf5=df[df['in_time']]['LOS_duration_time'].sum()
    return saf5

def compute_saf6(df):
    """ SAF-6: Geofence violations

    The number of geofence/building area violations.

    :param input_dataframes: the filtered by scenario geolog_datframe.
    :return: the computed SAF6 metric.
    """
    
    saf6=df[df['in_time']].shape[0]

    return saf6


def compute_saf6_1(df):
    """ SAF-6_1: Severe Geofence violations

    The number of severe geofence/building area violations.
    Every geofence violation in the GEOlog has a severity flag.

    :param input_dataframes: the filtered by scenario geolog_datframe.
    :return: the computed SAF6-1 metric.
    """
    saf6=df[(df['in_time'])&(df['Violation_severity'])].shape[0]

    return saf6

def compute_saf6_2(df):
    """ SAF-6_2: Severe loitering Geofence violations

    The number of severe geofence/building area violations in loitering.
    Every geofence violation in the GEOlog has a severity flag.

    :param input_dataframes: the filtered by scenario geolog_datframe.
    :return: the computed SAF6-2 metric.
    """
    saf6=df[(df['in_time'])&(df['Violation_severity'])&(df['Loitering_nfz'])].shape[0]

    return saf6

def compute_saf6_3(df):
    """ SAF-6_3: Severe not loitering Geofence violations

    The number of severe geofence/building area violations not loitering.
    Every geofence violation in the GEOlog has a severity flag.

    :param input_dataframes: the filtered by scenario geolog_datframe.
    :return: the computed SAF6-3 metric.    
    """
    saf6=df[(df['in_time'])&(df['Violation_severity'])&(df['Loitering_nfz']==False)].shape[0]

    return saf6

def compute_saf6_4(df):
    """ SAF-6_4: Severe openloitering Geofence violations

    The number of severe geofence/building area violations in open.
    Every geofence violation in the GEOlog has a severity flag.

    :param input_dataframes: the filtered by scenario geolog_datframe.
    :return: the computed SAF6-4 metric.
    """
    saf6=df[(df['in_time'])&(df['Violation_severity'])&(df['Open_airspace'])].shape[0]

    return saf6

def compute_saf6_5(df):
    """ SAF-6_5: Severe constrained Geofence violations

    The number of severe building area violations.
    Every geofence violation in the GEOlog has a severity flag.

    :param input_dataframes: the filtered by scenario geolog_datframe.
    :return: the computed SAF6-5 metric.
    """
    saf6=df[(df['in_time'])&(df['Violation_severity'])&(df['Open_airspace']==False)&(df['Loitering_nfz']==False)].shape[0]

    return saf6

def compute_saf6_6(df):
    """ SAF-6_6: Severe loitering Geofence violations which occured 
    because the aircraft's origin or destination was in the nfz area



    :param input_dataframes: the filtered by scenario geolog_datframe.
    :return: the computed SAF6-6 metric.
    """
    saf6=df[(df['in_time'])&(df['Violation_severity'])&(df['Loitering_nfz'])&(df['Node_in_nfz'])].shape[0]

    return saf6

def compute_saf6_7(df):
    """ SAF-6_7: Severe loitering Geofence violations which occured 
    because the aircraft was already in the nfz area



    :param input_dataframes: the filtered by scenario geolog_datframe.
    :return: the computed SAF6-7 metric.
    """
    saf6=df[(df['in_time'])&(df['Violation_severity'])&(df['Loitering_nfz'])&(df['In_nfz_applied'])].shape[0]

    return saf6
