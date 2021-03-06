# -*- coding: utf-8 -*-
"""
Created on Fri Apr 15 18:43:13 2022

@author: labpc2
"""

def compute_cap1(df):
    """ CAP-1: Average demand delay

    Average demand delay is computed as the arithmetic mean of the delays
    of all flight intentions in a scenario.

    :param input_dataframes:filtered by scenario flst_datframe.
    :return: the computed CAP1 metric.
    """
    df_filtered=df[(df['Spawned'])&(df['Mission_completed'])]
    cap_1=df_filtered["Arrival_delay"].mean()
    
    return cap_1

