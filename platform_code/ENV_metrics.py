# -*- coding: utf-8 -*-
"""
Created on Fri Apr 15 19:18:10 2022

@author: labpc2
"""
import pandas as pd

def compute_env1(df):
    """ ENV-1: Work done

    Representing total energy needed to perform all flight intentions,
    computed by integrating the thrust (force) over the route displacement.

    :param input_dataframes: dictionary with the dataframes from the log files.
    :return: query result with the ENV-1 metric per scenario.
    """
    df_filtered=df[df['Spawned']]
    env1=df_filtered["work_done"].sum()
    
    return env1

