# -*- coding: utf-8 -*-
"""
Created on Fri Apr 15 19:02:04 2022

@author: labpc2
"""

prio_weight_1=1
prio_weight_2=2
prio_weight_3=4
prio_weight_4=8

prio_weights_list=[prio_weight_1,prio_weight_2,prio_weight_3,prio_weight_4]


def compute_pri1(df):
    """ PRI-1: Weighted mission duration

    Total duration of missions weighted in function of priority level.

    :param dataframe: filtered by scenario flst_dataframe.
    :return: the computed PRI1 metric.
    """
    
    pri1=0
    df_filtered=df[(df['Spawned']==True)]
    df_filtered=df_filtered[df_filtered["Mission_completed"]==True]

    pri1+=prio_weight_1*(df_filtered[df_filtered["Priority"]=="1"]["FLIGHT_time"].sum())
    pri1+=prio_weight_2*(df_filtered[df_filtered["Priority"]=="2"]["FLIGHT_time"].sum())
    pri1+=prio_weight_3*(df_filtered[df_filtered["Priority"]=="3"]["FLIGHT_time"].sum())
    pri1+=prio_weight_4*(df_filtered[df_filtered["Priority"]=="4"]["FLIGHT_time"].sum())

    return pri1



def compute_pri2(df):
    """ PRI-2: Weighted mission track length

    Total distance travelled weighted in function of priority level.

    :param dataframe: filtered by scenario flst_dataframe.
    :return:the computed PRI2 metric.
    """
    pri2=0
    df_filtered=df[(df['Spawned']==True)]
    df_filtered=df_filtered[df_filtered["Mission_completed"]==True]
    
 
    
    pri2+=prio_weight_1*df_filtered[df_filtered["Priority"]=="1"]["3D_dist"].sum()
    pri2+=prio_weight_2*df_filtered[df_filtered["Priority"]=="2"]["3D_dist"].sum()
    pri2+=prio_weight_3*df_filtered[df_filtered["Priority"]=="3"]["3D_dist"].sum()
    pri2+=prio_weight_4*df_filtered[df_filtered["Priority"]=="4"]["3D_dist"].sum()

    return pri2

def compute_pri3(df,priority):
    """ PRI-3: Average mission duration per priority level

    The average mission duration for each priority level per aircraft.

    :param dataframe: filtered by scenario  flst_dataframe.
    :param priority: priority level.
    :return: the computed PRI3 metric.
    """
    
    df_filtered=df[(df['Spawned'])&(df['Mission_completed'])&(df['Priority']==priority)]
    pri3=df_filtered["FLIGHT_time"].mean()

    return pri3


def compute_pri4(df,priority):
    """ PRI-4: Average mission track length per priority level

    The average distance travelled for each priority level per aircraft.

    :param dataframe: filtered by scenario  flst_dataframe.
    :param priority: priority level.
    :return: the computed PRI4 metric.
    """
    df_filtered=df[(df['Spawned'])&(df['Mission_completed'])&(df['Priority']==priority)]
    pri4=df_filtered["3D_dist"].mean()

    return pri4



def compute_pri5(df,priority):
    """ PRI-5: Total delay per priority level

    The total delay experienced by aircraft in a certain priority category
    relative to ideal conditions.
    :param dataframe: filtered by scenario  flst_dataframe.
    :param priority: priority level.
    :return: the computed PRI5 metric.
    """
    df_filtered=df[(df['Spawned'])&(df['Mission_completed'])&(df['Priority']==priority)]
    pri5=df_filtered["Arrival_delay"].sum()

    return pri5

