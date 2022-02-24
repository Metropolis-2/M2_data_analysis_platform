# -*- coding: utf-8 -*-
"""
Created on Wed Feb 23 17:42:34 2022

@author: jpedrero
"""
from MainApp import *

class DataframeCreator():

    def __init__(self, scenario_name, spark):
        self.scenario_name = scenario_name
        self.spark = spark
        return

    ##LOSLOG dataframe
    def create_loslog_dataframe(self, log_file):

        loslog_file = open(log_file, "r")

        loslog_list = list()
        cnt = 0
        for line in loslog_file:
            cnt = cnt + 1
            if cnt < 10:
                continue
            line_list = line.split(",")
            tmp_list = [cnt - 10, self.scenario_name]
            for iv, value in enumerate(line_list):
                if iv < 3 or (iv > 4 and iv < 11):
                    tmp_list.append(float(value))
                elif iv == 11:
                    tmp_list.append(float(value[:-2]))
                else:
                    tmp_list.append(value)

            loslog_list.append(tmp_list)

        col_list = ["LOS_id", "Scenario_name", "LOS_exit_time", "LOS_start_time", "Time_of_min_distance", "ACID1",
                    "ACID2", "LAT1", "LON1", "ALT1", "LAT2", "LON2", "ALT2", "DIST"]

        df = pd.DataFrame(loslog_list, columns=col_list)

        loslog_data_frame = self.spark.createDataFrame(df)

        loslog_data_frame.show()
        return loslog_data_frame

    ####

    ##CONFLOG dataframe
    def create_conflog_dataframe(self, log_file):
        conflog_file = open(log_file, "r")

        conflog_list = list()
        cnt = 0
        for line in conflog_file:
            cnt = cnt + 1
            if cnt < 10:
                continue
            line_list = line.split(",")
            tmp_list = [cnt - 10, self.scenario_name]
            for iv, value in enumerate(line_list):
                if (iv == 0 or iv > 2) and iv != 7:
                    tmp_list.append(float(value))
                elif iv == 10:
                    tmp_list.append(float(value[:-2]))
                else:
                    tmp_list.append(value)

            conflog_list.append(tmp_list)

        col_list = ["CONF_id", "Scenario_name", "CONF_detected_time", "ACID1", "ACID2", "LAT1", "LON1", "ALT1", "LAT2",
                    "LON2", "ALT2", "CPALAT", "CPALON"]

        df = pd.DataFrame(conflog_list, columns=col_list)

        conflog_data_frame = self.spark.createDataFrame(df)

        conflog_data_frame.show()
        return conflog_data_frame

    ####

    ##GEOLOG dataframe
    def create_geolog_dataframe(self, log_file):
        geolog_file = open(log_file, "r")

        geolog_list = list()
        cnt = 0
        for line in geolog_file:
            cnt = cnt + 1
            if cnt < 10:
                continue
            line_list = line.split(",")
            tmp_list = [cnt - 10, self.scenario_name]
            for iv, value in enumerate(line_list):
                if (iv == 0 or iv > 3) and iv != 7:
                    tmp_list.append(float(value))
                elif iv == 7:
                    tmp_list.append(float(value[:-2]))
                else:
                    tmp_list.append(value)

            geolog_list.append(tmp_list)

        col_list = ["GEO_id", "Scenario_name", "Deletion_time", "ACID", "GEOF_ID", "GEOF_NAME", "MAX_intrusion",
                    "LAT_intrusion", "LON_intrusion", "intrusion_time"]

        df = pd.DataFrame(geolog_list, columns=col_list)

        geolog_data_frame = self.spark.createDataFrame(df)

        geolog_data_frame.show()
        return geolog_data_frame

    ####

    ##FLSTLOG dataframe
    def create_flstlog_dataframe(self, log_file):
        flstlog_file = open(log_file, "r")

        flstlog_list = list()
        cnt = 0
        for line in flstlog_file:
            cnt = cnt + 1
            if cnt < 10:
                continue
            line_list = line.split(",")
            tmp_list = [cnt - 10, self.scenario_name, line_list[1], "-", "-", "-", "-", "-", "-", "-", "-", "-"]
            for iv, value in enumerate(line_list):
                if iv == 0 or (iv > 1 and iv < 14) or (iv > 14 and iv < 18):
                    tmp_list.append(float(value))
                if iv == 14:
                    tmp_list.append(value)
                elif iv == 18:
                    tmp_list.append(float(value[:-2]))

            flstlog_list.append(tmp_list)

        col_list = ['Flight_id', "scenario_name", "ACID", "Origin", "Dest", "Baseline_deparure_time", "Aircarft_type",
                    "Priority", "Baseline_2d_path_length", "Baseline_3d_path_length", \
                    "Baseline_vertical_path_length", "Baseline_flight_duration", "DEL_time", "SPAWN_time",
                    "FLIGHT_time", "2D_dist", "3D_dist", "ALT_dist", "Work_done", "DEL_LAT" \
            , "DEL_LON", "DEL_ALT", "TAS", "DEL_VS", "DEL_HDG", "ASAS_active", "PILOT_ALT", "PILOT_SPD", "PILOT_HDG",
                    "PILOT_VS"]

        df = pd.DataFrame(flstlog_list, columns=col_list)

        flstlog_data_frame = self.spark.createDataFrame(df)

        flstlog_data_frame.show()
        return flstlog_data_frame

    ####

    ##REGLOG dataframe
    def read_reglog(self, log_file):
        reglog_file = open(log_file, "r")

        acid_lines_list = []
        alt_lines_list = []
        lon_lines_list = []
        lat_lines_list = []

        cnt_modulo = 0
        cnt = 0
        for line in reglog_file:
            cnt = cnt + 1
            if cnt < 10:
                continue

            if cnt_modulo % 4 == 0:
                acid_lines_list.append(line[:-2])
            elif cnt_modulo % 4 == 1:
                alt_lines_list.append(line[:-2])
            elif cnt_modulo % 4 == 2:
                lat_lines_list.append(line[:-2])
            else:
                lon_lines_list.append(line[:-2])
            cnt_modulo = cnt_modulo + 1

        return acid_lines_list, alt_lines_list, lon_lines_list, lat_lines_list

    ####

    ##REGLOG dataframe
    def create_reglog_dataframe(self):
        acid_lines_list, alt_lines_list, lon_lines_list, lat_lines_list = read_reglog()

        reglog_list = list()
        reglog_object_counter = 0

        for i, line in enumerate(acid_lines_list):
            acid_line_list = line.split(",")
        alt_line_list = alt_lines_list[i].split(",")
        lat_line_list = lat_lines_list[i].split(",")
        lon_line_list = lon_lines_list[i].split(",")

        for iv, value in enumerate(acid_line_list):
            if iv == 0:
                continue

            tmp_list = [reglog_object_counter, self.scenario_name, float(acid_line_list[0])]
            tmp_list.append(value)
            tmp_list.append(alt_line_list[iv])
            tmp_list.append(lat_line_list[iv])
            tmp_list.append(lon_line_list[iv])

            reglog_object_counter = reglog_object_counter + 1

            reglog_list.append(tmp_list)  # pyspark gives an error when trying to pass list as a value

        col_list = ["REG_id", "scenario_name", "Time_stamp", "ACID", "ALT", "LAT", "LON"]

        df = pd.DataFrame(reglog_list, columns=col_list)

        reglog_data_frame = self.spark.createDataFrame(df)

        # reglog_data_frame.show()

        return reglog_data_frame

    ##time object dataframe
    def create_time_object_dataframe(self):

        acid_lines_list, alt_lines_list, lon_lines_list, lat_lines_list = read_reglog()

        time_object_cnt = 0
        time_list = list()

        for line in acid_lines_list:
            line_list = line.split(",")
            tmp_list = [time_object_cnt, self.scenario_name, float(line_list[0])]
            time_object_cnt = time_object_cnt + 1
            aircraft_counter = len(line_list) - 1

            tmp_list.append(aircraft_counter)
            tmp_list.append("-")
            tmp_list.append("-")
            tmp_list.append("-")
            time_list.append(tmp_list)

        col_list = ["Time_object_id", "scenario_name", "Time_stamp", "#Aircaft_Alive", "Sound_exposure_p1",
                    "Sound_exposure_p2",
                    "Sound_exposure_p3"]  ## add as many "Sound_exposure_p1" as the points of interest

        df = pd.DataFrame(time_list, columns=col_list)

        time_data_frame = self.spark.createDataFrame(df)

        time_data_frame.show()
        return time_data_frame

    ####

    ##metrics dataframe
    def create_metrics_dataframe(self):

        metrics_list = list()
        tmp_list = [self.scenario_name, "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-",
                    "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-"]
        metrics_list.append(tmp_list)

        col_list = ["Scenario_name", "#Aircraft_number", "AEQ1", "AEQ1_1", "AEQ2", "AEQ2_1", "AEQ3", "AEQ4", "AEQ5",
                    "AEQ5_1", "CAP1", "CAP2", "CAP3", "CAP4", "ENV1", "ENV2", "ENV3", "ENV4", "SAF1", "SAF2", "SAF3",
                    "SAF4", "SAF5", "SAF6" \
                                    "EFF1", "EFF2", "EFF3", "EFF4", "EFF5", "EFF6", "EFF7", "EFF8", "PRI1", "PRI2",
                    "PRI3", "PRI4", "PRI5"]

        df = pd.DataFrame(metrics_list, columns=col_list)

        metrics_data_frame = self.spark.createDataFrame(df)

        metrics_data_frame.show()
        return metrics_data_frame

    ####
