# -*- coding: utf-8 -*-
"""
Created on Fri Feb 11 16:33:17 2022
@author: nipat
"""
import os
import pandas as pd


import DataframeCreator
#import AEQ_metrics
#import CAP_metrics
#import EFF_metrics
#import ENV_metrics
#import SAF_metrics
#import PRI_metrics



class MainClass():
    def __init__(self, *args, **kwargs):

        self.scenario_name = None

        #self.readLogFiles()
        self.dataframe_creator = DataframeCreator.DataframeCreator( ) # the dataframes are created in  the init function now

        self.fp_intention_dataframe = None #TODO: It was necessary a fpintention dataframe to manage the columns of the file
        self.loslog_dataframe = None
        self.conflog_dataframe = None
        self.geolog_dataframe = None
        self.flst_log_dataframe = None
        self.reglog_obj_dataframe = None
        #self.createDataframes()

       # self.AEQ_metrics = AEQ_metrics.AEQ_metrics(self.fp_intention_dataframe, self.flst_log_dataframe)
        #self.CAP_metrics = CAP_metrics.CAP_metrics(self.fp_intention_dataframe, self.flst_log_dataframe, self.loslog_dataframe)
        #self.EFF_metrics = EFF_metrics.EFF_metrics(self.fp_intention_dataframe, self.flst_log_dataframe)
        #self.ENV_metrics = ENV_metrics.ENV_metrics(self.flst_log_dataframe)
        #self.SAF_metrics = SAF_metrics.SAF_metrics(self.loslog_dataframe, self.conflog_dataframe, self.geolog_dataframe)
        #self.PRI_metrics = PRI_metrics.PRI_metrics(self.fp_intention_dataframe, self.flst_log_dataframe)
        
# =============================================================================
#     def readLogFiles(self):
#         ##Read the log file
#         concept = "3"  ##DECENTRALISED
#         density = "very_low"
#         distribution = "40"
#         repetition = "8"
#         uncertainty = "W1"
#         self.scenario_name = concept + "_" + density + "_" + distribution + "_" + repetition + "_" + uncertainty
# =============================================================================
        

    def createDataframes(self):
        #self.fp_intention_dataframe = self.dataframe_creator.create_fp_intention_dataframe(
        #    "example_logs/Flight_intention_very_low_40_8.csv")
        self.loslog_dataframe = self.dataframe_creator.create_loslog_dataframe() 
        self.conflog_dataframe = self.dataframe_creator.create_conflog_dataframe() 
        self.geolog_dataframe = self.dataframe_creator.create_geolog_dataframe()
        self.flst_log_dataframe = self.dataframe_creator.create_flstlog_dataframe() 
        self.reglog_obj_dataframe = self.dataframe_creator.create_reglog_dataframe()
        time_log_dataframe=self.dataframe_creator.create_time_object_dataframe() 
        metrics_dataframe=self.dataframe_creator.create_metrics_dataframe() 

    def main(self):

        def selectOptionMenu(dictionary):
            for key, value in dictionary.items():
                print(str(key) + " - " + value)
                
            num_OK = False
            option = 0
            while (not num_OK):
                option = int(input("Select metric indicator ID: "))
                if (dictionary.get(option) != None):
                    num_OK = True
                else:
                    print("Invalid indicator ID")
                    
            print("---------------")
            print("Option selected: {}".format(dictionary[option]))
            print("---------------")
            return option

        indicator_dict={
                1: 'ACCESS AND EQUITY',
                2: 'CAPACITY',
                3: 'EFFICIENCY',
                4: 'ENVIRONMENT',
                5: 'SAFETY',
                6: 'PRIORITY',
                7: 'EXIT'
            }
        
        exit_now = False
        while not exit_now:
            print("\n=========================")
            print("M2_data_analysis_platform")
            print("=========================")
            option = selectOptionMenu(indicator_dict)
            
            if option == 1:
                AEQ_metrics_dict={
                    1: 'AEQ-1: Number of cancelled demands',
                    2: 'AEQ-1.1 Percentage of cancelled demands',
                    3: 'AEQ-2: Number of inoperative trajectories',
                    4: 'AEQ-2.1: Percentage of inoperative trajectories',
                    5: 'AEQ-3: The demand delay dispersion',
                    6: 'AEQ-4: The worst demand delay',
                    7: 'AEQ-5: Number of inequitable delayed demands',
                    8: 'AEQ-5.1: Percentage of inequitable delayed demands',
                    9: "Go back"
                }
                AEQ_option = selectOptionMenu(AEQ_metrics_dict)
                result = self.AEQ_metrics.evaluate_AEQ_metric(AEQ_option)
                print (result)

                
            elif option == 2:
                CAP_metrics_dict={
                    1: 'CAP-1: Average demand delay',
                    2: 'CAP-2: Average number of intrusions',
                    3: 'CAP-3: Additional demand delay',
                    4: 'CAP-4: Additional number of intrusions',
                    5: "Go back"
                }
                CAP_option = selectOptionMenu(CAP_metrics_dict)
                result = self.CAP_metrics.evaluate_CAP_metric(CAP_option)
                print (result)


            elif option == 3:
                EFF_metrics_dict={
                    1: 'EFF-1: Horizontal distance route efficiency',
                    2: 'EFF-2: Vertical distance route efficiency',
                    3: 'EFF-3: Ascending route efficiency',
                    4: 'EFF-4: 3D distance route efficiency',
                    5: 'EFF-5: Route duration efficiency',
                    6: 'EFF-6: Departure delay',
                    7: 'EFF-7: Departure sequence delay',
                    8: 'EFF-8: Arrival sequence delay',
                    9: "Go back"
                }
                EFF_option = selectOptionMenu(EFF_metrics_dict)
                result = self.EFF_metrics.evaluate_EFF_metric(EFF_option)
                print (result)


            elif option == 4:
                ENV_metrics_dict={
                    1: 'ENV-1: Work done',
                    2: 'ENV-2: Weighted average altitude',
                    3: 'ENV-3: Equivalent Noise Level',
                    4: 'ENV-4: Altitude dispersion',
                    5: "Go back"
                }   
                ENV_option = selectOptionMenu(ENV_metrics_dict)
                result = self.ENV_metrics.evaluate_ENV_metric(ENV_option)
                print (result)

            elif option == 5:
                SAF_metrics_dict={
                    1: 'SAF-1: Number of conflicts',
                    2: 'SAF-2: Number of intrusions',
                    3: 'SAF-3: Intrusion prevention rate',
                    4: 'SAF-4: Minimum separation',
                    5: 'SAF-5: Time spent in LOS',
                    6: 'SAF-6: Geofence violations',
                    7: "Go back"
                }
                SAF_option = selectOptionMenu(SAF_metrics_dict)
                result = self.SAF_metrics.evaluate_SAF_metric(SAF_option)
                print (result)

                
            elif option == 6:
                PRI_metrics_dict={
                    1: 'PRI-1: Weighted mission duration',
                    2: 'PRI-2: Weighted mission track length',
                    3: 'PRI-3: Average mission duration per priority level',
                    4: 'PRI-4: Average mission track length per priority level',
                    5: 'PRI-5: Total delay per priority level',
                    6: "Go back"
                } 
                PRI_option = selectOptionMenu(PRI_metrics_dict)
                result = self.PRI_metrics.evaluate_PRI_metric(PRI_option)
                print (result)


            else: #last option
                exit_now = True

         
         
        print ("App finished")
    





        # ## Here are some filtering examples:
        # print("D1 apperances:", self.reglog_obj_dataframe.filter(self.reglog_obj_dataframe["ACID"] == "D1").count())
        # self.reglog_obj_dataframe.filter(reglog_obj_dataframe["ACID"] == "D1").show()
        # ##############
        #
        # print("objects from that secnario with time <210:", self.reglog_obj_dataframe.filter(
        #     (self.reglog_obj_dataframe["Time_stamp"] < 210) & (
        #                 self.reglog_obj_dataframe["scenario_name"] == scenario_name)).count())
        # self.reglog_obj_dataframe.filter((self.reglog_obj_dataframe["Time_stamp"] < 210) & (
        #             self.reglog_obj_dataframe["scenario_name"] == scenario_name)).show()
        # ################
        #
        # print("Unique acids in the scenario:", self.reglog_obj_dataframe.filter(
        #     (self.reglog_obj_dataframe["Time_stamp"] < 210) & (
        #                 self.reglog_obj_dataframe["scenario_name"] == scenario_name)).select("ACID").distinct().count())
        # self.reglog_obj_dataframe.filter((self.reglog_obj_dataframe["Time_stamp"] < 210) & (
        #             self.reglog_obj_dataframe["scenario_name"] == scenario_name)).select("ACID").distinct().show()
        #
        


if __name__ == "__main__":
    #TODO: This call to the main() function should be repeated for each of the scenarios in a loop or option menu
    #MainClass().main()
    m=MainClass()