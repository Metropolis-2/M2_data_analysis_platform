# -*- coding: utf-8 -*-
"""
Created on Tue Apr 12 15:07:06 2022

@author: nipat
"""

import matplotlib.pyplot as plt 
import dill
import seaborn as sns
import pandas as pd
import numpy as np

import random ##imported for testing purposes
from matplotlib.patches import PathPatch
import matplotlib
import matplotlib.colors as mc
import colorsys
import math

diagrams_path="output_graphs/"
dills_path="dills/"
hybrid_dills="hybrid_dills/"

scale_y=True

reference_drone_noise=73 #dB

matplotlib.use('Agg')


def adjust_box_widths(g, fac):
    """
    Adjust the withs of a seaborn-generated boxplot.
    """
    k=0

    # iterating through Axes instances
    for ax in g.axes:

        # iterating through axes artists:
        for c in ax.get_children():
            # searching for PathPatches
            if isinstance(c, PathPatch):
                # getting current width of box:
                p = c.get_path()
                verts = p.vertices
                verts_sub = verts[:-1]
                xmin = np.min(verts_sub[:, 0])
                xmax = np.max(verts_sub[:, 0])
                xmid = 0.5*(xmin+xmax)
                xhalf = 0.5*(xmax - xmin)

                # setting new width of box
                xmin_new = xmid-fac*xhalf
                xmax_new = xmid+fac*xhalf
                verts_sub[verts_sub[:, 0] == xmin, 0] = xmin_new
                verts_sub[verts_sub[:, 0] == xmax, 0] = xmax_new
                
                # Set the linecolor on the artist to the facecolor, and set the facecolor to None
                col = lighten_color(c.get_facecolor(), 1.3)
                c.set_edgecolor(col) 

                for j in range((k)*6,(k)*6+6):
                   line = ax.lines[j]
                   line.set_color(col)
                   line.set_mfc(col)
                   line.set_mec(col)
                   line.set_linewidth(0.7)
                    
                for l in ax.lines:
                    if np.all(l.get_xdata() == [xmin, xmax]):
                        l.set_xdata([xmin_new, xmax_new])
                k+=1

def lighten_color(color, amount=0.5):  
    # --------------------- SOURCE: @IanHincks ---------------------
    try:
        c = mc.cnames[color]
    except:
        c = color
    c = colorsys.rgb_to_hls(*mc.to_rgb(c))
    return colorsys.hls_to_rgb(c[0], 1 - amount * (1 - c[1]), c[2])

##Initialisation of the scenario types to be graphed

##Initialisation of the concept types to be graphed
#If you do not want to graph for all three concepts, you may delete the unwanted concept from the variables  concepts and concept_names and concepts_colours
concepts=["1_","2_","3_","4_"]
concept_names=["Centralised","Hybrid","Decentralised","Special_Hybrid"]
concepts_colours=['r','g','b','y']

##Initialisation of the density types to be graphed
#If you do not want to graph for all five density types, you may delete the unwanted densities from the variables  densities and density_names
densities=["very_low_","low_","medium_","high_","ultra_"]
density_names=["very low","low","medium","high","very high"]

##Initialisation of the traffic mix types to be graphed
#If you do not want to graph for all three traffic mix types, you may delete the unwanted traffic mix from the variables  traffic_mix and traffic_mix_names
traffic_mix=["40_"]
traffic_mix_names=["40%"]

##Initialisation of the repetition number to be graphed
#If you do not want to graph for all nine repetitions, you may delete the unwanted repetitions from the variable repetitions
repetitions=["0_","1_","2_","3_","4_","5_","6_","7_","8_"]

##Initialisation of the uncertaity type to be graphed
#If you do not want to graph for all seven uncertainy types, you may delete the unwanted uncertainty types from the variables uncertainties,rogue_uncertainties,wind_uncertainties and uncertainties_names
uncertainties=["","R1","R2","R3","W1","W3","W5"]
rogue_uncertainties=["","R1","R2","R3"]
wind_uncertainties=["","W1","W3","W5"]
uncertainties_names=["No uncertainty","R33","R66","R100","W1","W3","W5"]



percentage_metrics=["AEQ1_1","AEQ2_1","AEQ5_1","EFF1","EFF2","EFF3","EFF4","EFF5","SAF3"]    
metrics_units=[""," (%)",""," (%)"," (sec)"," (sec)",""," (%)"," (sec)",""," (%)"," (%)"," (%)"," (%)"," (%)"," (sec)"," (sec)",\
                       " (m)","","","","","","","","","","",""," (%)"," (m)"," (sec)"," (sec)","","","","","","","",\
                           ""," (sec)"," (m)"," (sec)",""," (sec)"," (m)"," (sec) "] 
        
metrics_title=["Number of cancelled demands","Percentage of cancelled demands","Number of inoperative trajectories","Percentage of inoperative trajectories"\
                       ,"Demand delay dispersion","The worst demand delay","Number of inequitable delayed demands","Percentage of inequitable delayed demands",\
                           "Average demand delay","Average number of intrusions","Horizontal distance route efficiency","Vertical distance route efficiency",\
                               "Ascending route efficiency","3D distance route efficiency","Route duration efficiency","Departure delay","Work done",\
                       "Weighted average altitude","Sound exposure","Number of points with significant sound exposure","Altitude dispersion","Number of conflicts",\
                           "Number of conflicts per flight","Number of conflicts in constrained airspace","Number of conflicts in open airspace","Number of intrusions","Number of severe intrusions",\
                           "Number of intrusions in constrained airspace","Number of intrusions in open airspace","Intrusion prevention rate","Minimum separation","Time spent in LOS","Average time spent in LOS","Number of geofence violations","Number of severe geofence violations"\
                               ,"Number of severe loitering NFZ violations","Number of severe buildings/static geofences violations","Number of severe open airspace geofences violations "\
                                   ,"Number of severe building violations ","Number of severe loitering NFZ violations \n with origin/destination in NFZ",\
                           "Number of severe loitering NFZ violations \n within 3 minutes of the NFZ activation","Weighted mission duration","Weighted mission track length",\
                               "Additional demand delay","Additional number of intrusions",\
                               "Average mission duration per priority level","Average mission track length per priority level","Total delay per priority level"]    
            
boxplot_metrics=["AEQ1","AEQ1_1","AEQ2","AEQ2_1","AEQ3","AEQ4","AEQ5","AEQ5_1","CAP1","CAP2","EFF1","EFF2","EFF3","EFF4","EFF5","EFF6","ENV1",\
                         "ENV2","ENV3_1","ENV3_2","ENV4","SAF1","SAF1_2","SAF1_3","SAF1_4","SAF2","SAF2_1","SAF2_2","SAF2_3","SAF3","SAF4","SAF5","SAF5_1","SAF6","SAF6_1","SAF6_2","SAF6_3","SAF6_4","SAF6_5",\
                             "SAF6_6","SAF6_7","PRI1","PRI2"]
    
metrics_names=["AEQ1","AEQ1.1","AEQ2","AEQ2.1","AEQ3","AEQ4","AEQ5","AEQ5.1","CAP1","CAP2","EFF1","EFF2","EFF3","EFF4","EFF5","EFF6","ENV1",\
                         "ENV2","ENV3.1","ENV3.2","ENV4","SAF1","SAF1.2","SAF1.3","SAF1.4","SAF2","SAF2.1","SAF2.2","SAF2.3","SAF3","SAF4","SAF5","SAF5.1","SAF6","SAF6.1","SAF6.2","SAF6.3","SAF6.4","SAF6.5",\
                             "SAF6.6","SAF6.7","PRI1","PRI2","CAP3","CAP4","PRI3","PRI4","PRI5"]

boxplot_metrics_rogues=["CAP3","CAP4"]
        
boxplot_metrics_priority=["PRI3","PRI4","PRI5"]


    
class GraphCreator():
    def __init__(self):

        self.concept_names_dict={}
        for i in range(len(concepts)):
            self.concept_names_dict[concepts[i]]=concept_names[i]

        self.density_names_dict={}
        for i in range(len(densities)):
            self.density_names_dict[densities[i]]=density_names[i]

        self.traffic_mix_names_dict={}
        for i in range(len(traffic_mix)):
            self.traffic_mix_names_dict[traffic_mix[i]]=traffic_mix_names[i]

        self.uncertainties_names_dict={}
        for i in range(len(uncertainties)):
            self.uncertainties_names_dict[uncertainties[i]]=uncertainties_names[i]

        self.metrics_titles_dict={}
        self.metrics_units_dict={}
        self.metrics_names_dict={}
        i=0
        for m in boxplot_metrics:
            self.metrics_titles_dict[m]=metrics_title[i]
            self.metrics_units_dict[m]=metrics_units[i]
            self.metrics_names_dict[m]=metrics_names[i]
            i+=1
        for m in boxplot_metrics_rogues:
            self.metrics_titles_dict[m]=metrics_title[i]
            self.metrics_units_dict[m]=metrics_units[i]
            self.metrics_names_dict[m]=metrics_names[i]
            i+=1
        for m in boxplot_metrics_priority:
            self.metrics_titles_dict[m]=metrics_title[i]
            self.metrics_units_dict[m]=metrics_units[i]
            self.metrics_names_dict[m]=metrics_names[i]
            i+=1

    def metric_boxplots_baseline(self,metric,dataframe,hybrid_dataframe):
        vals=[]
        for density in densities:
            for t_mix in traffic_mix:
                for conc in concepts:
                    for rep in repetitions:   
                        scenario_name=conc+density+t_mix+rep
                        try:
                            if conc=='4_':
                                metric_value=hybrid_dataframe[hybrid_dataframe["Scenario_name"]==scenario_name][metric].values[0]
                            else:
                                metric_value=dataframe[dataframe["Scenario_name"]==scenario_name][metric].values[0]
                            tmp=[self.concept_names_dict[conc],self.density_names_dict[density],self.traffic_mix_names_dict[t_mix],rep,metric_value]
                            vals.append(tmp)
                        except:
                            tmp=[self.concept_names_dict[conc],self.density_names_dict[density],self.traffic_mix_names_dict[t_mix],rep,0]
                            vals.append(tmp)
                            #metric_value=240+random.randint(-5,5)
                            #print("No value for scenario baseline",scenario_name,metric)
                        
        metric_pandas_df=pd.DataFrame(vals,columns=["Concept","Density","Traffic mix","repetition",metric])
        
        ##Create one graph for every traffic mix
        for t_mix in traffic_mix_names:
            df1=metric_pandas_df[metric_pandas_df["Traffic mix"]==t_mix]
            fig=plt.figure()
            sns.boxplot(y=metric, x='Density', data=df1, palette=concepts_colours,hue='Concept').set(title=self.metrics_titles_dict[metric]+" for "+t_mix+" traffic mix",ylabel = self.metrics_names_dict[metric]+self.metrics_units_dict[metric])
            plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
            adjust_box_widths(fig, 0.5)
            if metric in percentage_metrics and scale_y:
                plt.ylim(-5, 105)
            plt.savefig(diagrams_path+"boxplots/by_traffic_mix/"+metric+"_"+t_mix,bbox_inches='tight')
            plt.savefig(diagrams_path+"pdfs/boxplots/by_traffic_mix/"+metric+"_"+t_mix+".pdf",bbox_inches='tight')
            plt.clf()
            

             
           
    def metric_boxplots_wind(self,metric,dataframe,t_mix):
        vals=[]
        for density in densities:
            for wind in wind_uncertainties[1:]:
                for conc in concepts[:-1]:
                    for rep in repetitions:
                        scenario_name=conc+density+t_mix+rep+wind
                        baseline_scenario_name=conc+density+t_mix+rep+wind_uncertainties[0]
                        try:
                            metric_value=dataframe[dataframe["Scenario_name"]==scenario_name][metric].values[0]
                            baseline_metric_value=dataframe[dataframe["Scenario_name"]==baseline_scenario_name][metric].values[0]

                            tmp=[self.concept_names_dict[conc],self.density_names_dict[density],self.traffic_mix_names_dict[t_mix],rep,metric_value-baseline_metric_value,self.uncertainties_names_dict[wind]]
                            vals.append(tmp)
                        except:
                            #metric_value=240+random.randint(-5,5)
                            print("No value for scenario",scenario_name,metric)
                        
        metric_pandas_df=pd.DataFrame(vals,columns=["Concept","Density","Traffic mix","repetition",metric,"Wind level"])

        
            
        ##Create one graph for every density
        for dens in density_names:
             df1=metric_pandas_df[metric_pandas_df["Density"]==dens]
             fig=plt.figure()
             sns.boxplot(y=metric, x='Wind level', data=df1, palette=concepts_colours,hue='Concept',width=0.7).set(title=self.metrics_titles_dict[metric]+" for "+dens+" density",ylabel = self.metrics_names_dict[metric]+self.metrics_units_dict[metric])
             plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
             adjust_box_widths(fig, 0.5)
             if metric in percentage_metrics and scale_y:
                 plt.ylim(-5, 105)
             plt.savefig(diagrams_path+"boxplots/winds/by_density/"+metric+"_"+dens,bbox_inches='tight')
             plt.savefig(diagrams_path+"pdfs/boxplots/winds/by_density/"+metric+"_"+dens+".pdf",bbox_inches='tight')
             plt.clf()
         
    def metric_boxplots_rogues(self,metric,dataframe,t_mix):
        vals=[]
        for density in densities:
            for rogue in rogue_uncertainties[1:]:
                for conc in concepts[:-1]:
                    for rep in repetitions:
                        scenario_name=conc+density+t_mix+rep+rogue
                        baseline_scenario_name=conc+density+t_mix+rep+rogue_uncertainties[0]

                        try:
                            metric_value=dataframe[dataframe["Scenario_name"]==scenario_name][metric].values[0]
                            baseline_metric_value=dataframe[dataframe["Scenario_name"]==baseline_scenario_name][metric].values[0]

                            tmp=[self.concept_names_dict[conc],self.density_names_dict[density],self.traffic_mix_names_dict[t_mix],rep,metric_value-baseline_metric_value,self.uncertainties_names_dict[rogue]]
                            vals.append(tmp)
                        except:
                            #metric_value=240+random.randint(-5,5)
                            print("No value for scenario",scenario_name,metric)
                        
    
        metric_pandas_df=pd.DataFrame(vals,columns=["Concept","Density","Traffic mix","repetition",metric,"Rogue_level"])
        
        
            
        ##Create one graph for every density
        for dens in density_names:
             df1=metric_pandas_df[metric_pandas_df["Density"]==dens]
             fig=plt.figure()
             sns.boxplot(y=metric, x='Rogue_level', data=df1, palette=concepts_colours,hue='Concept',width=0.7).set(title=self.metrics_titles_dict[metric]+" for "+dens+" density",ylabel = self.metrics_names_dict[metric]+self.metrics_units_dict[metric])
             plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
             adjust_box_widths(fig, 0.5)
             if metric in percentage_metrics and scale_y:
                 plt.ylim(-5, 105)
             plt.savefig(diagrams_path+"pdfs/boxplots/rogues/by_density/"+metric+"_"+dens+".pdf",bbox_inches='tight')
             plt.savefig(diagrams_path+"boxplots/rogues/by_density/"+metric+"_"+dens,bbox_inches='tight')
             plt.clf()
   
  
    def createGraphs(self):       

        ##Load the metrics
        input_file=open(dills_path+"metrics_dataframe.dill", 'rb')
        scenario_metrics_df=dill.load(input_file)
        input_file.close()
        
     
        input_file=open(hybrid_dills+"metrics_dataframe.dill", 'rb')
        hybr_scenario_metrics_df=dill.load(input_file)
        input_file.close()
        
       
        
        



        ## Create the graphs
        boxplot_metrics=["AEQ1","AEQ1_1","AEQ2","AEQ2_1","AEQ3","AEQ4","AEQ5","AEQ5_1","CAP1","CAP2","EFF1","EFF2","EFF3","EFF4","EFF5","EFF6","ENV1","ENV2","ENV3_2","ENV4","SAF1","SAF1_2","SAF1_3","SAF1_4","SAF2","SAF2_1","SAF2_2","SAF2_3","SAF3","SAF4","SAF5","SAF5_1","SAF6","SAF6_1","SAF6_2","SAF6_3","SAF6_4","SAF6_5","SAF6_6","SAF6_7","PRI1","PRI2"]
        
        
        
        uncertainties_metrics=["AEQ2","SAF1_2","SAF2","SAF2_2"]

        

        
        for metric in boxplot_metrics:
             self.metric_boxplots_baseline(metric,scenario_metrics_df,hybr_scenario_metrics_df)
        


        
        for metric in uncertainties_metrics:
              self.metric_boxplots_rogues(metric,scenario_metrics_df,"40_") 
              self.metric_boxplots_wind(metric,scenario_metrics_df,"40_")
         

         
       