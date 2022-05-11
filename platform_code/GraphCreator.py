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

diagrams_path="output_graphs/"
dills_path="dills/"

scale_y=True

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
concepts=["1_","2_","3_"]
concept_names=["Centralised","Hybrid","Decentralised"]
concepts_colours=['r','g','b']

##Initialisation of the density types to be graphed
#If you do not want to graph for all five density types, you may delete the unwanted densities from the variables  densities and density_names
densities=["very_low_","low_","medium_","high_","ultra_"]
density_names=["very_low","low","medium","high","very_high"]

##Initialisation of the traffic mix types to be graphed
#If you do not want to graph for all three traffic mix types, you may delete the unwanted traffic mix from the variables  traffic_mix and traffic_mix_names
traffic_mix=["40_","50_","60_"]
traffic_mix_names=["40%","50%","60%"]

##Initialisation of the repetition number to be graphed
#If you do not want to graph for all nine repetitions, you may delete the unwanted repetitions from the variable repetitions
repetitions=["0_","1_","2_","3_","4_","5_","6_","7_","8_"]

##Initialisation of the uncertaity type to be graphed
#If you do not want to graph for all seven uncertainy types, you may delete the unwanted uncertainty types from the variables uncertainties,rogue_uncertainties,wind_uncertainties and uncertainties_names
uncertainties=["","R1","R2","R3","W1","W3","W5"]
rogue_uncertainties=["","R1","R2","R3"]
wind_uncertainties=["","W1","W3","W5"]
uncertainties_names=["No uncertainty","R1","R2","R3","W1","W3","W5"]



percentage_metrics=["AEQ1_1","AEQ2_1","AEQ5_1","EFF1","EFF2","EFF3","EFF4","EFF5","SAF3"]    
metrics_units=[""," (%)",""," (%)"," (sec)"," (sec)",""," (%)"," (sec)",""," (%)"," (%)"," (%)"," (%)"," (%)"," (sec)"," (sec)",\
                       " (m)","","","","","","",""," (%)"," (m)"," (sec)","","","","","","","",\
                           ""," (sec)"," (m)"," (sec)",""," (sec)"," (m)"," (sec) "] 
        
metrics_title=["Number of cancelled demands","Percentage of cancelled demands","Number of inoperative trajectories","Percentage of inoperative trajectories"\
                       ,"Demand delay dispersion","The worst demand delay","Number of inequitable delayed demands","Percentage of inequitable delayed demands",\
                           "Average demand delay","Average number of intrusions","Horizontal distance route efficiency","Vertical distance route efficiency",\
                               "Ascending route efficiency","3D distance route efficiency","Route duration efficiency","Departure delay","Work done",\
                       "Weighted average altitude","Sound exposure","Number of points with significant sound exposure","Altitude dispersion","Number of conflicts","Number of conflicts per flight","Number of intrusions","Number of severe intrusions",\
                           "Intrusion prevention rate","Minimum separation","Time spent in LOS","Number of geofence violations","Number of severe geofence violations"\
                               ,"Number of severe loitering NFZ violations","Number of severe buildings/static geofences violations","Number of severe open airspace geofences violations "\
                                   ,"Number of severe building violations ","Number of severe loitering NFZ violations, with origin/destination in NFZ",\
                           "Number of severe loitering NFZ violations within 3 minutes of the NFZ activation","Weighted mission duration","Weighted mission track length",\
                               "Additional demand delay","Additional number of intrusions",\
                               "Average mission duration per priority level","Average mission track length per priority level","Total delay per priority level"]    
            
boxplot_metrics=["AEQ1","AEQ1_1","AEQ2","AEQ2_1","AEQ3","AEQ4","AEQ5","AEQ5_1","CAP1","CAP2","EFF1","EFF2","EFF3","EFF4","EFF5","EFF6","ENV1",\
                         "ENV2","ENV3_1","ENV3_2","ENV4","SAF1","SAF1_2","SAF2","SAF2_1","SAF3","SAF4","SAF5","SAF6","SAF6_1","SAF6_2","SAF6_3","SAF6_4","SAF6_5",\
                             "SAF6_6","SAF6_7","PRI1","PRI2"]

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
        i=0
        for m in boxplot_metrics:
            self.metrics_titles_dict[m]=metrics_title[i]
            self.metrics_units_dict[m]=metrics_units[i]
            i+=1
        for m in boxplot_metrics_rogues:
            self.metrics_titles_dict[m]=metrics_title[i]
            self.metrics_units_dict[m]=metrics_units[i]
            i+=1
        for m in boxplot_metrics_priority:
            self.metrics_titles_dict[m]=metrics_title[i]
            self.metrics_units_dict[m]=metrics_units[i]
            i+=1

    def metric_boxplots_baseline(self,metric,dataframe):
        vals=[]
        for density in densities:
            for t_mix in traffic_mix:
                for conc in concepts:
                    for rep in repetitions:   
                        scenario_name=conc+density+t_mix+rep
                        try:
                            metric_value=dataframe[dataframe["Scenario_name"]==scenario_name][metric].values[0]
                            tmp=[self.concept_names_dict[conc],self.density_names_dict[density],self.traffic_mix_names_dict[t_mix],rep,metric_value]
                            vals.append(tmp)
                        except:
                            #metric_value=240+random.randint(-5,5)
                            print("No value for scenario baseline",scenario_name,metric)
                        
        metric_pandas_df=pd.DataFrame(vals,columns=["Concept","Density","Traffic mix","repetition",metric])
        
        ##Create one graph for every traffic mix
        for t_mix in traffic_mix_names:
            df1=metric_pandas_df[metric_pandas_df["Traffic mix"]==t_mix]
            fig=plt.figure()
            sns.boxplot(y=metric, x='Density', data=df1, palette=concepts_colours,hue='Concept').set(title=self.metrics_titles_dict[metric]+" for "+t_mix+" traffic mix",ylabel = metric+self.metrics_units_dict[metric])
            plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
            adjust_box_widths(fig, 0.5)
            if metric in percentage_metrics and scale_y:
                plt.ylim(0, 100)
            plt.savefig(diagrams_path+"boxplots/by_traffic_mix/"+metric+"_"+t_mix,bbox_inches='tight')
            plt.savefig(diagrams_path+"pdfs/boxplots/by_traffic_mix/"+metric+"_"+t_mix+".pdf",bbox_inches='tight')
            plt.clf()
            
        ##Create one graph for every density
        for dens in density_names:
             df1=metric_pandas_df[metric_pandas_df["Density"]==dens]
             fig=plt.figure()
             sns.boxplot(y=metric, x='Traffic mix', data=df1, palette=concepts_colours,hue='Concept',width=0.7).set(title=self.metrics_titles_dict[metric]+" for "+dens+" density",ylabel = metric+self.metrics_units_dict[metric])
             plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
             adjust_box_widths(fig, 0.5)
             if metric in percentage_metrics and scale_y:
                 plt.ylim(0, 100)
             plt.savefig(diagrams_path+"boxplots/by_density/"+metric+"_"+dens,bbox_inches='tight')
             plt.savefig(diagrams_path+"pdfs/boxplots/by_density/"+metric+"_"+dens+".pdf",bbox_inches='tight')
             plt.clf()
             
    def all_values_CAP1(self,dataframe):
        vals=[]
        metric="CAP1"
        for density in densities:
            for t_mix in traffic_mix:
                for conc in concepts:
                    for rep in repetitions:   
                        scenario_name=conc+density+t_mix+rep
                        try:
                            metric_values=dataframe[(dataframe["Scenario_name"]==scenario_name)&(dataframe['Spawned'])&(dataframe['Mission_completed'])]["Arrival_delay"].values
                            for metric_list in metric_values:
                                for metric_value in metric_list:
                                    tmp=[self.concept_names_dict[conc],self.density_names_dict[density],self.traffic_mix_names_dict[t_mix],rep,metric_value]
                                    vals.append(tmp)
                        except:
                            #metric_value=240+random.randint(-5,5)
                            print("No value for scenario baseline",scenario_name,metric)
    
        metric_pandas_df=pd.DataFrame(vals,columns=["Concept","Density","Traffic mix","repetition",metric])
        ##Create one graph for every traffic mix
        for t_mix in traffic_mix_names:
            df1=metric_pandas_df[metric_pandas_df["Traffic mix"]==t_mix]
            fig=plt.figure()
            sns.boxplot(y=metric, x='Density', data=df1, palette=concepts_colours,hue='Concept').set(title=self.metrics_titles_dict[metric]+" for "+t_mix+" traffic mix",ylabel = metric+self.metrics_units_dict[metric])
            plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
            adjust_box_widths(fig, 0.5)
            plt.savefig(diagrams_path+"boxplots/by_traffic_mix/cap1_all_values_"+t_mix,bbox_inches='tight')
            plt.savefig(diagrams_path+"pdfs/boxplots/by_traffic_mix/cap1_all_values_"+t_mix+".pdf",bbox_inches='tight')
            plt.clf()
            
        ##Create one graph for every density
        for dens in density_names:
             df1=metric_pandas_df[metric_pandas_df["Density"]==dens]
             fig=plt.figure()
             sns.boxplot(y=metric, x='Traffic mix', data=df1, palette=concepts_colours,hue='Concept',width=0.7).set(title=self.metrics_titles_dict[metric]+" for "+dens+" density",ylabel = metric+self.metrics_units_dict[metric])
             plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
             adjust_box_widths(fig, 0.5)
             plt.savefig(diagrams_path+"boxplots/by_density/cap1_all_values_"+dens,bbox_inches='tight')
             plt.savefig(diagrams_path+"pdfs/boxplots/by_density/cap1_all_values_"+dens+".pdf",bbox_inches='tight')
             plt.clf()

    def all_values_boxplots_baseline(self,metric,dataframe):
        vals=[]
        for density in densities:
            for t_mix in traffic_mix:
                for conc in concepts:
                    for rep in repetitions:   
                        scenario_name=conc+density+t_mix+rep
                        try:
                            metric_values=dataframe[dataframe["Scenario_name"]==scenario_name][metric].values
                            for metric_list in metric_values:
                                for metric_value in metric_list:
                                    tmp=[self.concept_names_dict[conc],self.density_names_dict[density],self.traffic_mix_names_dict[t_mix],rep,metric_value]
                                    vals.append(tmp)
                        except:
                            #metric_value=240+random.randint(-5,5)
                            print("No value for scenario baseline",scenario_name,metric)
    
        metric_pandas_df=pd.DataFrame(vals,columns=["Concept","Density","Traffic mix","repetition",metric])
        ##Create one graph for every traffic mix
        for t_mix in traffic_mix_names:
            df1=metric_pandas_df[metric_pandas_df["Traffic mix"]==t_mix]
            fig=plt.figure()
            sns.boxplot(y=metric, x='Density', data=df1, palette=concepts_colours,hue='Concept').set(title=self.metrics_titles_dict[metric]+" for "+t_mix+" traffic mix",ylabel = metric+self.metrics_units_dict[metric])
            plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
            adjust_box_widths(fig, 0.5)
            plt.savefig(diagrams_path+"boxplots/by_traffic_mix/"+metric+"_"+t_mix,bbox_inches='tight')
            plt.savefig(diagrams_path+"pdfs/boxplots/by_traffic_mix/"+metric+"_"+t_mix+".pdf",bbox_inches='tight')
            plt.clf()
            
        ##Create one graph for every density
        for dens in density_names:
             df1=metric_pandas_df[metric_pandas_df["Density"]==dens]
             fig=plt.figure()
             sns.boxplot(y=metric, x='Traffic mix', data=df1, palette=concepts_colours,hue='Concept',width=0.7).set(title=self.metrics_titles_dict[metric]+" for "+dens+" density",ylabel = metric+self.metrics_units_dict[metric])
             plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
             adjust_box_widths(fig, 0.5)
             plt.savefig(diagrams_path+"boxplots/by_density/"+metric+"_"+dens,bbox_inches='tight')
             plt.savefig(diagrams_path+"pdfs/boxplots/by_density/"+metric+"_"+dens+".pdf",bbox_inches='tight')
             plt.clf()

    def metric_boxplots_wind(self,metric,dataframe,t_mix):
        vals=[]
        for density in densities:
            for wind in wind_uncertainties:
                for conc in concepts:
                    for rep in repetitions:
                        scenario_name=conc+density+t_mix+rep+wind
                        try:
                            metric_value=dataframe[dataframe["Scenario_name"]==scenario_name][metric].values[0]
                            tmp=[self.concept_names_dict[conc],self.density_names_dict[density],self.traffic_mix_names_dict[t_mix],rep,metric_value,self.uncertainties_names_dict[wind]]
                            vals.append(tmp)
                        except:
                            #metric_value=240+random.randint(-5,5)
                            print("No value for scenario",scenario_name,metric)
                        
        metric_pandas_df=pd.DataFrame(vals,columns=["Concept","Density","Traffic mix","repetition",metric,"Wind level"])
        
        ##Create one graph for every wind level
        for r in wind_uncertainties:
            df1=metric_pandas_df[metric_pandas_df["Wind level"]==self.uncertainties_names_dict[r]]
            fig=plt.figure()
            sns.boxplot(y=metric, x='Density', data=df1, palette=concepts_colours,hue='Concept').set(title=self.metrics_titles_dict[metric]+" for wind level "+self.uncertainties_names_dict[r],ylabel = metric+self.metrics_units_dict[metric])
            plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
            adjust_box_widths(fig, 0.5)
            if metric in percentage_metrics and scale_y:
                plt.ylim(0, 100)
            plt.savefig(diagrams_path+"boxplots/winds/by_wind_level/"+metric+"_"+r,bbox_inches='tight')
            plt.savefig(diagrams_path+"pdfs/boxplots/winds/by_wind_level/"+metric+"_"+r+".pdf",bbox_inches='tight')
            plt.clf()
            
        ##Create one graph for every density
        for dens in density_names:
             df1=metric_pandas_df[metric_pandas_df["Density"]==dens]
             fig=plt.figure()
             sns.boxplot(y=metric, x='Wind level', data=df1, palette=concepts_colours,hue='Concept',width=0.7).set(title=self.metrics_titles_dict[metric]+" for "+dens+" density",ylabel = metric+self.metrics_units_dict[metric])
             plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
             adjust_box_widths(fig, 0.5)
             if metric in percentage_metrics and scale_y:
                 plt.ylim(0, 100)
             plt.savefig(diagrams_path+"boxplots/winds/by_density/"+metric+"_"+dens,bbox_inches='tight')
             plt.savefig(diagrams_path+"pdfs/boxplots/winds/by_density/"+metric+"_"+dens+".pdf",bbox_inches='tight')
             plt.clf()
         
    def metric_boxplots_rogues(self,metric,dataframe,t_mix):
        vals=[]
        for density in densities:
            for rogue in rogue_uncertainties:
                for conc in concepts:
                    for rep in repetitions:
                        scenario_name=conc+density+t_mix+rep+rogue
                        try:
                            metric_value=dataframe[dataframe["Scenario_name"]==scenario_name][metric].values[0]
                            tmp=[self.concept_names_dict[conc],self.density_names_dict[density],self.traffic_mix_names_dict[t_mix],rep,metric_value,self.uncertainties_names_dict[rogue]]
                            vals.append(tmp)
                        except:
                            #metric_value=240+random.randint(-5,5)
                            print("No value for scenario",scenario_name,metric)
                        
    
        metric_pandas_df=pd.DataFrame(vals,columns=["Concept","Density","Traffic mix","repetition",metric,"Rogue_level"])
        
        ##Create one graph for every rogue level
        for r in rogue_uncertainties:
            df1=metric_pandas_df[metric_pandas_df["Rogue_level"]==self.uncertainties_names_dict[r]]
            fig=plt.figure()
            sns.boxplot(y=metric, x='Density', data=df1, palette=concepts_colours,hue='Concept').set(title=self.metrics_titles_dict[metric]+" for rogue level "+self.uncertainties_names_dict[r],ylabel = metric+self.metrics_units_dict[metric])
            plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
            adjust_box_widths(fig, 0.5)
            if metric in percentage_metrics and scale_y:
                plt.ylim(0, 100)
            plt.savefig(diagrams_path+"boxplots/rogues/by_rogue_level/"+metric+"_"+r,bbox_inches='tight')
            plt.savefig(diagrams_path+"pdfs/boxplots/rogues/by_rogue_level/"+metric+"_"+r+".pdf",bbox_inches='tight')
            plt.clf()
            
        ##Create one graph for every density
        for dens in density_names:
             df1=metric_pandas_df[metric_pandas_df["Density"]==dens]
             fig=plt.figure()
             sns.boxplot(y=metric, x='Rogue_level', data=df1, palette=concepts_colours,hue='Concept',width=0.7).set(title=self.metrics_titles_dict[metric]+" for "+dens+" density",ylabel = metric+self.metrics_units_dict[metric])
             plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
             adjust_box_widths(fig, 0.5)
             if metric in percentage_metrics and scale_y:
                 plt.ylim(0, 100)
             plt.savefig(diagrams_path+"pdfs/boxplots/rogues/by_density/"+metric+"_"+dens+".pdf",bbox_inches='tight')
             plt.savefig(diagrams_path+"boxplots/rogues/by_density/"+metric+"_"+dens,bbox_inches='tight')
             plt.clf()
   
    
    def special_metric_boxplots_rogues(self,metric,dataframe,t_mix):
        #Only for CAP3 and CAP4
        
        vals=[]
        
        for density in densities:
            for rogue in rogue_uncertainties[1:]:
                for conc in concepts:
                    for rep in repetitions:
                        scenario_name=conc+density+t_mix+rep+rogue
                        try:
                            metric_value=dataframe[dataframe["Scenario_name"]==scenario_name][metric].values[0]
                            tmp=[self.concept_names_dict[conc],self.density_names_dict[density],self.traffic_mix_names_dict[t_mix],rep,metric_value,self.uncertainties_names_dict[rogue]]
                            vals.append(tmp)
                        except:
                            #metric_value=240+random.randint(-5,5)
                            print("No value for scenario",scenario_name,metric)
                        
    
        metric_pandas_df=pd.DataFrame(vals,columns=["Concept","Density","Traffic mix","repetition",metric,"Rogue_level"])
    
        ##Create one graph for every rogue level
        for r in rogue_uncertainties[1:]:
            #print(uncertainties_names_dict[r])
            df1=metric_pandas_df[metric_pandas_df["Rogue_level"]==self.uncertainties_names_dict[r]]
            #print(df1.shape[0])
            fig=plt.figure()
            sns.boxplot(y=metric, x='Density', data=df1, palette=concepts_colours,hue='Concept').set(title=self.metrics_titles_dict[metric]+" for rogue level "+self.uncertainties_names_dict[r],ylabel = metric+self.metrics_units_dict[metric])
            plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
            adjust_box_widths(fig, 0.5)
            if metric in percentage_metrics and scale_y:
                plt.ylim(0, 100)
            plt.savefig(diagrams_path+"pdfs/boxplots/rogues/by_rogue_level/"+metric+"_"+r+".pdf",bbox_inches='tight')
            plt.savefig(diagrams_path+"boxplots/rogues/by_rogue_level/"+metric+"_"+r,bbox_inches='tight')
            plt.clf()
            
        ##Create one graph for every density
        for dens in density_names:
             df1=metric_pandas_df[metric_pandas_df["Density"]==dens]
             fig=plt.figure()
             sns.boxplot(y=metric, x='Rogue_level', data=df1, palette=concepts_colours,hue='Concept',width=0.7).set(title=self.metrics_titles_dict[metric]+" for "+dens+" density",ylabel = metric+self.metrics_units_dict[metric])
             plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
             adjust_box_widths(fig, 0.5)
             if metric in percentage_metrics and scale_y:
                 plt.ylim(0, 100)
             plt.savefig(diagrams_path+"pdfs/boxplots/rogues/by_density/"+metric+"_"+dens+".pdf",bbox_inches='tight')
             plt.savefig(diagrams_path+"boxplots/rogues/by_density/"+metric+"_"+dens,bbox_inches='tight')
             plt.clf()
   
    def metric_boxplots_priority(self,metric,dataframe,priority):
        ##Only for PRI3, PRI4, PRI5
        vals=[]
        for density in densities:
            for t_mix in traffic_mix:
                for conc in concepts:
                    for rep in repetitions:
                        scenario_name=conc+density+t_mix+rep
                        try:
                            metric_value=dataframe[(dataframe["Scenario_name"]==scenario_name)&(dataframe["Priority"]==priority)][metric].values[0]
                            tmp=[self.concept_names_dict[conc],self.density_names_dict[density],self.traffic_mix_names_dict[t_mix],rep,metric_value]
                            vals.append(tmp)
                        except:
                            #metric_value=240+random.randint(-5,5)
                            print("No value for scenario",scenario_name,metric)
    
        metric_pandas_df=pd.DataFrame(vals,columns=["Concept","Density","Traffic mix","repetition",metric])
        
        ##Create one graph for every traffic mix
        for t_mix in traffic_mix_names:
            df1=metric_pandas_df[metric_pandas_df["Traffic mix"]==t_mix]
            fig=plt.figure()
            sns.boxplot(y=metric, x='Density', data=df1, palette=concepts_colours,hue='Concept').set(title=self.metrics_titles_dict[metric]+" for traffic mix "+t_mix+" for priority "+priority,ylabel = metric+self.metrics_units_dict[metric])
            plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
            adjust_box_widths(fig, 0.5)
            if metric in percentage_metrics and scale_y:
                plt.ylim(0, 100)
            plt.savefig(diagrams_path+"pdfs/boxplots/priority/by_traffic_mix/"+metric+"_"+t_mix+".pdf",bbox_inches='tight')
            plt.savefig(diagrams_path+"boxplots/priority/by_traffic_mix/"+metric+"_"+t_mix,bbox_inches='tight')
            plt.clf()
            
        ##Create one graph for every density
        for dens in density_names:
             df1=metric_pandas_df[metric_pandas_df["Density"]==dens]
             fig=plt.figure()
             sns.boxplot(y=metric, x='Traffic mix', data=df1, palette=concepts_colours,hue='Concept',width=0.7).set(title=self.metrics_titles_dict[metric]+" density "+dens+" for priority "+priority,ylabel = metric+self.metrics_units_dict[metric])
             plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
             adjust_box_widths(fig, 0.5)
             if metric in percentage_metrics and scale_y:
                 plt.ylim(0, 100)
             plt.savefig(diagrams_path+"boxplots/priority/by_density/"+metric+"_"+dens,bbox_inches='tight')  
             plt.savefig(diagrams_path+"pdfs/boxplots/priority/by_density/"+metric+"_"+dens+".pdf",bbox_inches='tight')    
             plt.clf()
    
    def density_graph(self,density, t_mix,rep,dataframe):
        vals=[]
        for conc in concepts:
           scenario_name=conc+density+t_mix+rep
           data=[]
           time_stamp=0
           while time_stamp< 10800:
               time_stamp+=30
               try:
                   
                   metric_value=dataframe[(dataframe["scenario_name"]==scenario_name)&(dataframe["Time_stamp"]==time_stamp)]["Density"].values[0]
                   data.append(metric_value)
               except:
                   data.append(0)
                   #print("No value for scenario",scenario_name)
          
           vals.append(data)
    
        time_stamps=range(30,10830,30)
        plt.clf()
        
        plt.plot(time_stamps, vals[0],   label = 'Centralised', marker='o',color=concepts_colours[0], linewidth=3)
        plt.plot(time_stamps, vals[1],   label = 'Hyrbid',  marker='o',color=concepts_colours[1], linewidth=3)
        plt.plot(time_stamps, vals[2], label = 'Decentralised', marker='o',color=concepts_colours[2], linewidth=3)
        
        plt.xlabel('Time')
        plt.ylabel('Traffic density')
        plt.legend(loc='upper right')
    
        plt.title('Aircraft density for density '+self.density_names_dict[density])
        plt.savefig(diagrams_path+"density_graph/"+density+"_"+t_mix+"_"+rep) 
        plt.savefig(diagrams_path+"pdfs/density_graph/"+density+"_"+t_mix+"_"+rep+".pdf") 
        plt.clf()

       
    def density_constr_graph(self,density, t_mix,rep,dataframe):
        vals=[]
        for conc in concepts:
           scenario_name=conc+density+t_mix+rep
           data=[]
           time_stamp=0
           while time_stamp< 10800:
               time_stamp+=30
               try:
                   
                   metric_value=dataframe[(dataframe["scenario_name"]==scenario_name)&(dataframe["Time_stamp"]==time_stamp)]["Density_constrained"].values[0]
                   data.append(metric_value)
               except:
                   data.append(0)
                   #print("No value for scenario",scenario_name)
          
           vals.append(data)
    
        time_stamps=range(30,10830,30)
        plt.clf()
        
        plt.plot(time_stamps, vals[0],   label = 'Centralised', marker='o',color=concepts_colours[0], linewidth=3)
        plt.plot(time_stamps, vals[1],   label = 'Hyrbid',  marker='o',color=concepts_colours[1], linewidth=3)
        plt.plot(time_stamps, vals[2], label = 'Decentralised', marker='o',color=concepts_colours[2], linewidth=3)
        
        plt.xlabel('Time')
        plt.ylabel('Traffic density')
        plt.legend(loc='upper right')
    
        plt.title('Aircraft density in constrained for density '+self.density_names_dict[density])
        plt.savefig(diagrams_path+"density_graph/constrained-"+density+"_"+t_mix+"_"+rep) 
        plt.savefig(diagrams_path+"pdfs/density_graph/constrained-"+density+"_"+t_mix+"_"+rep+".pdf") 
        plt.clf()
       
       
    def createGraphs(self):       

        ##Load the metrics
        input_file=open(dills_path+"metrics_dataframe.dill", 'rb')
        scenario_metrics_df=dill.load(input_file)
        input_file.close()
        
        input_file=open(dills_path+"prio_metrics_dataframe.dill", 'rb')
        scenario_priority_metrics_df=dill.load(input_file)
        input_file.close()
        
        input_file=open(dills_path+"densitylog_dataframe.dill", 'rb')
        density_metrics_dataframe=dill.load(input_file)
        input_file.close()
        
        input_file=open(dills_path+"density_constrained_dataframe.dill", 'rb')
        density_constr_metrics_dataframe=dill.load(input_file)
        input_file.close()
        
        input_file=open(dills_path+"env3_1_metric_dataframe.dill", 'rb')
        env3_1_metric_dataframe=dill.load(input_file)
        input_file.close()
        #scenario_metrics_df.to_csv("metrics.csv")
        #scenario_priority_metrics_df.to_csv("scenario.csv")
        #density_metrics_dataframe.to_csv("density.csv")
        #env3_1_metric_dataframe.to_csv("env31.csv")


        ## Create the graphs
        boxplot_metrics=["AEQ1","AEQ1_1","AEQ2","AEQ2_1","AEQ3","AEQ4","AEQ5","AEQ5_1","CAP1","CAP2","EFF1","EFF2","EFF3","EFF4","EFF5","EFF6","ENV1","ENV2","ENV3_2","ENV4","SAF1","SAF1_2","SAF2","SAF2_1","SAF3","SAF4","SAF5","SAF6","SAF6_1","SAF6_2","SAF6_3","SAF6_4","SAF6_5","SAF6_6","SAF6_7","PRI1","PRI2"]
        
        boxplot_metrics_rogues=["CAP3","CAP4"]
        
        boxplot_metrics_priority=["PRI3","PRI4","PRI5"]
        

        
        for metric in boxplot_metrics:
             self.metric_boxplots_baseline(metric,scenario_metrics_df)
             self.metric_boxplots_rogues(metric,scenario_metrics_df,"40_") 
             self.metric_boxplots_wind(metric,scenario_metrics_df,"40_")
        
        for metric in boxplot_metrics_rogues:
             self.special_metric_boxplots_rogues(metric,scenario_metrics_df,"40_")
        
        for metric in boxplot_metrics_priority:
             for priority in ["1","2","3","4"]:
                 self.metric_boxplots_priority(metric,scenario_priority_metrics_df,priority)


        t_mix="40_"
        rep="0_"
        for dens in densities:
            self.density_graph(dens, t_mix,rep,density_metrics_dataframe)
            
        t_mix="40_"
        rep="0_"
        for dens in densities:
             self.density_constr_graph(dens, t_mix,rep,density_constr_metrics_dataframe)    

         
        self.all_values_boxplots_baseline("ENV3_1",env3_1_metric_dataframe)
        
        
        input_file=open("dills/flstlog_dataframe.dill", 'rb')
        flst_log_dataframe=dill.load(input_file)
        input_file.close()
        
        self.all_values_CAP1(flst_log_dataframe)
        #boxplot_metrics_all_values=["CAP1","EFF6","ENV1","SAF4","SAF5"]
