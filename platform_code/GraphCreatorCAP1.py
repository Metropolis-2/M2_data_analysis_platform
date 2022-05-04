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




concepts=["1_","2_","3_"]
concept_names=["Centralised","Hybrid","Decentralised"]
concept_names_dict={}
for i in range(len(concepts)):
    concept_names_dict[concepts[i]]=concept_names[i]

densities=["very_low_","low_","medium_","high_","ultra_"]
density_names=["very_low","low","medium","high","very_high"]
density_names_dict={}
for i in range(len(densities)):
    density_names_dict[densities[i]]=density_names[i]

traffic_mix=["40_","50_","60_"]
traffic_mix_names=["40%","50%","60%"]
traffic_mix_names_dict={}
for i in range(len(traffic_mix)):
    traffic_mix_names_dict[traffic_mix[i]]=traffic_mix_names[i]

repetitions=["0_","1_","2_","3_","4_","5_","6_","7_","8_"]

uncertainties=["","R1","R2","R3","W1","W3","W5"]
rogue_uncertainties=["","R1","R2","R3"]
wind_uncertainties=["","W1","W3","W5"]
uncertainties_names=["No uncertainty","R1","R2","R3","W1","W3","W5"]
uncertainties_names_dict={}
for i in range(len(uncertainties)):
    uncertainties_names_dict[uncertainties[i]]=uncertainties_names[i]

concepts_colours=['r','g','b']


    
metrics_title=["CAP1"]    
boxplot_metrics=["CAP1"]


metrics_titles_dict={}
i=0
for m in boxplot_metrics:
    metrics_titles_dict[m]=metrics_title[i]
    i+=1


def metric_boxplots_baseline(metric,dataframe):
    vals=[]
    for density in densities:
        for t_mix in traffic_mix:
            for conc in concepts:
                for rep in repetitions:   
                    scenario_name=conc+density+t_mix+rep
                    try:
                        metric_values=dataframe[dataframe["Scenario_name"]==scenario_name][metric].values
                        for metric_value in metric_values:
                            tmp=[concept_names_dict[conc],density_names_dict[density],traffic_mix_names_dict[t_mix],rep,metric_value]
                            vals.append(tmp)
                    except:
                        #metric_value=240+random.randint(-5,5)
                        print("No value for scenario baseline",scenario_name,metric)
                    
                    
    
    metric_pandas_df=pd.DataFrame(vals,columns=["Concept","Density","Traffic mix","repetition",metric])
    
    ##Create one graph for every traffic mix
    for t_mix in traffic_mix_names:
        df1=metric_pandas_df[metric_pandas_df["Traffic mix"]==t_mix]
        fig=plt.figure()
        sns.boxplot(y=metric, x='Density', data=df1, palette=concepts_colours,hue='Concept').set(title=metric+" for "+t_mix+" traffic mix")
        plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
        adjust_box_widths(fig, 0.5)
        plt.savefig(diagrams_path+"boxplots/by_traffic_mix/"+metric+"_"+t_mix,bbox_inches='tight')
        plt.savefig(diagrams_path+"pdfs/boxplots/by_traffic_mix/"+metric+"_"+t_mix+".pdf",bbox_inches='tight')
        plt.clf()
        
    ##Create one graph for every density
    for dens in density_names:
         df1=metric_pandas_df[metric_pandas_df["Density"]==dens]
         fig=plt.figure()
         sns.boxplot(y=metric, x='Traffic mix', data=df1, palette=concepts_colours,hue='Concept',width=0.7).set(title=metric+" for "+dens+" density")
         plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
         adjust_box_widths(fig, 0.5)
         plt.savefig(diagrams_path+"boxplots/by_density/"+metric+"_"+dens,bbox_inches='tight')
         plt.savefig(diagrams_path+"pdfs/boxplots/by_density/"+metric+"_"+dens+".pdf",bbox_inches='tight')
         plt.clf()


def metric_boxplots_wind(metric,dataframe,t_mix):
    vals=[]
    for density in densities:
        for wind in wind_uncertainties:
            for conc in concepts:
                for rep in repetitions:
                    scenario_name=conc+density+t_mix+rep+wind
                    try:
                        metric_values=dataframe[dataframe["Scenario_name"]==scenario_name][metric].values
                        for metric_value in metric_values:
                            tmp=[concept_names_dict[conc],density_names_dict[density],traffic_mix_names_dict[t_mix],rep,metric_value,uncertainties_names_dict[wind]]
                            vals.append(tmp)
                    except:
                        #metric_value=240+random.randint(-5,5)
                        print("No value for scenario",scenario_name,metric)
                    
                    
 
    
    metric_pandas_df=pd.DataFrame(vals,columns=["Concept","Density","Traffic mix","repetition",metric,"Wind level"])
    
    ##Create one graph for every wind level
    for r in wind_uncertainties:
        df1=metric_pandas_df[metric_pandas_df["Wind level"]==uncertainties_names_dict[r]]
        fig=plt.figure()
        sns.boxplot(y=metric, x='Density', data=df1, palette=concepts_colours,hue='Concept').set(title=metric+" for wind level "+uncertainties_names_dict[r])
        plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
        adjust_box_widths(fig, 0.5)
        plt.savefig(diagrams_path+"boxplots/winds/by_wind_level/"+metric+"_"+r,bbox_inches='tight')
        plt.savefig(diagrams_path+"pdfs/boxplots/winds/by_wind_level/"+metric+"_"+r+".pdf",bbox_inches='tight')
        plt.clf()
        
    ##Create one graph for every density
    for dens in density_names:
         df1=metric_pandas_df[metric_pandas_df["Density"]==dens]
         fig=plt.figure()
         sns.boxplot(y=metric, x='Wind level', data=df1, palette=concepts_colours,hue='Concept',width=0.7).set(title=metric+" for "+dens+" density")
         plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
         adjust_box_widths(fig, 0.5)
         plt.savefig(diagrams_path+"boxplots/winds/by_density/"+metric+"_"+dens,bbox_inches='tight')
         plt.savefig(diagrams_path+"pdfs/boxplots/winds/by_density/"+metric+"_"+dens+".pdf",bbox_inches='tight')
         plt.clf()
         
def metric_boxplots_rogues(metric,dataframe,t_mix):
    vals=[]
    for density in densities:
        for rogue in rogue_uncertainties:
            for conc in concepts:
                for rep in repetitions:
                    scenario_name=conc+density+t_mix+rep+rogue
                    try:
                        metric_values=dataframe[dataframe["Scenario_name"]==scenario_name][metric].values
                        for metric_value in metric_values:
                            tmp=[concept_names_dict[conc],density_names_dict[density],traffic_mix_names_dict[t_mix],rep,metric_value,uncertainties_names_dict[rogue]]
                            vals.append(tmp)
                    except:
                        #metric_value=240+random.randint(-5,5)
                        print("No value for scenario",scenario_name,metric)
                    
                    

    
    metric_pandas_df=pd.DataFrame(vals,columns=["Concept","Density","Traffic mix","repetition",metric,"Rogue_level"])
    
    ##Create one graph for every rogue level
    
    for r in rogue_uncertainties:
        df1=metric_pandas_df[metric_pandas_df["Rogue_level"]==uncertainties_names_dict[r]]
        fig=plt.figure()
        sns.boxplot(y=metric, x='Density', data=df1, palette=concepts_colours,hue='Concept').set(title=metric+" for rogue level "+uncertainties_names_dict[r])
        plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
        adjust_box_widths(fig, 0.5)
        plt.savefig(diagrams_path+"boxplots/rogues/by_rogue_level/"+metric+"_"+r,bbox_inches='tight')
        plt.savefig(diagrams_path+"pdfs/boxplots/rogues/by_rogue_level/"+metric+"_"+r+".pdf",bbox_inches='tight')
        plt.clf()
        
    ##Create one graph for every density
    for dens in density_names:
         df1=metric_pandas_df[metric_pandas_df["Density"]==dens]
         fig=plt.figure()
         sns.boxplot(y=metric, x='Rogue_level', data=df1, palette=concepts_colours,hue='Concept',width=0.7).set(title=metric+" for "+dens+" density")
         plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
         adjust_box_widths(fig, 0.5)
         plt.savefig(diagrams_path+"pdfs/boxplots/rogues/by_density/"+metric+"_"+dens+".pdf",bbox_inches='tight')
         plt.savefig(diagrams_path+"boxplots/rogues/by_density/"+metric+"_"+dens,bbox_inches='tight')
         plt.clf()
   
    
def special_metric_boxplots_rogues(metric,dataframe,t_mix):
    #Only for CAP3 and CAP4
    ##TODO should taht be for mutliple traffix mixes as well?
    
    vals=[]
    
    for density in densities:
        for rogue in rogue_uncertainties[1:]:
            for conc in concepts:
                for rep in repetitions:
                    scenario_name=conc+density+t_mix+rep+rogue
                    try:
                        metric_values=dataframe[dataframe["Scenario_name"]==scenario_name][metric].values
                        for metric_value in metric_values:
                            tmp=[concept_names_dict[conc],density_names_dict[density],traffic_mix_names_dict[t_mix],rep,metric_value,uncertainties_names_dict[rogue]]
                            vals.append(tmp)
                    except:
                        #metric_value=240+random.randint(-5,5)
                        print("No value for scenario",scenario_name,metric)
                    
                    

    
    metric_pandas_df=pd.DataFrame(vals,columns=["Concept","Density","Traffic mix","repetition",metric,"Rogue_level"])
    #print(metric_pandas_df)
    ##Create one graph for every rogue level
    for r in rogue_uncertainties[1:]:
        #print(uncertainties_names_dict[r])
        df1=metric_pandas_df[metric_pandas_df["Rogue_level"]==uncertainties_names_dict[r]]
        #print(df1.shape[0])
        fig=plt.figure()
        sns.boxplot(y=metric, x='Density', data=df1, palette=concepts_colours,hue='Concept').set(title=metric+" for rogue level "+uncertainties_names_dict[r])
        plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
        adjust_box_widths(fig, 0.5)
        plt.savefig(diagrams_path+"pdfs/boxplots/rogues/by_rogue_level/"+metric+"_"+r+".pdf",bbox_inches='tight')
        plt.savefig(diagrams_path+"boxplots/rogues/by_rogue_level/"+metric+"_"+r,bbox_inches='tight')
        plt.clf()
        
    ##Create one graph for every density
    for dens in density_names:
         df1=metric_pandas_df[metric_pandas_df["Density"]==dens]
         fig=plt.figure()
         sns.boxplot(y=metric, x='Rogue_level', data=df1, palette=concepts_colours,hue='Concept',width=0.7).set(title=metric+" for "+dens+" density")
         plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
         adjust_box_widths(fig, 0.5)
         plt.savefig(diagrams_path+"pdfs/boxplots/rogues/by_density/"+metric+"_"+dens+".pdf",bbox_inches='tight')
         plt.savefig(diagrams_path+"boxplots/rogues/by_density/"+metric+"_"+dens,bbox_inches='tight')
         plt.clf()


       
       

##Load the FLST
input_file=open("dills/flstlog_dataframe.dill", 'rb')
flst_log_dataframe=dill.load(input_file)
input_file.close()



## Create the graphs
boxplot_metrics=["CAP1"]

metric_boxplots_baseline("Arrival_delay",flst_log_dataframe)
metric_boxplots_rogues("Arrival_delay",flst_log_dataframe,"40_") 
metric_boxplots_wind("Arrival_delay",flst_log_dataframe,"40_")


    
