# M2_data_analysis_platform
# Scope

The platform was designed and developed as part of Metropolis2 to support the concepts’ evaluation and trade off. The platform’s main scope is to read through all the data generated from the Metropolis2 simulations , reorganise them, process them to produce more complex data, compute the metrics values for all scenarios and finally create the graphs and diagrams to represent the calculated values.

# Basic architecture – I/O

The platform inputs are the log files produced from the Metropolis2 simulations and the flight intentions files, describing the simulated scenarios. There are five types of log files: REGLOG, FLSTLOG, CONFLOG, LOSLOG and GEOLOG.
The platform loads the inputs files and saves the information of interest in pandas dataframes. There are two types of outputs produced; pandas dataframes saved as dill files and diagrams presenting the computed metrics. 

# Setup and run

The platform is written in python and it is based on the utilisation of pandas dataframes to save the data and the computed metrics. The required dependencies to set up the environment can be found at the requirements.txt  or the environment.yml. The platform has been tested for python 3.9.12 .

The input files (log files and flight intentions) for the platform are divided in four categories : Centralised, Decentralised, Hybrid and Fligth_intentions and they should be placed in the regarding folders in the platform_code/input_logs  folder. 
The output dataframes are saved as dill files in the platform_code/dills folder. The created graphs/diagrams are saved in the platform_code/output_graphs folder.

Before running the code you will have to check that all the necessary folders exist (if you cloned the repository/branch they should already be there but you will still need to add the input data = log files and flight intentions). The following checks will probably not be necessary, so you may skip them except if you get an error while trying to run the platform’s code. Here is a detailed description of what you will need to check:
1.	If you want to process new logs to create the dataframes: 

  a.	You need a folder called “input_logs”, which should contain four folders called: “Flight_intentions”, “Centralised”, “Hybrid”, “Decentralised” and the log files and flight_intention files should be placed in those folders. That folder is original in the “platform_code” folder in which the MainApp.py code is also located. If your “input_logs” folder is placed somewhere else you will have to change the path_to_logs variable at the beginning of the DataFrameCreator.py code to the path of your folder.
  
  b.	You will need an empty folder called “dills” in which the created dataframes will be saved and it should be placed in the “platform_code” folder in which the MainApp.py code is also located. 
  
2.	If you want to create graphs from already generated dataframes:

  a.	You will need folder called “dills” in which the dataframes of interest should be placed. That folder is original in the “platform_code” folder in which the GraphCreator.py code is also located. If your “dills” folder is placed somewhere else you will have to change the dills_path variable at the beginning of the GraphCreator.py code to the path of your folder.
  
  b.	To save the graph you will need a folder called “output_graphs” in the “platform_code” folder. The output_graphs folder has separate subfolders for each type of plot, so if you need to create a new output_graphs folder copy the original.


To run the overall system :
1.	Place your input data in the corresponding folder
2.	If you want to filter the log data for the first 1.5 hours simulation you may set the global variable time_filtering to True , in the DataframeCreator.py code.
3.	Run the MainApp.py 
4.	Press 1 and then Enter
5.	Insert the wanted number of threads, to run it as a single thread press 1 and then Enter
6.	After the MainApp.py code is completed check that it created the dataframes. There should be 9 dill files in the dills folder.
7.	Run the MainApp.py 
8.	Press 2 and then Enter
9.	If you want to compute and print sound exposure metrics
10.	Run the MainApp.py 
11.	Press 3 and then Enter
12.	Insert the wanted number of threads, to run it as a single thread press 1 and then Enter
13.	The statistics for the noise exposure for the 5 different densities should be printed.
