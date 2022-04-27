# -*- coding: utf-8 -*-
"""
Created on Fri Feb 11 16:33:17 2022
@author: nipat
"""
import os
import pandas as pd


import DataframeCreator




class MainClass():
    def __init__(self, *args, **kwargs):


        self.dataframe_creator = DataframeCreator.DataframeCreator( ) # the dataframes are created in  the init function now


if __name__ == "__main__":
    #TODO: This call to the main() function should be repeated for each of the scenarios in a loop or option menu
    #MainClass().main()
    m=MainClass()