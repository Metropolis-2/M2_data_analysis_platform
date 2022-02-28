import time

class Utils():

    def __init__(self, ):
        self.MINUTS_CONVERSION = 3600
        self.SECONDS_CONVERSION = 60

    def get_sec(self, time_str):  # Convert hh:mm:ss format to tiemstamp seconds
        h, m, s = time_str.split(':')
        return int(h) * self.MINUTS_CONVERSION + int(m) * self.SECONDS_CONVERSION + int(s)

    def get_min(self, time_int):  # Convert tiemstamp seconds to hh:mm:ss
        timestamp = time.strftime('%H:%M:%S', time.gmtime(time_int))
        return timestamp

    def mergeDict(self, dict1, dict2):
        ''' Merge dictionaries and keep values of common keys in list'''
        dict3 = {**dict1, **dict2}
        for key, value in dict3.items():
            if key in dict1 and key in dict2:
                dict3[key] = value + dict1[key] #ATENTION: #both dict should be like: dict1: [k1] = [v1,v11,...], dict2: [k2] = [v2,...]
        return dict3