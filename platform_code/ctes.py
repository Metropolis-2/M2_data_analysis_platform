import configparser

class Ctes():

    def __init__(self):
        config = configparser.ConfigParser()
        config.read(r'config.txt')
        self.WEIGHT_PRIO1 = config.get('Priority', 'WEIGHT_PRIO1')
        self.WEIGHT_PRIO2 = config.get('Priority', 'WEIGHT_PRIO2')
        self.WEIGHT_PRIO3 = config.get('Priority', 'WEIGHT_PRIO3')
        self.WEIGHT_PRIO4 = config.get('Priority', 'WEIGHT_PRIO4')
        self.HEIGHT_LOW_LAYER = config.get('Efficiency', 'HEIGHT_LOW_LAYER')
        self.CANCELLATION_DELAY_LIMIT_emergency_mission = config.get('Access_and_equity', 'CANCELLATION_DELAY_LIMIT_emergency_mission')
        self.CANCELLATION_DELAY_LIMIT_delivery_mission = config.get('Access_and_equity', 'CANCELLATION_DELAY_LIMIT_delivery_mission')
        self.CANCELLATION_DELAY_LIMIT_loitering_mission = config.get('Access_and_equity', 'CANCELLATION_DELAY_LIMIT_loitering_mission')
        self.AUTONOMY_MP20 = config.get('Access_and_equity', 'AUTONOMY_MP20')
        self.AUTONOMY_MP30 = config.get('Access_and_equity', 'AUTONOMY_MP30')
        self.THRESHOLD_DELAY = config.get('Access_and_equity', 'THRESHOLD_DELAY')

if __name__ == "__main__":
    ctes = Ctes()
    print("WEIGHT_PRIO1: ",ctes.WEIGHT_PRIO1)
    print("WEIGHT_PRIO2: ", ctes.WEIGHT_PRIO2)
    print("WEIGHT_PRIO3: ", ctes.WEIGHT_PRIO3)
    print("WEIGHT_PRIO4: ", ctes.WEIGHT_PRIO4)
    print("HEIGHT_LOW_LAYER: ", ctes.HEIGHT_LOW_LAYER)
    print("CANCELLATION_DELAY_LIMIT_emergency_mission: ", ctes.CANCELLATION_DELAY_LIMIT_emergency_mission)
    print("CANCELLATION_DELAY_LIMIT_delivery_mission: ", ctes.CANCELLATION_DELAY_LIMIT_delivery_mission)
    print("CANCELLATION_DELAY_LIMIT_loitering_mission: ", ctes.CANCELLATION_DELAY_LIMIT_loitering_mission)
    print("AUTONOMY_MP20: ", ctes.AUTONOMY_MP20)
    print("AUTONOMY_MP30: ", ctes.AUTONOMY_MP30)
    print("THRESHOLD_DELAY: ", ctes.THRESHOLD_DELAY)
