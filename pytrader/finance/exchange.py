

class Exchange:

    # hours should be a list of tuples ([(start time, end time), ...])
    # times should be str 'HH:MM'
    def __init__(self, manager, name, hours = [('9:30', '16:00')]):
        self.manager = manager
        self.name = name
        self.hours = hours
        self.times = self.__clean_hours(hours)
    
    def __clean_hours(self, hours):
        cleaned = []
        
        for times in hours:
            start, end = times
            cleaned.append( (int(start.replace(':','')), int(end.replace(':',''))) )

        return cleaned

    def __check_time(self, time):
        
        now_hour = self.manager.now.hour
        now_minute = self.manager.now.minute
        now_int = int(f'{now_hour}{now_minute:02d}')
        start, end = time

        return start <= now_int and end >= now_int

    @property
    def is_open(self):
        for time in self.times:
            check = self.__check_time(time)
            if not check:
                return False
        return True
