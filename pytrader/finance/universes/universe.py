class Universe:
    
    def __init__(
            self,
            id_type,
            name,
            start_date,
            end_date,
            start_time,
            end_time
            ):
        self.id_type = id_type
        self.name = name
        self.start_date = start_date
        self.end_date = end_date
        self.start_time = start_time
        self.end_time = end_time
        self.assets = {}
    
    def set_manager(self, manager):
        self.manager = manager 
        for a in self.assets.values():
            a.set_manager(manager)
