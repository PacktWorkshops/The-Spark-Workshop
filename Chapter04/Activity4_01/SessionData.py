class SessionData:
    def __init__(self, uuid: str, id: int, frequency: int, group: int, registered: bool):
        self.uuid = uuid
        self.user_id = id
        self.frequency = frequency
        self.group = group
        self.registered = registered
