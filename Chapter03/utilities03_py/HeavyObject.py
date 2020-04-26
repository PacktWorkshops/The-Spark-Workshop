class HeavyObject:
    def __init__(self, prefix):
        print(prefix + ' => new heavy object created')
        self.prefix = prefix
    def get_id(self):
        return id(self)