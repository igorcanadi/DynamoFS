class ServerStub:
    kvstore = dict()
    def put(key, value):
        self.kvstore[key] = value

    def get(key):
        return self.kvstore[key]
