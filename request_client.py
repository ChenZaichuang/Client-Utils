import requests


class NewRequest:

    def __init__(self, session, method, args, kwargs):
        self.session = session
        self.method = method
        self.args = args
        self.kwargs = kwargs
        self.response = None

    def __enter__(self):
        self.response = getattr(self.session, self.method)(*self.args, **self.kwargs)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()
        self.response.close()

    @classmethod
    def get(cls, *args, **kwargs):
        session = requests.sessions.Session()
        return NewRequest(session, 'get', args, kwargs)

    @classmethod
    def post(cls, *args, **kwargs):
        session = requests.sessions.Session()
        return NewRequest(session, 'get', args, kwargs)

    @classmethod
    def put(cls, *args, **kwargs):
        session = requests.sessions.Session()
        return NewRequest(session, 'get', args, kwargs)

    @classmethod
    def delete(cls, *args, **kwargs):
        session = requests.sessions.Session()
        return NewRequest(session, 'get', args, kwargs)
