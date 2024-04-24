import json
from commands import StateMachineCommand, HashtablePost


class LogEntry:
    def __init__(self, term: int, command: StateMachineCommand):
        self.term = term
        self.command = command

#check instatnse 
class LogEntryEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, LogEntry):
            return {'term': obj.term, 'command': self.default(obj.command)}
        if isinstance(obj, HashtablePost):
            return {'command': "post", "data": {"key": obj.key, "value": obj.value}}
        return super().default(obj)

#methos can be used without creat class, where data is dictionary, in if check value 
class LogEntryDecoder:
    @staticmethod
    def decode_my_class(obj: list[dict]):
        res = []
        for i in obj:
            term = i["term"]

            command = i["command"]
            if command["command"] == "post":
                command = HashtablePost(command["data"]["key"], command["data"]["value"])
            res.append(LogEntry(term, command))
        return res
