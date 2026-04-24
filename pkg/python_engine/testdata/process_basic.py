import json


def Process(msg, metadata, msgType, dataType):
    data = msg if isinstance(msg, dict) else json.loads(msg)
    data["processed"] = True
    return json.dumps(data), metadata, msgType
