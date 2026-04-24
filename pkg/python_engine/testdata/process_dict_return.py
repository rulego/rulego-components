import json


def Process(msg, metadata, msgType, dataType):
    data = msg if isinstance(msg, dict) else json.loads(msg)
    data["source"] = "dict_return"
    return {"msg": json.dumps(data), "msgType": "DICT_RESULT"}
