import json


def Process(msg, metadata, msgType, dataType, vars={}, globalProps={}):
    data = msg if isinstance(msg, dict) else json.loads(msg)
    data["server"] = vars.get("server", "unknown")
    data["env"] = vars.get("env", "unknown")
    metadata["processedBy"] = vars.get("server", "unknown")
    return json.dumps(data), metadata, msgType
