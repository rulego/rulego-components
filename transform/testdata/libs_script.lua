function Transform(msg, metadata, msgType)
local crypto = require("crypto")
-- md5
metadata.md5 = crypto.md5("1\n")
  -- 返回修改后的 msg, metadata, msgType
  return msg, metadata, msgType
end
