function Transform(msg, metadata, msgType)
local crypto = require("crypto")
-- md5
metadata.md5 = crypto.md5("1\n")
  -- Return the modified msg, metadata, and msgType
  return msg, metadata, msgType
end
