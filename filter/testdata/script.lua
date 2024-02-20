-- 定义一个 Filter 函数，接受三个参数：msg, metadata, msgType
function Filter(msg, metadata, msgType)
   return msg.temperature > 50
end