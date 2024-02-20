-- 定义一个 Transform 函数，接受三个参数：msg, metadata, msgType
-- 根据 msg, metadata, msgType 来转换消息，返回修改后的值
function Transform(msg, metadata, msgType)
  -- 如果 msg 中有 temperature 字段，表示温度值
  if msg.temperature then
    -- 将温度值从摄氏度转换为华氏度
    msg.temperature = msg.temperature * 1.8 + 32
    -- 在 metadata 中添加一个字段，表示温度单位
    metadata.unit = "F"
  end
  -- 返回修改后的 msg, metadata, msgType
  return msg, metadata, msgType
end
