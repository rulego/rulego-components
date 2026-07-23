-- Define a Filter function that accepts three arguments: msg, metadata, msgType
function Filter(msg, metadata, msgType)
   return msg.temperature > 50
end
