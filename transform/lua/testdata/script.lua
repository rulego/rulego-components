-- Define a Transform function that accepts three arguments: msg, metadata, msgType
-- Transform the message based on msg, metadata, and msgType, then return the modified values
function Transform(msg, metadata, msgType)
  -- If msg has a temperature field, it represents a temperature value
  if msg.temperature then
    -- Convert the temperature value from Celsius to Fahrenheit
    msg.temperature = msg.temperature * 1.8 + 32
    -- Add a field to metadata that represents the temperature unit
    metadata.unit = "F"
  end
  -- Return the modified msg, metadata, and msgType
  return msg, metadata, msgType
end
