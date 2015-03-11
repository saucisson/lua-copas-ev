local socket = require "socket"
local copev  = require "copas.ev"

copev.compatibility = false

local server = socket.bind ("127.0.0.1", 8080)
--local profiler = require "profiler"
--profiler:start "data.ou"

copev.addserver (server, function (skt)
  while true do
    local message = skt:receive "*l"
    if message == nil then
      break
    elseif message == "stop" then
--      profiler.stop ()
      copev.removeserver (server)
    else
      skt:send (message .. "\n")
    end
  end
end)

copev.loop ()
