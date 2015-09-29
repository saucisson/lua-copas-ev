local socket = require "socket"
local copev  = require "copas.ev"

copev.compatibility = false

local server   = socket.bind ("127.0.0.1", 8080)
--local profiler = require "profiler"
--profiler:start "data.out"

copev.addserver (server, function (skt)
  repeat
    local message = skt:receive "*l"
    if message == nil then
      break
    elseif message ~= "stop" then
      skt:send (message .. "\n")
    end
  until message == "stop"
--  profiler.stop ()
end)

copev.loop ()
