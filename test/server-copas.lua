local socket = require "socket"
local copas  = require "copas"

local server = socket.bind ("127.0.0.1", 8080)

copas.addserver (server, function (skt)
  skt = copas.wrap (skt)
  repeat
    local message = skt:receive "*l"
    if message == nil then
      break
    elseif message ~= "stop" then
      skt:send (message .. "\n")
    end
  until message == "stop"
end)

copas.loop ()