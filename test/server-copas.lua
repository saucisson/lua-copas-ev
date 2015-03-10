local socket = require "socket"
local copas  = require "copas.copas"

local server = socket.bind ("127.0.0.1", 8080)

copas.addserver (server, function (skt)
  skt = copas.wrap (skt)
  while true do
    local message = skt:receive "*l"
    if message == nil then
      break
    else
      skt:send (message .. "\n")
    end
  end
end)

copas.loop ()