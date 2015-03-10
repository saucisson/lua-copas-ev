local socket = require "socket"
local copev  = require "copas.ev"

local server = socket.bind ("127.0.0.1", 8080)
--local profiler = require "profiler"
--profiler:start "data.ou"

local answer = [[
HTTP/1.0 200 OK
Server: Coevas
Content-Type: text/plain
Content-Length: 8

Hello, World!
]]

copev.addserver (server, function (skt)
  local message = skt:receive "*l"
  if message ~= nil then
    skt:send (answer)
  end
end)

copev.loop ()