local socket = require "socket"
local copev  = require "copas.ev"

copev.compatibility = false

local server = socket.bind ("127.0.0.1", 8080)
--local profiler = require "profiler"
--profiler:start "data.out"

local answer = [[
HTTP/1.0 200 OK
Server: Coevas
Content-Type: text/plain
Content-Length: 14

Hello, World!
]]

copev.addserver (server, function (skt)
  while true do
    local message = skt:receive "*l"
    if message == nil then
      return
    elseif message == "" then
      skt:send (answer)
      return
    end
  end
end)

copev.loop ()