local socket = require "socket"
local copev  = require "copas.ev"

copev.compatibility = false

local server = socket.bind ("127.0.0.1", 8080)
server:setoption ("keepalive", true)
server:setoption ("reuseaddr", true)
server:setoption ("tcp-nodelay", true)

--local profiler = require "profiler"
--profiler:start "data.out"

local answer = [[
HTTP/1.0 200 OK
Server: Coevas
Content-Type: text/plain
Content-Length: 14

Hello, World!
]]

--local count = 0
copev.addserver (server, function (skt)
--  if count == 10000 then
--    copev.removeserver (server)
--  end
  while true do
    local message = skt:receive "*l"
    if message == nil then
      return
    elseif message == "" then
--      count = count+1
      skt:send (answer)
      return
    end
  end
end)

--ProFi = require "ProFi"
--ProFi:start ()
copev.loop  ()
--ProFi:stop  ()
--ProFi:writeReport "report2.txt"
