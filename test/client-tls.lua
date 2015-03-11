local socket = require "socket"
local ssl    = require "ssl"
local copev  = require "copas.ev"

local nb_threads    = 1
local nb_iterations = 1
local finished      = 0

local nb = 0
--local profiler = require "profiler"
--profiler:start "data.ou"
local start = socket.gettime ()

local query = [[
GET / HTTP/1.1
User-Agent: curl/7.38.0
Host: www.google.com
Accept: */*

]]
print (query)

for i = 1, nb_threads do
  copev.addthread (function ()
    local skt = copev.wrap (socket.tcp ())
    skt:connect ("www.google.com", 443)
    skt = ssl.wrap (skt, {
      mode     = "client",
      protocol = "tlsv1_2",
    })
    skt:dohandshake () 
    skt:send (query)
    skt:settimeout (1)
    repeat
      local message = skt:receive "*l"
      print (message)
    until message == nil
    os.exit (0)
  end)
end

copev.loop ()