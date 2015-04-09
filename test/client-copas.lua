local socket = require "socket"
local copas  = require "copas"

local nb_threads    = 500
local nb_iterations = 100
local finished      = 0

local nb = 0
local start = socket.gettime ()

for i = 1, nb_threads do
  copas.addthread (function ()
    local skt = copas.wrap (socket.tcp ())
    skt:connect ("127.0.0.1", 8080)
    for j = 1, nb_iterations do
      skt:send "message\n"
      local answer = skt:receive "*l"
      assert (answer == "message")
      nb = nb + 1
    end
    finished = finished + 1
    if finished == nb_threads then
      assert (nb == nb_threads * nb_iterations)
      skt:send "stop\n"
      local average = math.floor (nb / (socket.gettime () - start))
      print ("# send/receive per second:", average)
--      profiler:stop ()
    end
  end)
end

copas.loop ()