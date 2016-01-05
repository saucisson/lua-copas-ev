require "busted.runner" ()

local assert = require "luassert"

describe ("the copas.ev module", function ()

  it ("can be required", function ()
    assert.has.no.error (function ()
      require "copas.ev"
    end)
  end)

  it ("allows to run a client/server example", function ()
    local socket = require "socket"
    local copev  = require "copas.ev"
    local server = socket.bind ("127.0.0.1", 8080)
    copev.addserver (server, function (skt)
      repeat
        local message = skt:receive "*l"
        if message == nil then
          break
        elseif message ~= "stop" then
          skt:send (message .. "\n")
        end
      until message == "stop"
    end)
    copev.addthread (function ()
      local skt = copev.wrap (socket.tcp ())
      skt:connect ("127.0.0.1", 8080)
      do
        skt:send "message\n"
        local answer = skt:receive "*l"
        assert (answer == "message")
      end
      do
        skt:send "stop\n"
        local answer = skt:receive "*l"
        assert (answer == nil)
      end
      copev.removeserver (server)
    end)
    copev.loop ()
  end)

  it ("allows TLS connections", function ()
    local socket = require "socket"
    local ssl    = require "ssl"
    local copev  = require "copas.ev"
    local query = [[
GET / HTTP/1.1
User-Agent: copas.ev
Accept: */*

]]
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
      local message = skt:receive "*l"
      assert (message:match "HTTP/%d.%d%s+.*")
      skt:close ()
    end)
    copev.loop ()
  end)

end)
