                 require "copas"
local ev       = require "ev"
coroutine.make = require "coroutine.make"

local Coevas = {}

local Socket = {
  Tcp = {},
  Udp = {},
}

function Coevas.__index (coevas, key)
  local found = Coevas [key]
  if not found then
    return nil
  end
  local result = found
  if type (found) == "function" then
    result = function (...)
      return found (coevas, ...)
    end
  end
  coevas [key] = result
  return result
end

function Coevas.new ()
  local coevas = {
    autoclose    = true,
    compatibilty = false,
    schedule     = Coevas.default_schedule,
    _coroutine   = coroutine.make (),
    _loop        = ev.Loop.new (),
    _errors      = {},
    _awaken      = {},
    _sleeping    = {},
    _servers     = {},
    _sockets     = {},
    _idle        = nil,
  }
  coevas._idle = ev.Idle.new (function (loop, idle)
    local ok = coevas.step ()
    if not ok then
      idle:stop (loop)
      if coevas.finished () then
        loop:unloop ()
      end
    end
  end)
  return setmetatable (coevas, Coevas)
end

-- Replace `copas` by its `ev` variant in subsequent `require`.
function Coevas.make_default (coevas)
  package.loaded .copas = nil
  package.preload.copas = function ()
    return coevas
  end
end

-- Socket Operations
-- -----------------

function Coevas.wrap (coevas, socket, sslparams)
  local mt = getmetatable (socket)
  if mt == Socket.Tcp.__metatable or mt == Socket.Udp.__metatable then
    assert (socket._coevas == coevas)
    return socket
  else
    local prefix = string.sub (tostring (socket), 1, 3)
    if     prefix == "udp" then
      return setmetatable ({
        _coevas = coevas,
        _socket = socket,
        _ssl    = sslparams,
      }, Socket.Udp)
    elseif prefix == "tcp" then
      return setmetatable ({
        _coevas = coevas,
        _socket = socket,
        _ssl    = sslparams,
      }, Socket.Tcp)
    elseif prefix == "SSL" then
      return setmetatable ({
        _coevas = coevas,
        _socket = socket,
        _ssl    = sslparams,
      }, Socket.Tcp)
    end
  end
end

function Coevas.connect (coevas, socket, address, port)
  local co = coevas._coroutine.running ()
  local mt = getmetatable (socket)
  local sslparams
  if mt == Socket.Tcp.__metatable or mt == Socket.Udp.__metatable then
    assert (socket._coevas == coevas)
    sslparams = socket._ssl
    socket    = socket._socket
  end
  socket:settimeout (0)
  local on_write = ev.IO.new (function (loop, watcher)
    watcher:stop (loop)
    coevas.wakeup (co)
  end, socket:getfd (), ev.WRITE)
  repeat
    local ok, err = socket:connect (address, port)
    if err == "timeout" then
      on_write:start (coevas._loop)
      coevas.sleep (-math.huge)
    elseif sslparams then
      break
    else
      return ok, err
    end
  until false
  return coevas.dohandshake (socket, sslparams)
end

function Coevas.dohandshake (coevas, socket, sslparams)
  local ssl = require "ssl"
  local mt  = getmetatable (socket)
  if mt == Socket.Tcp.__metatable or mt == Socket.Udp.__metatable then
    assert (socket._coevas == coevas)
    sslparams = sslparams or socket._ssl
  else
    socket = coevas.wrap (socket)
  end
  local ok, err
  socket, err = ssl.wrap (socket, sslparams)
  if not socket then
    error (err)
  end
  ok, err = socket:dohandshake ()
  if not ok then
    error (err)
  end
  return socket
end

function Coevas.handler (coevas, connhandler, sslparams)
  return function (socket, ...) 
    socket = coevas.wrap (socket)
    if sslparams then
      socket:dohandshake (sslparams)
    end
    return connhandler (socket, ...)
  end
end

function Coevas.flush ()
end

function Coevas.setoption (coevas, socket, option, value)
  local mt = getmetatable (socket)
  if mt == Socket.Tcp.__metatable or mt == Socket.Udp.__metatable then
    assert (socket._coevas == coevas)
    socket = socket._socket
  end
  return socket:setoption (option, value)
end

function Coevas.receive (coevas, socket, pattern, part)
  local co      = coevas._coroutine.running ()
  local mt      = getmetatable (socket)
  local timeout = 1
  if mt == Socket.Tcp.__metatable or mt == Socket.Udp.__metatable then
    assert (socket._coevas == coevas)
    socket  = socket._socket
    timeout = socket._timeout
  end
  socket:settimeout (0)
  local timed_out = (timeout == 0)
  if timeout and timeout > 0 then
    local on_timeout = ev.Timer.new (function (loop, watcher)
      watcher:stop (loop)
      coevas.wakeup (co)
      timed_out = true
    end, timeout)
    on_timeout:start (coevas._loop)
  end
  pattern = pattern or "*l"
  local s, err
  repeat
    s, err, part = socket:receive (pattern, part)
    if math.random (100) > 90 then
      coevas.pass ()
    end
    if timed_out then
      return s, err, part
    elseif err == "timeout" or err == "wantread" then
      local on_read = ev.IO.new (function (loop, watcher)
        watcher:stop (loop)
        coevas.wakeup (co)
      end, socket:getfd (), ev.READ)
      on_read:start (coevas._loop)
      coevas.sleep (-math.huge)
    else
      return s, err, part
    end
  until false
end

local UDP_DATAGRAM_MAX = 8192

function Coevas.receivefrom (coevas, socket, size)
  local co      = coevas._coroutine.running ()
  local mt      = getmetatable (socket)
  local timeout = 1
  if mt == Socket.Tcp.__metatable or mt == Socket.Udp.__metatable then
    assert (socket._coevas == coevas)
    timeout = socket._timeout
    socket = socket._socket
  end
  socket:settimeout (0)
  local timed_out = (timeout == 0)
  if timeout and timeout > 0 then
    local on_timeout = ev.Timer.new (function (loop, watcher)
      watcher:stop (loop)
      coevas.wakeup (co)
      timed_out = true
    end, timeout)
    on_timeout:start (coevas._loop)
  end
  size = size or UDP_DATAGRAM_MAX
  repeat
    local s, err, port = socket:receivefrom (size)
    if math.random (100) > 90 then
      coevas.pass ()
    end
    if timed_out then
      return s, err, port
    elseif err == "timeout" or err == "wantread" then
      local on_read = ev.IO.new (function (loop, watcher)
        watcher:stop (loop)
        coevas.wakeup (co)
      end, socket:getfd (), ev.READ)
      on_read:start (coevas._loop)
      coevas.sleep (-math.huge)
    else
      return s, err, port
    end
  until false
end

function Coevas.send (coevas, socket, data, from, to)
  local co      = coevas._coroutine.running ()
  local mt      = getmetatable (socket)
  local timeout = 1
  if mt == Socket.Tcp.__metatable or mt == Socket.Udp.__metatable then
    assert (socket._coevas == coevas)
    socket  = socket._socket
    timeout = socket._timeout
  end
  socket:settimeout (0)
  local timed_out = (timeout == 0)
  if timeout and timeout > 0 then
    local on_timeout = ev.Timer.new (function (loop, watcher)
      watcher:stop (loop)
      coevas.wakeup (co)
      timed_out = true
    end, timeout)
    on_timeout:start (coevas._loop)
  end
  from = from or 1
  local last = from-1
  repeat
    local s, err
    s, err, last = socket:send (data, last+1, to)
    if math.random (100) > 90 then
      coevas.pass ()
    end
    if timed_out then
      return s, err, last
    elseif err == "timeout" or err == "wantwrite" then
      local on_write = ev.IO.new (function (loop, watcher)
        watcher:stop (loop)
        coevas.wakeup (co)
      end, socket:getfd (), ev.WRITE)
      on_write:start (coevas._loop)
      coevas.sleep (-math.huge)
    else
      return s, err, last
    end
  until false
end

function Coevas.sendto (coevas, socket, data, ip, port)
  local co      = coevas._coroutine.running ()
  local mt      = getmetatable (socket)
  local timeout = 1
  if mt == Socket.Tcp.__metatable or mt == Socket.Udp.__metatable then
    assert (socket._coevas == coevas)
    timeout = socket._timeout
    socket = socket._socket
  end
  socket:settimeout (0)
  local timed_out = (timeout == 0)
  if timeout and timeout > 0 then
    local on_timeout = ev.Timer.new (function (loop, watcher)
      watcher:stop (loop)
      coevas.wakeup (co)
      timed_out = true
    end, timeout)
    on_timeout:start (coevas._loop)
  end
  repeat
    local s, err = socket:sendto (data, ip, port)
    if math.random (100) > 90 then
      coevas.pass ()
    end
    if timed_out then
      return s, err
    elseif err == "timeout" or err == "wantwrite" then
      local on_write = ev.IO.new (function (loop, watcher)
        watcher:stop (loop)
        coevas.wakeup (co)
      end, socket:getfd (), ev.WRITE)
      on_write:start (coevas._loop)
      coevas.sleep (-math.huge)
    else
      return s, err
    end
  until false
end

-- Error Handling
-- --------------

function Coevas.setErrorHandler (coevas, err)
  local co = coevas._coroutine.running ()
  if co then
    coevas._errors [co] = err
  end
end

function Coevas.defaultErrorHandler (msg, co, skt)
  print (msg, co, skt)
end

-- Scheduling
-- ----------

function Coevas.pass (coevas)
  coevas.sleep (0)
end

function Coevas.addserver (coevas, socket, handler)
  local mt = getmetatable (socket)
  if mt == Socket.Tcp.__metatable or mt == Socket.Udp.__metatable then
    assert (socket._coevas == coevas)
    socket = socket._socket
  end
  socket:settimeout (0, "b")
  local server = ev.IO.new (function ()
    print "accept"
    local client = socket:accept ()
    print ("client", client)
    if client then
      client:settimeout (0)
      if not coevas.compatibility then
        client = coevas.wrap (client)
      end
      local co = coevas._coroutine.create (function ()
        return handler (client)
      end)
      print ("co", co)
      coevas.wakeup (co)
    end
  end, socket:getfd (), ev.READ)
  coevas._servers [socket] = server
  server:start (coevas._loop)
end

function Coevas.removeserver (coevas, socket)
  local mt = getmetatable (socket)
  if mt == Socket.Tcp.__metatable or mt == Socket.Udp.__metatable then
    assert (socket._coevas == coevas)
    socket = socket._socket
  end
  local watcher = coevas._servers [socket]
  if watcher then
    watcher:stop (coevas._loop)
  end
  coevas._servers [socket] = nil
  return socket:close() 
end

local unpack = table.unpack or unpack

function Coevas.addthread (coevas, f, ...)
  local args = { ... }
  local co = coevas._coroutine.create (function ()
    return f (unpack (args))
  end)
  coevas.wakeup (co)
  return co
end

function Coevas.sleep (coevas, time)
  time = time or 0
  if time == 0 then
    coevas._coroutine.yield ()
    return
  end
  local co = coevas._coroutine.running ()
  coevas._awaken   [co] = nil
  coevas._sleeping [co] = true
  if time >= 0 then
    local on_timeout = ev.Timer.new (function (loop, watcher)
      watcher:stop (loop)
      coevas.wakeup (co)
    end, time)
    on_timeout:start (coevas._loop)
  end
  coevas._coroutine.yield ()
end

function Coevas.wakeup (coevas, co)
  coevas._awaken   [co] = true
  coevas._sleeping [co] = nil
  coevas._idle:start (coevas._loop)
end

math.randomseed (require "socket".gettime ())

function Coevas.default_schedule (cos)
  return next (cos, nil)
end

function Coevas.finished (coevas)
  return next (coevas._servers , nil) == nil
     and next (coevas._awaken  , nil) == nil
     and next (coevas._sleeping, nil) == nil
end

function Coevas.step (coevas)
  print ("step", next (coevas._awaken, nil))
  if next (coevas._awaken, nil) == nil then
    return false
  end
  local co     = coevas.schedule (coevas._awaken)
  print ("selected", co)
  local socket = coevas._sockets [co]
  coevas._awaken [co] = nil
  if coevas._coroutine.status (co) ~= "dead" then
    local ok, res = coevas._coroutine.resume (co)
    if not ok then
      local handler = coevas._errors [co] or Coevas.defaultErrorHandler
      pcall (handler, res, co, socket)
    end
    if coevas._coroutine.status (co) == "dead" then
      if socket and coevas.autoclose then
        socket:close ()
      end
      coevas._sockets [co] = nil
      coevas._errors  [co] = nil
    end
  end
  return true
end

function Coevas.loop (coevas)
  coevas._idle:start (coevas._loop)
  print "loop"
  coevas._loop:loop ()
  print "end loop"
end

function Coevas.unloop (coevas)
  coevas._loop:unloop ()
end

function Coevas.in_loop (coevas)
  return coevas._in_loop
end

-- Socket Wrapper
-- --------------

Socket.Tcp.__index = {
  connect = function (self, address, port)
    return Coevas.connect (self._coevas, self, address, port)
  end,
  close = function (self)
    return self._socket:close ()
  end,
  shutdown = function (self)
    return self._socket:shutdown ()
  end,
  getfd = function (self)
    return self._socket:getfd ()
  end,
  setfd = function (self, fd)
    return self._socket:setfd (fd)
  end,
  send = function (self, data, from, to)
    return Coevas.send (self._coevas, self, data, from, to)
  end,
  receive = function (self, pattern)
    return Coevas.receive (self._coevas, self, pattern)
  end,
  flush = function (self)
    return Coevas.flush (self._coevas, self)
  end,
  settimeout = function (self, time)
    self._timeout = time
  end,
  setoption = function (self, option, value)
    return Coevas.setoption (self._coevas, self, option, value)
  end,
  dohandshake = function (self, parameters)
    return Coevas.dohandshake (self._coevas, self, parameters)
  end,
}
Socket.Tcp.__tostring = function (self)
  return tostring (self._socket)
end
Socket.Tcp.__metatable = "copas-ev-tcp"

Socket.Udp.__index = {
  connect = function (self, address, port)
    return Coevas.connect (self._coevas, self, address, port)
  end,
  close = function (self)
    return self._socket:close ()
  end,
  shutdown = function (self)
    return self._socket:shutdown ()
  end,
  getfd = function (self)
    return self._socket:getfd ()
  end,
  setfd = function (self, fd)
    return self._socket:setfd (fd)
  end,
  send = function (self, data, from, to)
    return Coevas.send (self._coevas, self, data, from, to)
  end,
  sendto = function (self, data, ip, port)
    return Coevas.sendto (self._coevas, self, data, ip, port)
  end,
  receive = function (self, size)
    return Coevas.receive (self._coevas, self, (size or UDP_DATAGRAM_MAX))
  end,
  receivefrom = function (self, size)
    return Coevas.receivefrom (self._coevas, self, (size or UDP_DATAGRAM_MAX))
  end,
  flush = function (self)
    return Coevas.flush (self._coevas, self)
  end,
  settimeout = function (self, time)
    self._timeout = time
  end,
  setoption = function (self, option, value)
    return Coevas.setoption (self._coevas, self, option, value)
  end,
  dohandshake = function (self, parameters)
    return Coevas.dohandshake (self._coevas, self, parameters)
  end,
}
Socket.Udp.__tostring = function (self)
  return tostring (self._socket)
end
Socket.Udp.__metatable = "copas-ev-udp"

-- Module
-- ------

return Coevas.new ()
