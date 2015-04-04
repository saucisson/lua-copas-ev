                 require "copas"
local ev       = require "ev"
coroutine.make = require "coroutine.make"

local Coevas = {}

local Socket = {
  Tcp = {},
  Udp = {},
}

function Coevas.new ()
  local coevas = {
    autoclose    = true,
    compatibilty = false,
    scheduler    = Coevas.default_scheduler,
    _coroutine   = coroutine.make (),
    _loop        = ev.Loop.new (),
    _errors      = {},
    _servers     = {},
    _sockets     = {},
    _awaken      = {},
    _idle        = nil,
  }
  for k, v in pairs (Coevas) do
    if type (v) == "function" then
      coevas [k] = function (...)
        return v (coevas, ...)
      end
    end
  end
  coevas._idle = ev.Idle.new (function (loop, idle)
    if not Coevas.step (coevas) then
      idle:stop (loop)
    end
  end)
  setmetatable (coevas._awaken, {
    __newindex = function (t, k, v)
      rawset (t, k, v)
      if k == 1 then
        coevas._idle:start (coevas._loop)
      end
    end,
  })
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

function Coevas.wrap (coevas, socket)
  local mt = getmetatable (socket)
  if mt == Socket.Tcp.__metatable or mt == Socket.Udp.__metatable then
    assert (socket._coevas == coevas)
    return socket
  else
    local prefix = string.sub (tostring (socket), 1, 3)
    if     prefix == "udp" then
      return setmetatable ({
        _coevas     = coevas,
        _socket     = socket,
      }, Socket.Udp)
    elseif prefix == "tcp" then
      return setmetatable ({
        _coevas = coevas,
        _socket = socket,
      }, Socket.Tcp)
    elseif prefix == "SSL" then
      return setmetatable ({
        _coevas = coevas,
        _socket = socket,
      }, Socket.Tcp)
    end
  end
end

function Coevas.connect (coevas, socket, host, port)
  local co = coevas._coroutine.running ()
  local mt = getmetatable (socket)
  if mt == Socket.Tcp.__metatable or mt == Socket.Udp.__metatable then
    assert (socket._coevas == coevas)
    socket = socket._socket
  end
  socket:settimeout (0)
  local on_write = ev.IO.new (function (loop, watcher)
    watcher:stop (loop)
    coevas._awaken [#coevas._awaken+1] = co
  end, socket:getfd (), ev.WRITE)
  repeat
    local ok, err = socket:connect (host, port)
    if err == "timeout" then
      on_write:start (coevas._loop)
      coevas._coroutine.yield ()
    else
      return ok, err
    end
  until false
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
      coevas._awaken [#coevas._awaken+1] = co
      timed_out = true
    end, timeout)
    on_timeout:start (coevas._loop)
  end
  local on_read = ev.IO.new (function (loop, watcher)
    watcher:stop (loop)
    coevas._awaken [#coevas._awaken+1] = co
  end, socket:getfd (), ev.READ)
  pattern = pattern or "*l"
  local s, err
  repeat
    s, err, part = socket:receive (pattern, part)
    if math.random (100) > 90 then
      coevas._awaken [#coevas._awaken+1] = co
      coevas._coroutine.yield ()
    end
    if timed_out then
      return s, err, part
    elseif err == "timeout" or err == "wantread" then
      on_read:start (coevas._loop)
      coevas._coroutine.yield ()
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
      coevas._awaken [#coevas._awaken+1] = co
      timed_out = true
    end, timeout)
    on_timeout:start (coevas._loop)
  end
  local on_read = ev.IO.new (function (loop, watcher)
    watcher:stop (loop)
    coevas._awaken [#coevas._awaken+1] = co
  end, socket:getfd (), ev.READ)
  size = size or UDP_DATAGRAM_MAX
  repeat
    local s, err, port = socket:receivefrom (size)
    if timed_out then
      return s, err, port
    elseif err == "timeout" or err == "wantread" then
      on_read:start (coevas._loop)
      coevas._coroutine.yield ()
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
      coevas._awaken [#coevas._awaken+1] = co
      timed_out = true
    end, timeout)
    on_timeout:start (coevas._loop)
  end
  local on_write = ev.IO.new (function (loop, watcher)
    watcher:stop (loop)
    coevas._awaken [#coevas._awaken+1] = co
  end, socket:getfd (), ev.WRITE)
  from = from or 1
  local last = from-1
  repeat
    local s, err
    s, err, last = socket:send (data, last+1, to)
    if math.random (100) > 90 then
      coevas._awaken [#coevas._awaken+1] = co
      coevas._coroutine.yield ()
    end
    if timed_out then
      return s, err, last
    elseif err == "timeout" or err == "wantwrite" then
      on_write:start (coevas._loop)
      coevas._coroutine.yield ()
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
      coevas._awaken [#coevas._awaken+1] = co
      timed_out = true
    end, timeout)
    on_timeout:start (coevas._loop)
  end
  local on_write = ev.IO.new (function (loop, watcher)
    watcher:stop (loop)
    coevas._awaken [#coevas._awaken+1] = co
  end, socket:getfd (), ev.WRITE)
  repeat
    local s, err = socket:sendto (data, ip, port)
    if math.random (100) > 90 then
      coevas._awaken [#coevas._awaken+1] = co
      coevas._coroutine.yield ()
    end
    if timed_out then
      return s, err
    elseif err == "timeout" or err == "wantwrite" then
      on_write:start (coevas._loop)
      coevas._coroutine.yield ()
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
  local co = coevas._coroutine.running ()
  coevas._awaken [#coevas._awaken+1] = co
  coevas._coroutine.yield ()
end

function Coevas.addserver (coevas, socket, handler)
  local mt = getmetatable (socket)
  if mt == Socket.Tcp.__metatable or mt == Socket.Udp.__metatable then
    assert (socket._coevas == coevas)
    socket = socket._socket
  end
  socket:settimeout (0)
  local server = ev.IO.new (function ()
    local client = socket:accept ()
    if client then
      client:settimeout (0)
      if not coevas.compatibility then
        client = coevas.wrap (client)
      end
      local co = coevas._coroutine.create (handler)
      coevas._sockets [co] = client
      coevas._awaken  [#coevas._awaken+1] = {
        thread = co,
        [1]    = client,
      }
    end
  end, socket:getfd (), ev.READ)
  server:start (coevas._loop)
  coevas._servers [socket] = server
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

function Coevas.addthread (coevas, thread, ...)
  if type (thread) ~= "thread" then
    thread = coevas._coroutine.create (thread)
  end
  local result  = { ... }
  result.thread = thread
  coevas._awaken [#coevas._awaken+1] = result
end

function Coevas.sleep (coevas, time)
  time = time or 0
  local co = coevas._coroutine.running ()
  if time < 0 then
    coevas._coroutine.yield ()
  else
    local on_timeout = ev.Timer.new (function (loop, watcher)
      watcher:stop (loop)
      coevas._awaken [#coevas._awaken+1] = co
    end, time)
    on_timeout:start (coevas._loop)
    coevas._coroutine.yield ()
  end
end

function Coevas.wakeup (coevas, co)
  coevas._awaken [#coevas._awaken+1] = co
end

-- `table.unpack` does not exist in Lua 5.1, but is available using the `unpack`
-- function:
table.unpack = table.unpack or unpack
math.randomseed (require "socket".gettime ())

function Coevas.default_scheduler (cos)
  return #cos
end

function Coevas.step (coevas)
  if #coevas._awaken == 0 then
    return false
  end
  local n = coevas.scheduler (coevas._awaken)
  local co, parameters = coevas._awaken [n]
  table.remove (coevas._awaken, n)
  if type (co) == "table" then
    co, parameters = co.thread, co
  end
  local socket = coevas._sockets [co]
  if coevas._coroutine.status (co) ~= "dead" then
    local ok, res
    if parameters then
      ok, res = coevas._coroutine.resume (co, table.unpack (parameters))
    else
      ok, res = coevas._coroutine.resume (co)
    end
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
  coevas._loop:loop ()
end

function Coevas.unloop (coevas)
  coevas._loop:unloop ()
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
  tls = function (self, parameters)
    return Coevas.tls (self._coevas, self, parameters)
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
}
Socket.Udp.__tostring = function (self)
  return tostring (self._socket)
end
Socket.Udp.__metatable = "copas-ev-udp"

-- Module
-- ------

return Coevas.new ()
