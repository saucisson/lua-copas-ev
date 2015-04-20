                 require "copas"
local ev       = require "ev"
coroutine.make = require "coroutine.make"

local Coevas = {}

local Levels = {
  Thread = 1,
  Server = 2,
}

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
  local result = {
    autoclose    = true,
    compatibilty = false,
    _coroutine   = coroutine.make (),
    _loop        = ev.Loop.new (),
    _idle        = nil,
    _running     = nil,
    _info        = setmetatable ({}, { __mode = "k" }),
    _sockets     = setmetatable ({}, { __mode = "v" }),
    _threads     = {},
  }
  result._idle = ev.Idle.new (function (loop, idle)
    if not result.step () then
      idle:stop (loop)
    end
    if result.finished () then
      loop:unloop ()
    end
  end)
  for _, level in pairs (Levels) do
    result._threads [level] = {
      _ready       = {},
      _blocking    = {},
      _nonblocking = {},
    }
  end
  return setmetatable (result, Coevas)
end

-- Replace `copas` by its `ev` variant in subsequent `require`.
function Coevas.make_default (coevas)
  package.loaded .copas = nil
  package.preload.copas = function ()
    return coevas
  end
end

-- Error Handling
-- --------------

function Coevas.setErrorHandler (coevas, err)
  local co    = coevas._running
  local info  = coevas._info [co]
  info._error = err
end

function Coevas.defaultErrorHandler (msg, co, skt)
  print (msg, co, skt)
end

-- Scheduling
-- ----------

local unpack = table.unpack or unpack

function Coevas.addthread (coevas, f, ...)
  local args = { ... }
  local co = coevas._coroutine.create (function ()
    return f (unpack (args))
  end)
  coevas._info [co] = {
    _level    = Levels.Thread,
    _error    = nil,
    _blocking = true,
  }
  coevas.wakeup (co)
  return co
end

function Coevas.addserver (coevas, socket, handler)
  local co = coevas._coroutine.create (function ()
    local socket = coevas.wrap (socket)
    while true do
      local client = socket:accept ()
      if client then
        client:settimeout (0)
        if not coevas.compatibility then
          client = coevas.wrap (client)
        end
        local co = coevas.addthread (handler, client)
        coevas._info [co]._socket = client
      end
    end
  end)
  socket = coevas.raw (socket)
  coevas._info [co] = {
    _level    = Levels.Server,
    _error    = nil,
    _socket   = socket,
    _blocking = true,
  }
  coevas._sockets [socket] = co
  coevas.wakeup (co)
  return co
end

function Coevas.removeserver (coevas, socket)
  socket   = coevas.raw (socket)
  local co = coevas._sockets [socket]
  coevas.kill (co)
  return socket:close() 
end

function Coevas.pass (coevas)
  coevas.sleep (0)
end

function Coevas.blocking (coevas, value)
  local co       = coevas._running
  local info     = coevas._info [co]
  info._blocking = value
end

function Coevas.sleep (coevas, time, handler)
  time = time or 0
  if time == 0 then
    coevas._coroutine.yield ()
    return
  end
  local co      = coevas._running
  local info    = coevas._info [co]
  local threads = coevas._threads [info._level]
  threads._ready   [co] = nil
  if info._blocking then
    threads._blocking    [co] = handler or true
  else
    threads._nonblocking [co] = handler or true
  end
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
  local info    = coevas._info [co]
  if not info then
    return
  end
  local threads = coevas._threads [info._level]
  threads._ready       [co] = true
  threads._blocking    [co] = nil
  threads._nonblocking [co] = nil
  coevas._idle:start (coevas._loop)
end

function Coevas.kill (coevas, co)
  local info    = coevas._info [co]
  local threads = coevas._threads [info._level]
  local socket  = info._socket
  local handler
  if info._blocking then
    handler = threads._blocking    [co]
  else
    handler = threads._nonblocking [co]
  end
  if handler and handler ~= true then
    handler:stop (coevas._loop)
  end
  coevas._info         [co] = nil
  threads._ready       [co] = nil
  threads._blocking    [co] = nil
  threads._nonblocking [co] = nil
  if socket then
    coevas._sockets [socket] = nil
  end
  if socket and coevas.autoclose then
    --socket:shutdown ()
    socket:close ()
  end
end

function Coevas.finished (coevas)
  for i = 1, #coevas._threads do
    local threads = coevas._threads [i]
    if threads and (next (threads._ready) or next (threads._blocking)) then
      return false
    end
  end
  return true
end

local max_level = 0
for _, level in pairs (Levels) do
  max_level = math.max (max_level, level)
end

function Coevas.step (coevas)
  for i = 1, max_level do
    local threads = coevas._threads [i]
    local co      = next (threads._ready)
    if co then
      local info   = coevas._info [co]
      local socket = info._socket
      if coevas._coroutine.status (co) ~= "dead" then
        coevas._running = co
        local ok, res   = coevas._coroutine.resume (co)
        coevas._running = nil
        if not ok then
          local handler = info._error or Coevas.defaultErrorHandler
          pcall (handler, res, co, socket)
        end
      end
      if coevas._coroutine.status (co) == "dead" then
        coevas.kill (co)
      end
      return true
    end
  end
  return false
end

function Coevas.loop (coevas)
  coevas._idle:start (coevas._loop)
  coevas._loop:loop ()
end

-- Socket Operations
-- -----------------

function Coevas.tcp (coevas)
  local socket = require "socket"
  return Coevas.wrap (coevas, socket.tcp ())
end

function Coevas.udp (coevas)
  local socket = require "socket"
  return Coevas.wrap (coevas, socket.udp ())
end

function Coevas.bind (coevas, socket, address, port)
  local raw = Coevas.raw (coevas, socket)
  return raw:bind (address, port)
end

function Coevas.raw (coevas, socket)
  local mt = getmetatable (socket)
  if mt == Socket.Tcp.__metatable or mt == Socket.Udp.__metatable then
    assert (socket._coevas == coevas)
    return socket._socket
  else
    return socket
  end
end

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

function Coevas.timeout (coevas, socket)
  local co      = coevas._running
  local mt      = getmetatable (socket)
  local timeout = -math.huge
  if mt == Socket.Tcp.__metatable or mt == Socket.Udp.__metatable then
    assert (socket._coevas == coevas)
    timeout = socket._timeout or timeout
  end
  local result = {
    timeout = timeout == 0,
  }
  if timeout > 0 then
    local on_timeout = ev.Timer.new (function (loop, watcher)
      watcher:stop (loop)
      coevas.wakeup (co)
      result.timeout = true
    end, timeout)
    on_timeout:start (coevas._loop)
  end
  socket = coevas.raw (socket)
  socket:settimeout (0)
  return socket, result
end

function Coevas.accept (coevas, skt)
  local co             = coevas._running
  local socket, signal = coevas.timeout (skt)
  repeat
    local client, err = socket:accept ()
    if signal.timeout then
      return nil, err
    elseif err == "timeout" or err == "wantread" then
      local on_read = ev.IO.new (function (loop, watcher)
        watcher:stop (loop)
        coevas.wakeup (co)
      end, socket:getfd (), ev.READ)
      on_read:start (coevas._loop)
      coevas.sleep (-math.huge, on_read)
    else
      return client, err
    end
  until false
end

function Coevas.connect (coevas, skt, address, port)
  local co             = coevas._running
  local socket, signal = coevas.timeout (skt)
  local sslparams      = socket ~= skt and skt._ssl or nil
  repeat
    local ok, err = socket:connect (address, port)
    if signal.timeout then
      return nil, err
    elseif err == "timeout" then
      local on_write = ev.IO.new (function (loop, watcher)
        watcher:stop (loop)
        coevas.wakeup (co)
      end, socket:getfd (), ev.WRITE)
      on_write:start (coevas._loop)
      coevas.sleep (-math.huge, on_write)
    elseif ok and sslparams then
      return coevas.dohandshake (socket, sslparams)
    else
      return ok, err
    end
  until false
end

function Coevas.dohandshake (coevas, socket, sslparams)
  local ssl = require "ssl"
  local ok, err
  socket      = coevas.wrap (socket)
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

function Coevas.setoption (coevas, skt, option, value)
  local socket = coevas.raw (skt)
  return socket:setoption (option, value)
end

function Coevas.receive (coevas, skt, pattern, part)
  local co             = coevas._running
  local socket, signal = coevas.timeout (skt)
  pattern = pattern or "*l"
  local s, err
  repeat
    s, err, part = socket:receive (pattern, part)
    if math.random (100) > 90 then
      coevas.pass ()
    end
    if signal.timeout then
      return s, err, part
    elseif err == "timeout" or err == "wantread" then
      local on_read = ev.IO.new (function (loop, watcher)
        watcher:stop (loop)
        coevas.wakeup (co)
      end, socket:getfd (), ev.READ)
      on_read:start (coevas._loop)
      coevas.sleep (-math.huge, on_read)
    else
      return s, err, part
    end
  until false
end

local UDP_DATAGRAM_MAX = 8192

function Coevas.receivefrom (coevas, skt, size)
  local co             = coevas._running
  local socket, signal = coevas.timeout (skt)
  size = size or UDP_DATAGRAM_MAX
  repeat
    local s, err, port = socket:receivefrom (size)
    if math.random (100) > 90 then
      coevas.pass ()
    end
    if signal.timeout then
      return s, err, port
    elseif err == "timeout" or err == "wantread" then
      local on_read = ev.IO.new (function (loop, watcher)
        watcher:stop (loop)
        coevas.wakeup (co)
      end, socket:getfd (), ev.READ)
      on_read:start (coevas._loop)
      coevas.sleep (-math.huge, on_read)
    else
      return s, err, port
    end
  until false
end

function Coevas.send (coevas, skt, data, from, to)
  local co             = coevas._running
  local socket, signal = coevas.timeout (skt)
  from = from or 1
  local last = from-1
  repeat
    local s, err
    s, err, last = socket:send (data, last+1, to)
    if math.random (100) > 90 then
      coevas.pass ()
    end
    if signal.timeout then
      return s, err, last
    elseif err == "timeout" or err == "wantwrite" then
      local on_write = ev.IO.new (function (loop, watcher)
        watcher:stop (loop)
        coevas.wakeup (co)
      end, socket:getfd (), ev.WRITE)
      on_write:start (coevas._loop)
      coevas.sleep (-math.huge, on_write)
    else
      return s, err, last
    end
  until false
end

function Coevas.sendto (coevas, skt, data, ip, port)
  local co             = coevas._running
  local socket, signal = coevas.timeout (skt)
  repeat
    local s, err = socket:sendto (data, ip, port)
    if math.random (100) > 90 then
      coevas.pass ()
    end
    if signal.timeout then
      return s, err
    elseif err == "timeout" or err == "wantwrite" then
      local on_write = ev.IO.new (function (loop, watcher)
        watcher:stop (loop)
        coevas.wakeup (co)
      end, socket:getfd (), ev.WRITE)
      on_write:start (coevas._loop)
      coevas.sleep (-math.huge, on_write)
    else
      return s, err
    end
  until false
end

-- Socket Wrapper
-- --------------

Socket.Tcp.__index = {
  bind   = function (self, address, port)
    return Coevas.bind (self._coevas, self, address, port)
  end,
  accept = function (self)
    return Coevas.accept (self._coevas, self)
  end,
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
