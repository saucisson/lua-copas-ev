                 require "copas" -- fix socket/copas interactions
local ev       = require "ev"
local coromake = require "coroutine.make"

local Coevas = {}

local Socket = {
  Tcp  = {},
  Udp  = {},
  Unix = {},
}

function Coevas.new ()
  local result = {
    autoclose    = true,
    compatibilty = false,
    clean_awaken = 500,
    ratio        = 95,
    _coroutine   = coromake (),
    _loop        = ev.Loop.new (),
    _idle        = nil,
    _running     = nil,
    _info        = setmetatable ({}, { __mode = "k" }),
    _sockets     = setmetatable ({}, { __mode = "v" }),
    _threads     = setmetatable ({}, {
      __index = function (self, n)
        self [n] = {
          _ready       = {},
          _blocking    = {},
          _nonblocking = {},
          _awaken      = {
            clean = 0,
          },
        }
        return self [n]
      end,
    }),
  }
  result._idle = ev.Idle.new (function (loop, idle)
    if not result.step () then
      idle:stop (loop)
    end
    if result.finished () then
      loop:unloop ()
    end
  end)
  for k, f in pairs (Coevas) do
    if type (f) == "function" then
      result [k] = function (...)
        return f (result, ...)
      end
    end
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
  local co   = coevas._coroutine.create (function ()
    return f (unpack (args))
  end)
  coevas._info [co] = {
    _level    = 1,
    _error    = nil,
    _blocking = true,
  }
  coevas.wakeup (co)
  return co
end

function Coevas.addserver (coevas, skt, handler)
  local co = coevas._coroutine.create (function ()
    local socket = coevas.wrap (skt)
    while true do
      local client = socket:accept ()
      if client then
        if not coevas.compatibility then
          client = coevas.wrap (client)
        end
        local co = coevas.addthread (handler, client)
        coevas._info [co]._socket = client
      end
    end
  end)
  local wrapped = coevas.wrap (skt)
  local socket  = coevas.raw  (skt)
  coevas._info [co] = {
    _level    = 0,
    _error    = nil,
    _socket   = wrapped,
    _blocking = true,
  }
  coevas._sockets [socket] = co
  coevas.wakeup (co)
  return co
end

function Coevas.removeserver (coevas, skt)
  local socket = coevas.raw (skt)
  local co     = coevas._sockets [socket]
  return coevas.kill (co)
end

function Coevas.pass (coevas)
  coevas.sleep (0)
end

function Coevas.blocking (coevas, value)
  local co       = coevas._running
  local info     = coevas._info [co]
  info._blocking = value
end

function Coevas.wakeup (coevas, co)
  local info = coevas._info [co]
  if not info then
    return
  end
  local threads = coevas._threads [info._level]
  threads._awaken [#threads._awaken+1] = co
  threads._ready       [co] = true
  threads._blocking    [co] = nil
  threads._nonblocking [co] = nil
  coevas._idle:start (coevas._loop)
end

function Coevas.sleep (coevas, time, handler)
  time = time or 0
  local co      = coevas._running
  local info    = coevas._info [co]
  local threads = coevas._threads [info._level]
  if time == 0 then
    threads._awaken [#threads._awaken+1] = co
    coevas._coroutine.yield ()
    return
  end
  threads._ready [co] = nil
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

function Coevas.kill (coevas, co)
  local info    = coevas._info [co]
  if not info then
    return
  end
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
    local raw = coevas.raw (socket)
    coevas._sockets [raw] = nil
    if coevas.autoclose then
      socket:close ()
    end
  end
end

function Coevas.finished (coevas)
  for i = 0, #coevas._threads do
    local threads = coevas._threads [i]
    if threads and (next (threads._ready) or next (threads._blocking)) then
      return false
    end
  end
  return true
end

function Coevas.step (coevas)
  for i = 1, #coevas._threads+1 do
    if i == #coevas._threads+1 then
      i = 0 -- for servers
    end
    local threads = coevas._threads [i]
    if threads._awaken.clean == coevas.clean_awaken then
      threads._awaken.clean = 0
      for j = 1, #threads._awaken do
        if not threads._awaken [j] then
          threads._awaken [j] = nil
        elseif j > #threads._awaken+1 then
          threads._awaken [#threads._awaken+1] = threads._awaken [j]
          threads._awaken [j] = nil
        end
      end
    end
    local co
    for j = 1, #threads._awaken do
      co = threads._awaken [j]
      if co and threads._ready [co] then
        threads._awaken [j]   = false
        threads._awaken.clean = threads._awaken.clean+1
        break
      else
        co = nil
      end
    end
    if co then
      if coevas._coroutine.status (co) ~= "dead" then
        coevas._running = co
        local ok, res   = coevas._coroutine.resume (co)
        coevas._running = nil
        if not ok then
          local info   = coevas._info [co]
          local socket = info._socket
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

--[==[
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
--]==]

function Coevas.wrap (coevas, socket, sslparams)
  if coevas.raw (socket) ~= socket then
    return socket
  end
  socket:settimeout (0)
  local prefix = string.sub (tostring (socket), 1, 3)
  if     prefix == "uni" then
    return setmetatable ({
      _coevas = coevas,
      _socket = socket,
      _ssl    = sslparams,
    }, Socket.Unix)
  elseif prefix == "udp" then
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

function Coevas.raw (coevas, socket)
  local mt = getmetatable (socket)
  if mt == Socket.Tcp .__metatable
  or mt == Socket.Udp .__metatable
  or mt == Socket.Unix.__metatable then
    assert (socket._coevas == coevas)
    return socket._socket
  else
    return socket
  end
end

function Coevas.timeout (coevas, skt)
  local co      = coevas._running
  local wrapped = coevas.wrap (skt)
  local timeout = -math.huge
  timeout = wrapped._timeout or timeout
  local result = {
    timeout = timeout == 0,
  }
  if timeout > 0 then
    result.on_timeout = ev.Timer.new (function (loop, watcher)
      watcher:stop (loop)
      result.timeout = true
      coevas.wakeup (co)
    end, timeout)
    result.on_timeout:start (coevas._loop)
  end
  return result
end

function Coevas.accept (coevas, skt)
  local co      = coevas._running
  local signal  = coevas.timeout (skt)
  local socket  = coevas.raw (skt)
  repeat
    local client, err = socket:accept ()
    if math.random (100) > coevas.ratio then
      coevas.pass ()
    end
    if signal.timeout then
      return nil, err
    elseif err == "timeout" or err == "wantread" then
      if tostring (socket):sub (1, 3) == "uni" then
        coevas.sleep (1, on_read)
      else
        local on_read = ev.IO.new (function (loop, watcher)
          watcher:stop (loop)
          coevas.wakeup (co)
        end, socket:getfd (), ev.READ)
        on_read:start (coevas._loop)
        coevas.sleep (-math.huge, on_read)
      end
    else
      if signal.on_timeout then
        signal.on_timeout:stop (coevas._loop)
      end
      return client, err
    end
  until false
end

function Coevas.connect (coevas, skt, address, port)
  local co        = coevas._running
  local signal    = coevas.timeout (skt)
  local socket    = coevas.raw (skt)
  local sslparams = socket ~= skt and skt._ssl or nil
  repeat
    local ok, err = socket:connect (address, port)
    if signal.timeout then
      return nil, err
    elseif err == "timeout" or err == "Operation already in progress" then
      local on_write = ev.IO.new (function (loop, watcher)
        watcher:stop (loop)
        coevas.wakeup (co)
      end, socket:getfd (), ev.WRITE)
      on_write:start (coevas._loop)
      coevas.sleep (-math.huge, on_write)
    elseif ok and sslparams then
      if signal.on_timeout then
        signal.on_timeout:stop (coevas._loop)
      end
      return coevas.dohandshake (skt, sslparams)
    else
      if signal.on_timeout then
        signal.on_timeout:stop (coevas._loop)
      end
      return ok, err
    end
  until false
end

function Coevas.dohandshake (coevas, skt, sslparams)
  local ssl = require "ssl"
  local ok, err
  local socket = coevas.wrap (coevas.raw (skt))
  socket, err = ssl.wrap (socket, sslparams)
  if not socket then
    error (err)
  end
  ok, err = socket:dohandshake ()
  if not ok then
    error (err)
  end
  local result = coevas.wrap (skt)
  result._socket = socket
  return result
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
  local co     = coevas._running
  local signal = coevas.timeout (skt)
  local socket = coevas.raw (skt)
  pattern = pattern or "*l"
  local s, err
  repeat
    s, err, part = socket:receive (pattern, part)
    if math.random (100) > coevas.ratio then
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
      if signal.on_timeout then
        signal.on_timeout:stop (coevas._loop)
      end
      return s, err, part
    end
  until false
end

local UDP_DATAGRAM_MAX = 8192

function Coevas.receivefrom (coevas, skt, size)
  local co     = coevas._running
  local signal = coevas.timeout (skt)
  local socket = coevas.raw (skt)
  size = size or UDP_DATAGRAM_MAX
  repeat
    local s, err, port = socket:receivefrom (size)
    if math.random (100) > coevas.ratio then
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
      if signal.on_timeout then
        signal.on_timeout:stop (coevas._loop)
      end
      return s, err, port
    end
  until false
end

function Coevas.send (coevas, skt, data, from, to)
  local co     = coevas._running
  local signal = coevas.timeout (skt)
  local socket = coevas.raw (skt)
  from = from or 1
  local last = from-1
  repeat
    local s, err
    s, err, last = socket:send (data, last+1, to)
    if math.random (100) > coevas.ratio then
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
      if signal.on_timeout then
        signal.on_timeout:stop (coevas._loop)
      end
      return s, err, last
    end
  until false
end

function Coevas.sendto (coevas, skt, data, ip, port)
  local co     = coevas._running
  local signal = coevas.timeout (skt)
  local socket = coevas.raw (skt)
  repeat
    local s, err = socket:sendto (data, ip, port)
    if math.random (100) > coevas.ratio then
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
      if signal.on_timeout then
        signal.on_timeout:stop (coevas._loop)
      end
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
  receive = function (self, pattern, prefix)
    return Coevas.receive (self._coevas, self, pattern, prefix)
  end,
  flush = function (self)
    return Coevas.flush (self._coevas, self)
  end,
  settimeout = function (self, time)
    self._timeout = time
    return true
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
    return true
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

Socket.Unix.__index = {
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
  receive = function (self, pattern, prefix)
    return Coevas.receive (self._coevas, self, pattern, prefix)
  end,
  flush = function (self)
    return Coevas.flush (self._coevas, self)
  end,
  settimeout = function (self, time)
    self._timeout = time
    return true
  end,
  setoption = function (self, option, value)
    return Coevas.setoption (self._coevas, self, option, value)
  end,
  dohandshake = function (self, parameters)
    return Coevas.dohandshake (self._coevas, self, parameters)
  end,
}
Socket.Unix.__tostring = function (self)
  return tostring (self._socket)
end
Socket.Unix.__metatable = "copas-ev-unix"

-- Module
-- ------

return Coevas.new ()
