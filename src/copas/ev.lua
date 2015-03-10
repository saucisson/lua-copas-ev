local copas    = require "copas"
local ev       = require "ev"
--local socket   = require "socket"
local bit      = require "bit"
coroutine.make = require "coroutine.make"

local Coevas = {}

local Socket = {
  Tcp = {},
  Udp = {},
}

function Coevas.new ()
  local coevas = {
    autoclose   = true,
    _coroutine  = coroutine.make (),
    _loop       = ev.Loop.new (),
    _errors     = {},
    _servers    = {},
    _sockets    = {},
    _parameters = {},
    _awaken     = {},
    _idle       = nil,
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

local bor = bit.bor

function Coevas.wrap (coevas, socket)
  if getmetatable (socket) == Socket.Tcp
  or getmetatable (socket) == Socket.Udp then
    assert (socket._coevas == coevas)
    return socket
  else
    if string.sub (tostring (socket), 1, 3) == "udp" then
      return setmetatable ({
        _coevas = coevas,
        _socket = socket,
      }, Socket.udp)
    else
      return setmetatable ({
        _coevas = coevas,
        _socket = socket,
      }, Socket.Tcp)
    end
  end
end

function Coevas.connect (coevas, socket, host, port)
  local co = coevas._coroutine.running ()
  if getmetatable (socket) == Socket.Tcp
  or getmetatable (socket) == Socket.Udp then
    assert (socket._coevas == coevas)
    socket = socket._socket
  end
  socket:settimeout (0)
  local ret, err
  repeat
    ret, err = socket:connect (host, port)
    if ret or err ~= "timeout" then
      return ret, err
    end
    local watcher = ev.IO.new (function (loop, watcher)
      watcher:stop (loop)
      coevas._awaken [#coevas._awaken+1] = co
    end, socket:getfd (), bor (ev.READ, ev.WRITE))
    watcher:start (coevas._loop)
    coevas._coroutine.yield ()
  until false
end

function Coevas.flush ()
end

function Coevas.setoption (coevas, socket, option, value)
  if getmetatable (socket) == Socket.Tcp
  or getmetatable (socket) == Socket.Udp then
    assert (socket._coevas == coevas)
    socket = socket._socket
  end
  return socket:setoption (option, value)
end

function Coevas.receive (coevas, socket, pattern, part)
  local co = coevas._coroutine.running ()
  if getmetatable (socket) == Socket.Tcp
  or getmetatable (socket) == Socket.Udp then
    assert (socket._coevas == coevas)
    socket = socket._socket
  end
  local s, err
  pattern = pattern or "*l"
  repeat
    s, err, part = socket:receive (pattern, part)
    if s or err ~= "timeout" then
      return s, err, part
    end
    local watcher = ev.IO.new (function (loop, watcher)
      watcher:stop (loop)
      coevas._awaken [#coevas._awaken+1] = co
    end, socket:getfd (), ev.READ)
    watcher:start (coevas._loop)
    coevas._coroutine.yield ()
  until false
end

local UDP_DATAGRAM_MAX = 8192

function copas.receivefrom (coevas, socket, size)
  local co = coevas._coroutine.running ()
  if getmetatable (socket) == Socket.Tcp
  or getmetatable (socket) == Socket.Udp then
    assert (socket._coevas == coevas)
    socket = socket._socket
  end
  local s, err, port
  size = size or UDP_DATAGRAM_MAX
  repeat
    s, err, port = socket:receivefrom (size)
    if s or err ~= "timeout" then
      return s, err, port
    end
    local watcher = ev.IO.new (function (loop, watcher)
      watcher:stop (loop)
      coevas._awaken [#coevas._awaken+1] = co
    end, socket:getfd (), ev.READ)
    watcher:start (coevas._loop)
    coevas._coroutine.yield ()
  until false
end

function Coevas.receivePartial (coevas, socket, pattern)
  local co = coevas._coroutine.running ()
  if getmetatable (socket) == Socket.Tcp
  or getmetatable (socket) == Socket.Udp then
    assert (socket._coevas == coevas)
    socket = socket._socket
  end
  local s, err, part
  pattern = pattern or "*l"
  repeat
    s, err, part = socket:receive (pattern)
    if s
    or (type (pattern) == "number" and part ~= "" and part ~= nil)
    or err ~= "timeout" then
      return s, err, part
    end
    local watcher = ev.IO.new (function (loop, watcher)
      watcher:stop (loop)
      coevas._awaken [#coevas._awaken+1] = co
    end, socket:getfd (), ev.READ)
    watcher:start (coevas._loop)
    coevas._coroutine.yield ()
  until false
end

function Coevas.send (coevas, socket, data, from, to)
  local co = coevas._coroutine.running ()
  if getmetatable (socket) == Socket.Tcp
  or getmetatable (socket) == Socket.Udp then
    assert (socket._coevas == coevas)
    socket = socket._socket
  end
  local s, err
  from = from or 1
  local lastIndex = from - 1
  repeat
    s, err, lastIndex = socket:send (data, lastIndex + 1, to)
    if math.random (100) > 90 then
      coevas._awaken [#coevas._awaken+1] = co
      coevas._coroutine.yield ()
    end
    if s or err ~= "timeout" then
      return s, err, lastIndex
    end
    local watcher = ev.IO.new (function (loop, watcher)
      watcher:stop (loop)
      coevas._awaken [#coevas._awaken+1] = co
    end, socket:getfd (), ev.WRITE)
    watcher:start (coevas._loop)
    coevas._coroutine.yield ()
  until false
end

function Coevas.sendto(coevas, socket, data, ip, port)
  local co = coevas._coroutine.running ()
  if getmetatable (socket) == Socket.Tcp
  or getmetatable (socket) == Socket.Udp then
    assert (socket._coevas == coevas)
    socket = socket._socket
  end
  local s, err
  repeat
    s, err = socket:sendto (data, ip, port)
    if math.random (100) > 90 then
      coevas._awaken [#coevas._awaken+1] = co
      coevas._coroutine.yield ()
    end
    if s or err ~= "timeout" then
      return s, err
    end
    local watcher = ev.IO.new (function (loop, watcher)
      watcher:stop (loop)
      coevas._awaken [#coevas._awaken+1] = co
    end, socket:getfd (), ev.WRITE)
    watcher:start (coevas._loop)
    coevas._coroutine.yield ()
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

function Coevas.defaultErrorHandler (_, msg, co, skt)
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
  if getmetatable (socket) == Socket.Tcp
  or getmetatable (socket) == Socket.Udp then
    assert (socket._coevas == coevas)
    socket = socket._socket
  end
  socket:settimeout (0)
  local watcher = ev.IO.new (function ()
    local client = socket:accept ()
    if client then
      client:settimeout (0)
      client = coevas.wrap (client)
      local co = coevas._coroutine.create (handler)
      coevas._parameters [co] = { client }
      coevas._sockets    [co] = client
      coevas._awaken [#coevas._awaken+1] = co
    end
  end, socket:getfd (), bor (ev.READ, ev.WRITE))
  watcher:start (coevas._loop)
  coevas._servers [socket] = watcher
end

function Coevas.removeserver (coevas, socket)
  if getmetatable (socket) == Socket.Tcp
  or getmetatable (socket) == Socket.Udp then
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
  coevas._parameters [thread] = { ... }
  coevas._awaken [#coevas._awaken+1] = thread
end

function Coevas.sleep (coevas, time)
  local co = coevas._coroutine.running ()
  local watcher = ev.Timer.new (function (loop, watcher)
    watcher:stop (loop)
    coevas._awaken [#coevas._awaken+1] = co
  end, time)
  watcher:start (coevas._loop)
  coevas._coroutine.yield ()
end

function Coevas.wakeup (coevas, co)
  coevas._awaken [#coevas._awaken+1] = co
end

-- `table.unpack` does not exist in Lua 5.1, but is available using the `unpack`
-- function:
table.unpack = table.unpack or unpack

function Coevas.step (coevas)
  if #coevas._awaken == 0 then
    return false
  end
  local co     = coevas._awaken  [1]
  local socket = coevas._sockets [co]
  table.remove (coevas._awaken, 1)
  if coevas._coroutine.status (co) ~= "dead" then
    local parameters = coevas._parameters [co] or {}
    coevas._parameters [co] = nil
    local ok, res = coevas._coroutine.resume (co, table.unpack (parameters))
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

-- Socket Wrapper
-- --------------

Socket.Tcp.__index = {
  connect = function (self, address, port)
    return Coevas.connect (self._coevas, self._socket, address, port)
  end,
  close = function (self)
    return self._socket:close ()
  end,
  getfd = function (self)
    return self._socket:getfd ()
  end,
  send = function (self, data, from, to)
    return Coevas.send (self._coevas, self._socket, data, from, to)
  end,
  receive = function (self, pattern)
    if (self.timeout == 0) then
      return Coevas.receivePartial (self._coevas, self._socket, pattern)
    end
    return Coevas.receive (self._coevas, self._socket, pattern)
  end,
  flush = function (self)
    return Coevas.flush (self._coevas, self._socket)
  end,
  settimeout = function (self, time)
    self._timeout = time
    return
  end,
  setoption = function (self, option, value)
    return Coevas.setoption (self._coevas, self._socket, option, value)
  end,
}

Socket.Udp.__index = {
  connect = function (self, address, port)
    return Coevas.connect (self._coevas, self._socket, address, port)
  end,
  close = function (self)
    return self._socket:close ()
  end,
  getfd = function (self)
    return self._socket:getfd ()
  end,
  send = function (self, data, from, to)
    return Coevas.send (self._coevas, self._socket, data, from, to)
  end,
  sendto = function (self, data, ip, port)
    return Coevas.sendto (self._coevas, self._socket, data, ip, port)
  end,
  receive = function (self, size)
    return Coevas.receive (self._coevas, self._socket, (size or UDP_DATAGRAM_MAX))
  end,
  receivefrom = function (self, size)
    return Coevas.receivefrom (self._coevas, self._socket, (size or UDP_DATAGRAM_MAX))
  end,
  flush = function (self)
    return Coevas.flush (self._coevas, self._socket)
  end,
  settimeout = function (self, time)
    self._timeout = time
    return
  end,
  setoption = function (self, option, value)
    return Coevas.setoption (self._coevas, self._socket, option, value)
  end,
}

-- Module
-- ------

return Coevas.new ()
