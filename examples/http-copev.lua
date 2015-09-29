local copev = require "copas.ev"

copev.compatibility = true
copev.make_default ()

copev.addthread (function ()
  local http = (require "copas.http").request
  print (http "https://github.com/saucisson/lua-copas-ev")
end)

copev.loop ()
