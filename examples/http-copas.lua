local copas  = require "copas"

copas.addthread (function ()
  local http = (require "copas.http").request
  print (http "https://github.com/saucisson/lua-copas-ev")
end)

copas.loop ()
