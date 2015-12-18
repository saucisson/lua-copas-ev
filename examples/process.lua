local copev = require "copas.ev"

copev.addthread (function ()
  print (copev.execute "sleep 2; echo toto")
end)

copev.addthread (function ()
  print (copev.execute "sleep 3; echototo")
end)

copev.loop ()
