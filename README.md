[![Build Status](https://travis-ci.org/saucisson/lua-copas-ev.svg?branch=master)](https://travis-ci.org/saucisson/lua-copas-ev)
[![Coverage Status](https://coveralls.io/repos/saucisson/lua-copas-ev/badge.svg?branch=master&service=github)](https://coveralls.io/github/saucisson/lua-copas-ev?branch=master)
[![Join the chat at https://gitter.im/saucisson/lua-copas-ev](https://badges.gitter.im/saucisson/lua-copas-ev.svg)](https://gitter.im/saucisson/lua-copas-ev?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

# Copas using lua-ev

Copas-ev is a reimplementation of [copas](http://keplerproject.github.io/copas/)
using [lua-ev](https://github.com/brimworks/lua-ev). It loses portability,
but gains performance, by using the `libev` binding.

The first goal of this module is to be 100% compatible with copas. It only
reimplements the core functionalities in `copas.lua`, not the ones in
`copas.*` modules. Thus, `copas.ev` depends on `copas` to be fully usable.

## Install

This module is available in [luarocks](https://luarocks.org):

````sh
    luarocks install copas-ev
````

## Usage

Everywhere you are using the `copas`` module,
replace it with `copas.ev`

```lua
    local copev = require "copas.ev"
```

If you are using an already existing `copas` application, make `copas.ev`
the default instead of `copas`, before any `require "copas"`:

```lua
    copev.make_default ()
```

## Compatibility

The default behavior of `copas.ev` should be compatible with `copas`.
There is also a slightly incompatible mode, that automatically wraps sockets.
It can be enabled using:

````lua
    copev.compatibility = false
````

Moreover, `copas.ev` adds some primitives, such as missing functions in socket
wrappers (`getpeername`, `getsockname`, `getstats`), a `unix` socket type, ...

# Test

Tests are written for [busted](http://olivinelabs.com/busted).
```bash
  busted test/*.lua
```
