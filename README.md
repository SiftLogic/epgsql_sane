epgsql_sane
===========

####License: MIT
####Copyright: 2012, 2013 SiftLogic LLC <http://siftlogic.com>

A sane and simplified interface for epgsql. Built on top of epgsql and
epgsql_pool.

## Short primer

If your `sys.config` looks like the following

```erlang
{epgsql_pool, [{pools, [foo_db, bar_db]},
               {foo_db, {150, [{host, "localhost"},
                               {port, 5432},
                               {username, "username"},
                               {password, "pazzw0rd"},
                               {database, "the_foo_database"}]}},
               {bar_db, {150, [{host, "localhost"},
                               {port, 5432},
                               {username, "username"},
                               {password, "pazzw0rd"},
                               {database, "some_baz"},
                               {timeout, 10000}]}}]}
```

then your database accessor module should start with

```erlang
-module(something_db).

-export([foobar/0]).

-define(SDB_POOL_NAME, foo_db).
-include("../../smlib/include/sdb.hrl").

foobar() ->
    [...]
```

This will allow access to all relevant database functions locally in
the module, automatically accessing the correct database pool/database.

## Functions

Through use of the `sdb.hrl` include file, the following database
accessor functions are available.

> It's also possible to call `sdb` directly if the pool name needs to
> be different.  It is not recommended to use a single module for
> calls to multiple database pools.

`bcall(Function, ArgsAndTypes, ReturnValues, Options)`
`bquery(Query, ArgsAndTypes, Options)`

> These are the main functions of `epgsql_sane`.  They allow for
> access to SQL functionality in an Erlangy way.

`transaction(F) -> any()`

> Wraps the function `F` in a SQL transaction, committing if F doesn't
> crash.

`transaction_ok(F) -> ok | {ok, any()} | {ok, any(), ...} | {error, any(), ...}`

> Calls `transaction(F)`, aborting the transaction if the return value
> is a tuple with `error` in the first position.  Committing the
> transaction if the return value is `ok` or an `ok` tuple (of any
> size).  Crashing for all other return values.

`savepoint(atom())`  
`rollback(atom())`

> Creates and rolls back a named savepoint respectively.  A common use
> case for this is where an operation might fail.  The transaction
> context will be unusable without a savepoint.  E.g.

>     savepoint(foo_insertion),
>     case bquery("INSERT INTO foo"
>                 "  (bar, baz)"
>                 "  VALUES"
>                 "  ($1, $1)"
>                 "  RETURNING foo_id",
>                 [{Foo, int4},
>                  {Bar, text}],
>                 [{return, one_value}]) of
>         {ok, 1, FooId} ->
>             ok;
>         {error, {conflict, foo_bar_uk}} ->
>             rollback(foo_insertion),
>             more_code_here
>     end,
