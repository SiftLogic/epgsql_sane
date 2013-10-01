-export([iquery/1, iquery/2,
         bcall/2, bcall/3, bcall/4,
         pcall/2, pcall/3,
         bquery/2, bquery/3,
         bfold/4, bfold/5,
         transaction/1,
         transaction_ok/1,
         savepoint/1,
         rollback/1,
         pquery/2,
         pfold/4,
         pfold/5,
         pexec/2,
         pexec/3,
         close_cursor/1,
         pclose/1,
         ok/1]).

iquery(Query) ->
    sdb:iquery(?SDB_POOL_NAME, Query).

iquery(Query, Opts) ->
    sdb:iquery(?SDB_POOL_NAME, Query, Opts).

bcall(Function, ArgsAndTypes) ->
    sdb:bcall(?SDB_POOL_NAME, Function, ArgsAndTypes).

bcall(Function, ArgsAndTypes, ReturnValues) ->
    sdb:bcall(?SDB_POOL_NAME, Function, ArgsAndTypes, ReturnValues).

bcall(Function, ArgsAndTypes, ReturnValues, Options) ->
    sdb:bcall(?SDB_POOL_NAME, Function, ArgsAndTypes, ReturnValues, Options).

pcall(Function, ArgTypes) ->
    sdb:pcall(?SDB_POOL_NAME, Function, ArgTypes).

pcall(Function, ArgTypes, ReturnValues) ->
    sdb:pcall(?SDB_POOL_NAME, Function, ArgTypes, ReturnValues).

bquery(Query, ArgsAndTypes) ->
    sdb:bquery(?SDB_POOL_NAME, Query, ArgsAndTypes).

bquery(Query, ArgsAndTypes, Options) ->
    sdb:bquery(?SDB_POOL_NAME, Query, ArgsAndTypes, Options).

bfold(Fun, Acc, Query, ArgsAndTypes) ->
    sdb:bfold(Fun, Acc, ?SDB_POOL_NAME, Query, ArgsAndTypes).

bfold(Fun, Acc, Query, ArgsAndTypes, Opts) ->
    sdb:bfold(Fun, Acc, ?SDB_POOL_NAME, Query, ArgsAndTypes, Opts).

transaction(F) ->
    sdb:transaction(?SDB_POOL_NAME, F).

transaction_ok(F) ->
    sdb:transaction_ok(?SDB_POOL_NAME, F).

savepoint(SavePoint) when is_atom(SavePoint) ->
    sdb:savepoint(?SDB_POOL_NAME, SavePoint).

rollback(SavePoint) when is_atom(SavePoint) ->
    sdb:rollback(?SDB_POOL_NAME, SavePoint).

pquery(Query0, ArgTypes) ->
    sdb:pquery(?SDB_POOL_NAME, Query0, ArgTypes).

pfold(Fun, Acc, Q, Args) ->
    sdb:pfold(Fun, Acc, Q, Args).

pfold(Fun, Acc, Q, Args, Opts) ->
    sdb:pfold(Fun, Acc, Q, Args, Opts).

pexec(QRef, Args) ->
    sdb:pexec(QRef, Args).

pexec(QRef, Args, Options) ->
    sdb:pexec(QRef, Args, Options).

close_cursor(Cursor) ->
    sdb:close_cursor(Cursor).

pclose(QRef) ->
    sdb:pclose(QRef).

ok({ok, X}) -> X;
ok({ok, _, X}) -> X;
ok({ok, _, _, X}) -> X;
ok(Other) ->
    catch throw(get_stacktrace),
    error({not_ok,
           Other,
           erlang:get_stacktrace()}).
