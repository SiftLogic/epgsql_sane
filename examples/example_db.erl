%% @author Daniel Luna <daniel@lunas.se>
%% @copyright 2013 SiftLogic LLC
-module(example_db).
-author('Daniel Luna <daniel@lunas.se>').
-export([example1/0]).

-define(SDB_POOL_NAME, your_pgsql_db_name).
-include("../include/sdb.hrl").

example1() ->
    Arg1 = 1,
    Arg2 = two,
    Arg3 = [<<"three">>],
    bquery("SELECT *"
           "  FROM table"
           "  WHERE column1 = $1,"
           "    AND column2 = $2,"
           "    AND column3 = $3",
           [{Arg1, int4},
            {Arg2, text},
            {Arg3, textarray}],
           [{return, proplists}]).
