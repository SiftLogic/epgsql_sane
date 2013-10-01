%%% @copyright 2012, 2013 SiftLogic LLC
%%% @created June 26, 2012
-module(sdb).
-author('Kat Marchán <kzm@sykosomatic.org>').
-author('Daniel Luna <daniel@lunas.se>').

-define(DB_CONN_TIMEOUT, 10*60000).
-define(MAX_NO_SLOT_RESPONSES, 2).
-define(MAX_TIMEOUTS, 2).

%% Inline queries
-export([iquery/2, iquery/3]).

%% Binding queries
-export([bquery/3, bquery/4,
         bcall/3, bcall/4, bcall/5,
         bfold/5, bfold/6]).

%% Persistent Queries
-export([pquery/3,
         pexec/2, pexec/3,
         pcall/3, pcall/4,
         pclose/1,
         pfold/4, pfold/5]).

%% Cursors
-export([get_next/1,
         close_cursor/1]).

%% Transactions
-export([transaction/2,
         transaction_ok/2,
         savepoint/2,
         rollback/2
         %%
         %% Internal until actually needed
         %%
         %% transaction/1,
         %% commit/1,
         %% rollback/1
        ]).

%% Utilities
-export([convert_filters/3]).

-include_lib("epgsql/include/pgsql.hrl").

-record(sdb_statement, {argtypes, query_text, conn_pool,
                        conn, has_rowcount, pgstatement}).

-record(cursor, {last_result = undefined,
                 sdb_statement, fetch_limit, return,
                 include_types, autoclose}).

%%
%% API
%%
iquery(ConnPool, Query) ->
    iquery(ConnPool, Query, []).

iquery(ConnPool, Query0, Options) ->
    {Query, ArgsAndTypes} = bquerify_query(Query0),
    bquery(ConnPool, Query, ArgsAndTypes, Options).

bquerify_query(Query) ->
    bquerify_query(Query, {"", []}).

bquerify_query(Empty, {Query, ArgsAndTypes})
  when Empty =:= <<>>;
       Empty =:= "" ->
    {Query, ArgsAndTypes};
bquerify_query(Bin, {Query, ArgsAndTypes}) when is_binary(Bin) ->
    bquerify_query(<<>>, {[Query | Bin], ArgsAndTypes});
bquerify_query([{Val, Type} | Rest], {Query, ArgsAndTypes}) ->
    Index = integer_to_list(length(ArgsAndTypes) + 1),
    bquerify_query(
      Rest,
      {[Query, $$, Index],
       ArgsAndTypes ++ [{Val, Type}]});
bquerify_query([X | Rest], {Query, ArgsAndTypes}) ->
    bquerify_query(Rest, bquerify_query(X, {Query, ArgsAndTypes}));
bquerify_query(X, {Query, ArgsAndTypes}) when is_integer(X) ->
    {[Query, X], ArgsAndTypes}.

bcall(ConnPool, Function, ArgsAndTypes) ->
    bcall(ConnPool, Function, ArgsAndTypes, all, []).

bcall(ConnPool, Function, ArgsAndTypes, ReturnValues) ->
    bcall(ConnPool, Function, ArgsAndTypes, ReturnValues, []).

bcall(ConnPool, Function, ArgsAndTypes, ReturnValues, Options0) ->
    Options = opts_to_list(Options0),
    bquery(ConnPool,
           format_call(
             Function,
             [case Arg of
                  {Name, _Value, Type} ->
                      {Name, Type};
                  {_Value, Type} ->
                      Type
              end || Arg <- ArgsAndTypes],
             ReturnValues),
           [case Arg of
                {_Name, Value, Type} ->
                    {Value, Type};
                {Value, Type} ->
                    {Value, Type}
            end || Arg <- ArgsAndTypes],
           Options).

pcall(ConnPool, Function, ArgTypes) ->
    pcall(ConnPool, Function, ArgTypes, all).

pcall(ConnPool, Function, ArgTypes, ReturnValues) ->
    pquery(ConnPool,
           format_call(Function, ArgTypes, ReturnValues),
           ArgTypes).

bquery(ConnPool, Query, ArgsAndTypes) ->
    bquery(ConnPool, Query, ArgsAndTypes, []).

bquery(ConnPool, Query, ArgsAndTypes, Options0) ->
    Options = opts_to_list(Options0),
    {Args, ArgTypes} = lists:unzip(ArgsAndTypes),
    case pquery(ConnPool, Query, ArgTypes, Options) of
        {ok, QRef} ->
            pexec(QRef, Args, [autoclose | Options]);
        {error, Error} ->
            {error, Error}
    end.

bfold(Fun, Acc, ConnPool, Query, ArgsAndTypes) ->
    bfold(Fun, Acc, ConnPool, Query, ArgsAndTypes, []).

bfold(Fun, Acc, ConnPool, Query, ArgsAndTypes, Options0) ->
    Options = opts_to_list(Options0),
    {Args, ArgTypes} = lists:unzip(ArgsAndTypes),
    case pquery(ConnPool, Query, ArgTypes, Options) of
        {ok, Statement} ->
            Ret = pfold(Fun, Acc, Statement, Args, Options),
            pclose(Statement),
            Ret;
        {error, Error} ->
            {error, Error}
    end.

get_connection(ConnPool) ->
    case get_transaction_conn(ConnPool) of
        false ->
            case pgsql_pool:get_connection(ConnPool, ?DB_CONN_TIMEOUT) of
                {ok, Conn} ->
                    {ok, {ConnPool, Conn}};
                {error, no_such_pool} ->
                    error({no_such_pool, ConnPool});
                {error, timeout} ->
                    get_connection(ConnPool)
            end;
        {value, ConnInfo} ->
            {ok, ConnInfo}
    end.

transaction(ConnPool, F) ->
    transaction(ConnPool),
    start_transaction_block(ConnPool),
    try Res = F(),
         end_transaction_block(ConnPool),
         commit(ConnPool),
         Res
    after
        case get_transaction_conn(ConnPool) of
            false ->
                ok;
            {value, {ConnPool, _Conn}} ->
                end_transaction_block(ConnPool),
                rollback(ConnPool)
        end
    end.

transaction_ok(ConnPool, F) ->
    transaction(
      ConnPool,
      fun() ->
              savepoint(ConnPool, transaction_ok_savepoint),
              case F() of
                  ok ->
                      ok;
                  Okish when element(1, Okish) =:= ok ->
                      Okish;
                  Errorish when element(1, Errorish) =:= error ->
                      rollback(ConnPool, transaction_ok_savepoint),
                      Errorish
              end
      end).

transaction(ConnPool) ->
    case get_transaction_conn(ConnPool) =:= false
        andalso not(in_transaction_block(ConnPool)) of
        true ->
            {ok, ConnInfo} = get_connection(ConnPool),
            set_transaction_conn(ConnPool, ConnInfo),
            ok = bquery(ConnPool, "BEGIN", [], none);
        false ->
            error(already_in_transaction)
    end.

commit(ConnPool) ->
    case in_transaction_block(ConnPool) of
        false ->
            ok = bquery(ConnPool, "COMMIT", [], none),
            {ConnPool, Conn} = pop_transaction_conn(ConnPool),
            ok = pgsql_pool:return_connection(ConnPool, Conn);
        true ->
            error(in_transaction_block)
    end.

savepoint(ConnPool, SavePoint) when is_atom(SavePoint) ->
    ok = bquery(ConnPool, ["SAVEPOINT ", atom_to_list(SavePoint)], [], none).

rollback(ConnPool) ->
    case in_transaction_block(ConnPool) of
        false ->
            ok = bquery(ConnPool, "ROLLBACK", [], none),
            {ConnPool, Conn} = pop_transaction_conn(ConnPool),
            ok = pgsql_pool:return_connection(ConnPool, Conn);
        true ->
            error(in_transaction_block)
    end.

rollback(ConnPool, SavePoint) when is_atom(SavePoint) ->
    ok = bquery(ConnPool, ["ROLLBACK TO ", atom_to_list(SavePoint)], [], none).

pquery(ConnPool, Query0, ArgTypes) ->
    pquery(ConnPool, Query0, ArgTypes, []).

pquery(ConnPool, Query0, ArgTypes, Options0) when is_atom(ConnPool) ->
    Options = opts_to_list(Options0),
    {ok, {ConnPool, Conn}} = get_connection(ConnPool),
    {Query, HasRowCount} = compile_query(Query0, Options),
    case pgsql:parse(Conn, Query, ArgTypes) of
        {ok, Pgstatement} ->
            {ok, #sdb_statement{argtypes = ArgTypes,
                                query_text = Query,
                                conn_pool = ConnPool,
                                conn = Conn,
                                has_rowcount = HasRowCount,
                                pgstatement = Pgstatement}};
        {error, Error} ->
            maybe_debug(Query, Options),
            {error, convert_error(Error)}
    end.

pfold(Fun, Acc, Q, Args) ->
    pfold(Fun, Acc, Q, Args, []).

pfold(Fun, Acc, StatementOrCursor, Args, Options0) ->
    Options = opts_to_list(Options0),
    Res = case StatementOrCursor of
              #sdb_statement{} ->
                 pexec(StatementOrCursor, Args, Options);
              #cursor{} ->
                 get_next(StatementOrCursor)
          end,
    case Res of
        {ok, #cursor{} = Cursor} ->
            pfold(Fun, Acc, Cursor, Args, Options);
        {error, Error} ->
            {error, Error};
        Res when element(1, Res) =:= ok ->
            case proplists:get_value(fetch_limit, Options, 0) =:= 0 of
                true ->
                    {ok, lists:foldl(Fun, Acc, get_result(Res))};
                false ->
                    {ok, Acc}
            end;
        Res when element(1, Res) =:= partial ->
           Cursor = element(2, Res),
           pfold(Fun,
                 lists:foldl(Fun, Acc, get_result(Res)),
                 Cursor,
                 Args,
                 Options)
    end.

pexec(Statement, Args) ->
    pexec(Statement, Args, []).

pexec(#sdb_statement{conn = Conn, pgstatement = Pgstatement} = Statement,
      Args, Options0) ->
    Options = opts_to_list(Options0),
    maybe_debug(Statement, Args, Options),
    case pgsql:bind(Conn, Pgstatement, Args) of
        ok ->
            Cursor =
                #cursor{
              sdb_statement = Statement,
              fetch_limit = proplists:get_value(
                              fetch_limit, Options, 0),
              return = proplists:get_value(return, Options, rows),
              include_types = proplists:get_value(
                                include_types, Options, false),
              autoclose = proplists:get_value(
                            autoclose, Options, false)},
            case Cursor#cursor.fetch_limit =:= 0 of
                true ->
                    get_next(Cursor);
                false ->
                    {ok, Cursor}
            end;
        {error, Error} ->
            {error, convert_error(Error)}
    end.

get_next(#cursor{last_result = Ret} = Cursor) when is_tuple(Ret) ->
    set_result(Ret, Cursor);
get_next(#cursor{sdb_statement =
                     #sdb_statement{conn = Conn,
                                    pgstatement = Pgstatement} = Statement,
                 last_result = undefined,
                 fetch_limit = LimitSetting,
                 return = ReturnSetting,
                 include_types = InclTypeSetting,
                 autoclose = AutocloseSetting} = Cursor) ->
    Ret = pgsql:execute(Conn, Pgstatement, LimitSetting),
    case Ret of
        {error, Error} ->
           pgsql:sync(Conn),
           case AutocloseSetting of
               true ->
                  pclose(Statement);
               false ->
                  ok
           end,
           {error, convert_error(Error)};
        _ when element(1, Ret) =:= ok ->
           Final =
                case LimitSetting =:= 0 of
                    true ->
                        transform_results(
                          Ret, Statement, ReturnSetting, InclTypeSetting);
                    false ->
                        case get_result(Ret) of
                            [] ->
                                set_result(Ret, Cursor);
                            Res when is_list(Res) ->
                                [ok | Rest] =
                                    tuple_to_list(Ret),
                                transform_results(
                                  list_to_tuple(
                                    [partial,
                                     Cursor#cursor{last_result = Ret}
                                     | Rest]),
                                  Statement,
                                  ReturnSetting,
                                  InclTypeSetting)
                        end
                end,
           pgsql:sync(Conn),
           case AutocloseSetting of
               true ->
                   pclose(Statement);
               false ->
                  ok
           end,
           Final;
        _ when element(1, Ret) =:= partial ->
           [partial | Rest] =
               tuple_to_list(Ret),
           transform_results(
             list_to_tuple([partial, Cursor | Rest]),
             Statement,
             ReturnSetting,
             InclTypeSetting)
    end.

close_cursor(#cursor{sdb_statement = #sdb_statement{conn = Conn} = Statement,
                     autoclose = Autoclose}) ->
    pgsql:sync(Conn),
    case Autoclose of
        true ->
           pclose(Statement);
        false ->
           ok
    end.

pclose(#sdb_statement{conn_pool = ConnPool,
                      conn = Conn,
                      pgstatement = Pgstatement}) ->
    pgsql:close(Conn, Pgstatement),
    pgsql:sync(Conn),
    case get({'$sdb_transaction', ConnPool}) of
        undefined ->
            ok = pgsql_pool:return_connection(ConnPool, Conn);
        {ConnPool, Conn} ->
            ok
    end.

%%
%% Utils
%%
convert_filters(Filters, NumberOfArgs, FieldsToFilter) ->
    lists:foldr(
      fun(_, {error, Error}) -> {error, Error};
         ({{Operator, Field, Condition}, N}, {ok, Acc}) ->
              case lists:keyfind(Field, 1, FieldsToFilter) of
                  false -> {error, {filter_not_supported, Field}};
                  {Field, Join, ActualField, Type} ->
                      case case Type of
                               date ->
                                   normalize_timestamp:date(
                                     Condition, ymd);
                               timestamptz ->
                                   normalize_timestamp:datetime(
                                     Condition, utc, ts_ymd);
                               varchararray ->
                                   {ok, binary:split(
                                          Condition, <<",">>, [global])};
                               varchar ->
                                   {ok, Condition};
                               text ->
                                   {ok, Condition};
                               Type when Type =:= int4;
                                         Type =:= int8 ->
                                   {ok, list_to_integer(binary_to_list(
                                                          Condition))}
                           end of
                          {ok, Normalized} ->
                              {ok, [{Join,
                                     ["    AND ",
                                      atom_to_list(ActualField),
                                      case Type of
                                          date -> "::date";
                                          _ -> ""
                                      end,
                                      case Operator of
                                          equals -> " = (";
                                          less_than -> " < (";
                                          less_or_equal -> " <= (";
                                          greater_or_equal -> " >= (";
                                          one_of -> " = ANY(";
                                          overlap -> " && ARRAY["
                                      end, "$", integer_to_list(N),
                                      case Operator of
                                          overlap -> "]";
                                          _ -> ")"
                                      end],
                                     {Normalized, Type}} | Acc]};
                          {error, _Error} ->
                              {error, {conversion_failed, Condition, Type}}
                      end
              end
      end,
      {ok, []},
      lists:zip(Filters,
                lists:seq(NumberOfArgs + 1,
                          NumberOfArgs + length(Filters)))).

%%
%% Query result transformation
%%
transform_results(Results0,
                  #sdb_statement{has_rowcount = HasRowCount,
                                 pgstatement = Pgstatement0},
                  ReturnFilter,
                  IncludeTypes) ->
    {Results1, Pgstatement} =
        case HasRowCount of
            true ->
                {case Results0 of
                     {ok, N} when is_integer(N) ->
                         {ok, N};
                     {ok, []} ->
                         %% XXX: we don't actually have the least
                         %% amount of clue of what value to use here.
                         %% Getting zero rows back could just as well
                         %% be because limit and/or offset are out of
                         %% bounds.
                         {ok, 0, []};
                     {ok, Rows} when is_list(Rows) ->
                         RowCount = lists:last(tuple_to_list(hd(Rows))),
                         NewRows = [list_to_tuple(
                                      lists:reverse(
                                        tl(lists:reverse(
                                             tuple_to_list(Row)))))
                                    || Row <- Rows],
                         {ok, RowCount, NewRows};
                     {ok, N, Rows} when is_integer(N), is_list(Rows) ->
                         NewRows = [lists:reverse(tl(lists:reverse(Row)))
                                    || Row <- Rows],
                         {ok, N, NewRows}
                 end,
                 drop_last_column(Pgstatement0)};
            false ->
                {Results0, Pgstatement0}
        end,
    Results =
        case IncludeTypes of
            combined ->
                set_result(Results1,
                           [list_to_tuple(
                              lists:zip(tuple_to_list(ValTuple),
                                        column_types(Pgstatement)))
                            || ValTuple <- get_result(Results1)]);
            separate ->
                Types = column_types(Pgstatement),
                case Results1 of
                    {ok, Res} ->
                        {ok, Types, Res};
                    {ok, X, Res} ->
                        {ok, X, Types, Res}
                end;
            false ->
                Results1
        end,
    transform_results(Results, Pgstatement, ReturnFilter).

column_types(#statement{columns = Cols}) ->
    [Col#column.type || Col <- Cols].

transform_results(Results, _, rows) ->
    Results;
transform_results(Results, _, none) ->
    element(1, Results);
transform_results(Results, _, has_results) ->
    case get_result(Results) of
        [] ->
            set_result(Results, false);
        0 ->
            set_result(Results, false);
        [_|_] ->
            set_result(Results, true)
    end;
transform_results(Results, _, has_one_result) ->
    case get_result(Results) of
        [] ->
            set_result(Results, false);
        0 ->
            set_result(Results, false);
        [_] ->
            set_result(Results, true);
        [_, _ | _] ->
            {error, too_many_results}
    end;
transform_results(Results, _, one_row) ->
    case get_result(Results) of
        [Row] ->
            set_result(Results, Row);
        [] ->
            {error, no_results};
        0 ->
            {error, no_results};
        [_, _ | _] ->
            {error, too_many_results}
    end;
transform_results(Results, _, one_value) ->
    case get_result(Results) of
        [{Val}] ->
            set_result(Results, Val);
        [Tuple] when is_tuple(Tuple) ->
            {error, too_many_values};
        [] ->
            {error, no_results};
        0 ->
            {error, no_results};
        [_, _ | _] ->
            {error, too_many_results}
    end;
transform_results(Results, Pgstatement, RecordSpec)
  when element(1, RecordSpec) =:= one_record ->
    case get_result(Results) of
        [Row] ->
            set_result(
              Results,
              case RecordSpec of
                  {one_record, RecName, RecInfo} ->
                      recordify(Row, Pgstatement, RecName, RecInfo);
                  {one_record, Prototype} ->
                      recordify(Row, Prototype)
              end);
        [] ->
            {error, no_results};
        0 ->
            {error, no_results};
        [_, _ | _] ->
            {error, too_many_results}
    end;
transform_results({ok, 0}, _, RecordSpec)
  when element(1, RecordSpec) =:= records ->
    {ok, 0, []};
transform_results(Results, Pgstatement, RecordSpec)
  when element(1, RecordSpec) =:= records ->
    set_result(
      Results,
      [case RecordSpec of
           {records, Prototype} ->
               recordify(Row, Prototype);
           {records, RecName, RecInfo} ->
               recordify(Row, Pgstatement, RecName, RecInfo)
       end|| Row <- get_result(Results)]);
transform_results({ok, 0}, _, proplists) ->
    {ok, 0, []};
transform_results(Results, Pgstatement, proplists) ->
    set_result(
      Results,
      [proplistify(Row, Pgstatement) || Row <- get_result(Results)]);
transform_results(Results, Pgstatement, one_proplist) ->
    case get_result(Results) of
        [Row] ->
            set_result(Results, proplistify(Row, Pgstatement));
        [] ->
            {error, no_results};
        0 ->
            {error, no_results};
        [_, _ | _] ->
            {error, too_many_results}
    end;
transform_results({ok, 0}, _, column) ->
    {ok, 0, []};
transform_results(Results, _, column) ->
    set_result(
      Results,
      [begin {V} = Row, V end || Row <- get_result(Results)]);
transform_results(_, _, Unknown) ->
    throw({unrecognized_result_transformer, Unknown}).

get_result(Results) ->
    element(tuple_size(Results), Results).

set_result(Results, NewVal) ->
    setelement(tuple_size(Results), Results, NewVal).

recordify(Row, Prototype) ->
    NewRes = list_to_tuple([element(1, Prototype) | tuple_to_list(Row)]),
    case tuple_size(Prototype) =:= tuple_size(NewRes) of
        true ->
            NewRes;
        false ->
            exit({record_size_mismatch, Prototype, NewRes})
    end.

recordify(Row, #statement{columns = Cols} = Pgstatement, RecName, RecInfo) ->
    case length(Cols) =:= length(RecInfo) of
        true ->
            Plist = proplistify(Row, Pgstatement),
            list_to_tuple(
              [RecName |
               [case lists:keyfind(Key, 1, Plist) of
                    {Key, Value} ->
                        Value;
                    false ->
                        exit({record_field_name_mismatch, RecName, Key})
                end || Key <- RecInfo]]);
        false ->
            exit({record_size_mismatch, Row, RecName})
    end.

proplistify(Row, #statement{columns = Cols}) ->
    ColNames = [binary_to_atom(Name, utf8) || #column{name = Name} <- Cols],
    lists:zip(ColNames, tuple_to_list(Row)).

drop_last_column(#statement{columns = Cols} = Pgstatement) ->
    Pgstatement#statement{columns = lists:reverse(tl(lists:reverse(Cols)))}.

%%
%% sorting and pagination
%%

compile_query(Query, Options) ->
    {Preprefix, Prefix, Postfix} =
        case lists:keymember(sort, 1, Options) orelse
            lists:keymember(paginate, 1, Options) of
            true ->
                {"SELECT listing.*",
                 "  FROM (",
                 ") AS listing"};
            false ->
                {"", "", ""}
        end,
    Sort =
        case lists:keyfind(sort, 1, Options) of
            false -> "";
            {sort, SortColumn, SortOrder} ->
                ["  ORDER BY ", SortColumn, " ",
                 case SortOrder of
                     ascending -> "ASC NULLS FIRST";
                     descending -> "DESC NULLS LAST"
                 end]
        end,
    {RowCount, Paginate} =
        case lists:keyfind(paginate, 1, Options) of
            false -> {"", ""};
            {paginate, Limit, Offset} ->
                {"       COUNT(*) OVER () AS \"__totalrowcount__\"",
                 [case Limit =:= infinity of
                      true -> " ";
                      false -> ["  LIMIT ", integer_to_list(Limit)]
                  end,
                  " OFFSET ", integer_to_list(Offset)]}
        end,
    {iolist_to_binary(
       [Preprefix,
        case RowCount =:= "" orelse Preprefix =:= "" of
            true -> "";
            false -> ", "
        end,
        RowCount, Prefix, Query,
        Postfix, Sort, Paginate]),
     RowCount =/= "" orelse
     proplists:get_value(has_rowcount, Options, false)}.

%%
%% *call formatting
%%
format_call(Function, ArgSpecs, ReturnValues) ->
    FName = case Function of
                {Schema, FuncName} ->
                    io_lib:format("~s.~s", [Schema, FuncName]);
                Function when is_atom(Function) ->
                    atom_to_list(Function);
                Function when is_list(Function);
                              is_binary(Function) ->
                    Function
            end,
    io_lib:format("SELECT ~s FROM ~s~s",
                  [case ReturnValues of
                       all ->
                           "*";
                       _ when is_list(ReturnValues) ->
                           string:join(
                             [case ValSpec of
                                  {Col, Alias} ->
                                      io_lib:format("~s AS ~s", [Col, Alias]);
                                  Col ->
                                      io_lib:format("~s", [Col])
                              end || ValSpec <- ReturnValues],
                             ", ");
                         _ when is_atom(ReturnValues);
                                is_integer(ReturnValues) ->
                           ReturnValues
                   end,
                   FName,
                   generate_arglist(ArgSpecs)]).

generate_arglist(ArgSpecs) ->
    Args = [case Spec of
                Spec when is_atom(Spec) ->
                    [$$, integer_to_list(N)];
                {Var, _Type} ->
                    io_lib:format("_~s := $~p", [Var, N])
            end
            || {N, Spec} <-
                   lists:zip(
                     lists:seq(1, length(ArgSpecs)),
                     ArgSpecs)],
    [$(, string:join(Args, ", "), $)].

%%
%% Transactions
%%
in_transaction_block(ConnPool) ->
    true =:= get({'$sdb_in_transaction_block', ConnPool}).

get_transaction_conn(ConnPool) ->
    case get({'$sdb_transaction', ConnPool}) of
        undefined ->
            false;
        {_, _} = Conn ->
            {value, Conn}
    end.

set_transaction_conn(ConnPool, ConnInfo) ->
    undefined = put({'$sdb_transaction', ConnPool}, ConnInfo),
    ok.

pop_transaction_conn(ConnPool) ->
    {value, {ConnPool, ConnInfo}} = get_transaction_conn(ConnPool),
    {ConnPool, ConnInfo} = erase({'$sdb_transaction', ConnPool}).

start_transaction_block(ConnPool) ->
    undefined = put({'$sdb_in_transaction_block', ConnPool}, true),
    ok.

end_transaction_block(ConnPool) ->
    erase({'$sdb_in_transaction_block', ConnPool}),
    ok.

%%
%% Error conversion
%%
convert_error(#error{code = <<"22P02">>, message = Msg}) ->
    [Type, Data] =
        re_run(Msg, "invalid input syntax for ([^:]*): \"([^\"]*)\""),
    {invalid_input_syntax, b2a(Type), Data};
convert_error(#error{code = <<"23502">>, message = Msg}) ->
    [Column] =
        re_run(Msg,
               "null value in column \"([^\"]*)\" violates"
               " not-null constraint"),
    {not_null_violation, b2a(Column)};
convert_error(#error{code = <<"23503">>, message = Msg}) ->
    {Relation, Constraint} =
        relation_constraint_error_values(Msg),
    {foreign_key_violation, b2a(Relation), b2a(Constraint)};
convert_error(#error{code = <<"23505">>, message = Msg}) ->
    [UniqueConstraint] =
        re_run(Msg,
               "duplicate key value violates"
               " unique constraint \"([^\"]*)\""),
    {conflict, b2a(UniqueConstraint)};
convert_error(#error{code = <<"23514">>, message = Msg}) ->
    {Relation, Constraint} =
        relation_constraint_error_values(Msg),
    {constraint_violation, b2a(Relation), b2a(Constraint)};
convert_error(#error{code = <<"42703">>, message = Msg}) ->
    case re_run(Msg,
                "column (\"[^\"]*\"|[^\ ]*)"
                " (?:does not exist|of relation"
                " \"\([^\"]*)\" does not exist)") of
        [ColumnName, TableName] ->
            {invalid_column, ColumnName, TableName};
        [ColumnName] ->
            {invalid_column, ColumnName}
    end;
convert_error(#error{code = <<"22021">>, message = Msg}) ->
    [Encoding, Sequence] =
        re_run(Msg, "invalid byte sequence for encoding \"([^\"]*)\": (.+)"),
    {invalid_byte_sequence, Encoding, Sequence};
convert_error(#error{code = <<"P0001">>, message = Msg}) ->
    {exception, Msg};
convert_error(Error) ->
    Error.

re_run(Data, Pattern) ->
    case re:run(Data, Pattern, [{capture, all_but_first, binary}]) of
        {match, Matches} ->
            Matches;
        nomatch ->
            exit({unexpected_error_message, Data, Pattern})
    end.

b2a(B) -> binary_to_atom(B, latin1).

relation_constraint_error_values(Msg) ->
    {match, [[Relation], [Constraint]]} =
        re:run(Msg, "\"([^\"]*)\"",
               [global, {capture, all_but_first, binary}]),
    {Relation, Constraint}.

%%
%% Misc
%%
maybe_debug(Query, Options) ->
    case proplists:get_value(debug, Options, false) orelse
        proplists:get_value(erldebug, Options, false) of
        true ->
            io:format("SDB QUERY:~n~s;~n", [prettify_query(Query)]);
        false ->
            ok
    end.

maybe_debug(Statement, Args, Options) ->
    case proplists:get_value(debug, Options, false) of
        true ->
            io:format("EXECUTING SDB QUERY:~n~s;~n",
                      [prettify_query(
                         Statement#sdb_statement.query_text,
                         Args,
                         Statement#sdb_statement.argtypes)]);
        false ->
            ok
    end,
    case proplists:get_value(erldebug, Options, false) of
        true ->
            Pretty = prettify_query(Statement#sdb_statement.query_text),
            io:format("EXECUTING SDB QUERY:~n~s;~nUnformatted: ~s;"
                      "~nArgs: ~p;~n",
                      [Pretty,
                       re:replace(Pretty, "\\s+", " ", [global]),
                       lists:zip(
                         Args, Statement#sdb_statement.argtypes)]);
        false ->
            ok
    end.

opts_to_list(Return) when is_atom(Return); is_tuple(Return) ->
    [{return, Return}];
opts_to_list(Options) when is_list(Options) ->
    Options.

prettify_query(Query, Args, ArgTypes) ->
    lists:foldr(
      fun({Index, Arg, ArgType}, QueryAcc) ->
              re:replace(QueryAcc,
                         ["\\$",integer_to_list(Index)],
                         format_type(ArgType, Arg),
                         [global])
      end,
      prettify_query(Query),
      lists:zip3(lists:seq(1, length(Args)), Args, ArgTypes)).

prettify_query(Query) ->
    re:replace(Query, "([^\\s]) {2}", "\\1\n  ", [global, {return, binary}]).

format_type(Type, null) -> io_lib:format("NULL::~s", [Type]);
format_type(bool, true) -> "TRUE";
format_type(bool, false) -> "FALSE";
format_type(boolean, true) -> "TRUE";
format_type(boolean, false) -> "FALSE";
format_type(point, {X, Y}) -> io_lib:format("(~p,~p)::point", [X, Y]);
format_type(Type, Text) when Type =:= bytea;
                             Type =:= text;
                             Type =:= varchar ->
    io_lib:format("'~s'::~s",
                  [re:replace(io_lib:format("~s", [Text]), "'", "''", [global]),
                   Type]);
format_type(Type, {A, B, C, D}) when Type =:= inet;
                                     Type =:= cidr ->
    io_lib:format("'~p.~p.~p.~p'::~s", [A, B, C, D, Type]);
format_type(Type, {{A, B, C, D}, Mask}) when Type =:= inet;
                                             Type =:= cidr ->
    io_lib:format("'~p.~p.~p.~p/~p'::~s", [A, B, C, D, Mask, Type]);
format_type(Type, {_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _} = Ip)
  when Type =:= inet;
       Type =:= cidr ->
    %% TODO - this probably needs more work.
    io_lib:format("'~s'::~s", [string:join(
                                 lists:map(fun erlang:integer_to_list/1,
                                           tuple_to_list(Ip)),
                                 "::"),
                               Type]);
format_type(Type, {{_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _} = Ip,
                   Mask}) when Type =:= inet;
                               Type =:= cidr ->
    %% TODO - this probably needs more work.
    io_lib:format("'~s/~p'::~s", [string:join(
                                    lists:map(fun erlang:integer_to_list/1,
                                              tuple_to_list(Ip)),
                                    "::"),
                                  Mask,
                                  Type]);
format_type(boolarray, L) when is_list(L)        -> format_array(bool, L);
format_type(inetarray, L) when is_list(L)        -> format_array(inet, L);
format_type(int2array, L) when is_list(L)        -> format_array(int2, L);
format_type(int4array, L) when is_list(L)        -> format_array(int4, L);
format_type(int8array, L) when is_list(L)        -> format_array(int8, L);
format_type(float4array, L) when is_list(L)      -> format_array(float4, L);
format_type(float8array, L) when is_list(L)      -> format_array(float8, L);
format_type(chararray, L) when is_list(L)        -> format_array(bpchar, L);
format_type(varchararray, L) when is_list(L)     -> format_array(varchar, L);
format_type(textarray, L) when is_list(L)        -> format_array(text, L);
format_type(recordarray, L) when is_list(L)      -> format_array(record, L);
format_type(Type, Data) -> io_lib:format("~p::~s", [Data, Type]).

format_array(Type, L) ->
    io_lib:format("ARRAY[~s]::~s[]",
                  [string:join(
                     [re:replace(
                        format_type(Type, X),
                        io_lib:format("::~s", [Type]),
                        "") || X <- L],
                     ","),
                   Type]).

%% format_type(time = Type, B)                      -> ?datetime:encode(Type, B);
%% format_type(timetz = Type, B)                    -> ?datetime:encode(Type, B);
%% format_type(date = Type, B)                      -> ?datetime:encode(Type, B);
%% format_type(timestamp = Type, B)                 -> ?datetime:encode(Type, B);
%% format_type(timestamptz = Type, B)               -> ?datetime:encode(Type, B);
%% format_type(interval = Type, B)                  -> ?datetime:encode(Type, B);
