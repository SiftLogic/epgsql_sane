%% @author Daniel Luna <daniel@lunas.se>
%% @copyright 2011, 2012, 2013 SiftLogic LLC
-module(normalize_timestamp).
-author('Daniel Luna <daniel@lunas.se>').
-export([datetime/3, date/2]).

-export([test/0, test_goread/0]).

%%  The following line is only because of a bug elsewhere in the
%%  codebase.  Sorry about that.
datetime(_, missing ,_) -> {error, {invalid_default_timezone, missing}};
datetime(String, DefaultTz, FieldType) ->
    DefaultOrder = type_to_default(FieldType),
    case string_to_datetime(String, DefaultOrder) of
        {{{_, _, _}, {_, _, _}} = Datetime, missing} ->
            datetime_to_iso8601(Datetime, tz_offset(DefaultTz));
        {{{_, _, _}, {_, _, _}} = Datetime, Offset} ->
            datetime_to_iso8601(Datetime, Offset);
        {error, Error} ->
            {error, Error}
    end.

datetime_to_iso8601({{Y0, M0, D0}, {_, _, _}} = Datetime, Tz) ->
    case calendar:valid_date(Y0, M0, D0) of
       true ->
            {ok, calendar:gregorian_seconds_to_datetime(
                   calendar:datetime_to_gregorian_seconds(Datetime) + Tz)};
       false ->
           {error, invalid_date}
    end.

date(String, FieldType) ->
    DefaultOrder = type_to_default(FieldType),
    case string_to_datetime(String, DefaultOrder) of
        {{{Y, M, D} = Date, {_, _, _}}, _} ->
           case calendar:valid_date(Y, M, D) of
               true ->
                   {ok, Date};
               false ->
                   {error, invalid_date}
           end;
        {error, Error} ->
            {error, Error}
    end.

type_to_default(dt_dmy) -> dmy;
type_to_default(ts_dmy) -> dmy;
type_to_default(_) -> mdy. %% Ugly...

string_to_datetime(String, DefaultOrder) ->
    Lower = string:to_lower(String),
    parse(tokenize(normalize_whitespace(Lower)), DefaultOrder).

normalize_whitespace(String) ->
    re:replace(
      re:replace(String, "\\s+", " ", [global, unicode]),
      "^ | $",
      "",
      [global, {return, list}, unicode]).

-define(SECONDS_PER_MINUTE, 60).
-define(SECONDS_PER_HOUR, 3600).

-define(is_sign(X), (X =:= '+' orelse X =:= '-')).

-define(is_y(Y), is_integer(Y)).
-define(is_m(M), ((is_integer(M) andalso M =< 12) orelse
                 M =:= jan orelse
                 M =:= feb orelse
                 M =:= mar orelse
                 M =:= apr orelse
                 M =:= may orelse
                 M =:= jun orelse
                 M =:= jul orelse
                 M =:= aug orelse
                 M =:= sep orelse
                 M =:= oct orelse
                 M =:= nov orelse
                 M =:= dec)).
-define(is_d(D), (is_integer(D) andalso D =< 31)).
-define(is_hh(HH), (is_integer(HH) andalso HH =< 24)).
-define(is_mm(MM), (is_integer(MM) andalso MM < 60)).
-define(is_ss(SS), (is_integer(SS) andalso SS < 60)).

-define(is_tz(Tz), is_atom(Tz)).

parse([Unix], _) when is_integer(Unix) ->
    %% This works since now_to_datetime doesn't do range checking for
    %% its arguments.
    {calendar:now_to_datetime({0, Unix, 0}), 0};
parse(Tokens, DefaultOrder) ->
    Results = lists:flatten(parse(Tokens, DefaultOrder, [])),
    case Results of
        [] -> {error, invalid_date};
        [Result] -> Result;
        [Result | _] = All ->
            error_logger:warning_msg(
              "Multiple possible parse results for date.~n"
              "Tokens: ~p~nResults: ~p", [Tokens, All]),
            Result
    end.

parse([], DefaultOrder, Rest) ->
    try
        %%io:format("Rest: ~p", [Rest]),
        Date = parse_date(lists:reverse(Rest), DefaultOrder),
        [{{Date, {0,0,0}}, missing}]
    catch
        _:_ ->
            []
    end;
parse(Tokens, DefaultOrder, Rest) ->
    try
        %%io:format("T: ~p, R: ~p~n", [Tokens, Rest]),
        {Time, Tz, RemainingTokens} = parse_time(Tokens),
        %%io:format("Time: ~p, ~p, ~p~n", [Time, Tz, RemainingTokens]),
        %%io:format("parse_date(~p, ~p)~n", [lists:reverse(Rest) ++ RemainingTokens, DefaultOrder]),
        Date = parse_date(lists:reverse(Rest) ++ RemainingTokens,
                          DefaultOrder),
        [{{Date, Time}, Tz} |
         parse(tl(Tokens), DefaultOrder, [hd(Tokens) | Rest])]
    catch
        _T:_E ->
            %%io:format("T:E ~p:~p~n", [_T, _E]),
            parse(tl(Tokens), DefaultOrder, [hd(Tokens) | Rest])
    end.

parse_time([HH, ':', MM, ':', SS, PartSeconds | Rest])
  when ?is_hh(HH), ?is_mm(MM), ?is_ss(SS), is_integer(PartSeconds) ->
    {Meridiem, Rest1} = parse_meridiem(Rest),
    {Tz, Rest2} = parse_tz(Rest1),
    {{add_meridiem(HH, Meridiem), MM, SS}, Tz, Rest2};
parse_time([HH, ':', MM, ':', SS | Rest])
  when ?is_hh(HH), ?is_mm(MM), ?is_ss(SS) ->
    %%    io:format("----------- ~p ------~n", [Rest]),
    {Meridiem, Rest1} = parse_meridiem(Rest),
    %%io:format("----------- ~p ------~n", [Rest1]),
    {Tz, Rest2} = parse_tz(Rest1),
    %%io:format("----------- ~p ------~n", [Rest2]),
    {{add_meridiem(HH, Meridiem), MM, SS}, Tz, Rest2};
parse_time([HH, ':', MM, NotColon | Rest])
  when ?is_hh(HH), ?is_mm(MM), NotColon =/= ':' ->
    {Meridiem, Rest1} = parse_meridiem([NotColon | Rest]),
    {Tz, Rest2} = parse_tz(Rest1),
    {{add_meridiem(HH, Meridiem), MM, 0}, Tz, Rest2};
parse_time([HH, ':', MM])
  when ?is_hh(HH), ?is_mm(MM) ->
    {{HH, MM, 0}, missing, []}.

parse_meridiem([Meridiem | Rest])
  when Meridiem =:= am; Meridiem =:= pm ->
    {Meridiem, Rest};
parse_meridiem(Rest) ->
    {none, Rest}.

add_meridiem(Hour, none) -> Hour;
add_meridiem(12,   am) -> 0;
add_meridiem(12,   pm) -> 12;
add_meridiem(Hour, am) -> Hour;
add_meridiem(Hour, pm) -> Hour + 12.

parse_tz([Sign, Offset | Rest])
  when ?is_sign(Sign), is_integer(Offset), Offset =< 24 ->
    {sign(Sign, Offset) * ?SECONDS_PER_HOUR, Rest};
parse_tz([Sign, Offset | Rest])
  when ?is_sign(Sign), is_integer(Offset), Offset =< 1500 ->
    HH = Offset div 100,
    MM = Offset rem 100,
    {sign(Sign, HH * ?SECONDS_PER_HOUR + MM * ?SECONDS_PER_MINUTE),
     Rest};
parse_tz([Sign, Offset | Rest])
  when ?is_sign(Sign), is_integer(Offset) ->
    {sign(Sign, Offset), Rest};
parse_tz([Sign, HH, ':', MM | Rest])
  when ?is_sign(Sign), ?is_hh(HH), ?is_mm(MM) ->
    {sign(Sign, HH * ?SECONDS_PER_HOUR + MM * ?SECONDS_PER_MINUTE),
     Rest};
parse_tz([Tz, Sign, Offset | Rest])
  when ?is_tz(Tz), ?is_sign(Sign), is_integer(Offset), Offset =< 24 ->
    {sign(Sign, Offset) * ?SECONDS_PER_HOUR +
         tz_offset(Tz), Rest};
parse_tz([Tz, Sign, Offset | Rest])
  when ?is_tz(Tz), ?is_sign(Sign), is_integer(Offset), Offset =< 1500 ->
    HH = Offset div 100,
    MM = Offset rem 100,
    {sign(Sign, HH * ?SECONDS_PER_HOUR + MM * ?SECONDS_PER_MINUTE) +
         tz_offset(Tz), Rest};
parse_tz([Tz, Sign, Offset | Rest])
  when ?is_tz(Tz), ?is_sign(Sign), is_integer(Offset) ->
    {sign(Sign, Offset) + tz_offset(Tz), Rest};
parse_tz([Tz, Sign, HH, ':', MM | Rest])
  when ?is_tz(Tz), ?is_sign(Sign), ?is_hh(HH), ?is_mm(MM) ->
    {sign(Sign, HH * ?SECONDS_PER_HOUR + MM * ?SECONDS_PER_MINUTE) +
         tz_offset(Tz), Rest};
parse_tz([Tz | Rest])
  when ?is_tz(Tz) ->
    {tz_offset(Tz), Rest};
parse_tz(Rest) ->
    {missing, Rest}.

sign('+', Value) -> -Value;
sign('-', Value) -> Value.

parse_date([Y, '-', M, '-', D, t], _Order)
  when ?is_y(Y), ?is_m(M), ?is_d(D) ->
    {Y, to_int(M), D};
parse_date([Y, '-', M, '-', D], _Order)
  when ?is_y(Y), ?is_m(M), ?is_d(D) ->
    {Y, to_int(M), D};
parse_date([D, '-', M, '-', Y], _Order)
  when ?is_y(Y), ?is_m(M), is_atom(M), ?is_d(D) ->
    {Y, to_int(M), D};
parse_date([D, '/', M, '/', Y], dmy)
  when ?is_y(Y), ?is_m(M), ?is_d(D) ->
    {Y, to_int(M), D};
parse_date([D, '/', M, Y], dmy)
  when ?is_y(Y), ?is_m(M), ?is_d(D) ->
    {Y, to_int(M), D};
parse_date([D, M, Y], dmy)
  when ?is_y(Y), ?is_m(M), ?is_d(D) ->
    {Y, to_int(M), D};
parse_date([M, '/', D, '/', Y], mdy)
  when ?is_y(Y), ?is_m(M), ?is_d(D) ->
    {Y, to_int(M), D};
parse_date([M, '/', D, Y], mdy)
  when ?is_y(Y), ?is_m(M), ?is_d(D) ->
    {Y, to_int(M), D};
parse_date([M, D, Y], mdy)
  when ?is_y(Y), ?is_m(M), ?is_d(D) ->
    {Y, to_int(M), D};
parse_date([D, '/', M, '/', Y], _Order)
  when ?is_y(Y), ?is_m(M), ?is_d(D) ->
    {Y, to_int(M), D};
parse_date([M, '/', D, '/', Y], _Order)
  when ?is_y(Y), ?is_m(M), ?is_d(D) ->
    {Y, to_int(M), D};
parse_date([D, '/', M, Y], _Order)
  when ?is_y(Y), ?is_m(M), ?is_d(D) ->
    {Y, to_int(M), D};
parse_date([M, '/', D, Y], _Order)
  when ?is_y(Y), ?is_m(M), ?is_d(D) ->
    {Y, to_int(M), D};
parse_date([D, M, Y], _Order)
  when ?is_y(Y), ?is_m(M), ?is_d(D) ->
    {Y, to_int(M), D};
parse_date([M, D, Y], _Order)
  when ?is_y(Y), ?is_m(M), ?is_d(D) ->
    {Y, to_int(M), D}.

to_int(jan) -> 1;
to_int(feb) -> 2;
to_int(mar) -> 3;
to_int(apr) -> 4;
to_int(may) -> 5;
to_int(jun) -> 6;
to_int(jul) -> 7;
to_int(aug) -> 8;
to_int(sep) -> 9;
to_int(oct) -> 10;
to_int(nov) -> 11;
to_int(dec) -> 12;
to_int(Int) when is_integer(Int) -> Int.

-define(is_num(X),      (X >= $0 andalso X =< $9) ).

-define(TZ_OFFSET,
        [{a, "Alpha Time Zone", +1},
         {b, "Bravo Time Zone", +2},
         {c, "Charlie Time Zone", +3},
         {d, "Delta Time Zone", +4},
         {e, "Echo Time Zone", +5},
         {f, "Foxtrot Time Zone", +6},
         {g, "Golf Time Zone", +7},
         {h, "Hotel Time Zone", +8},
         {i, "India Time Zone", +9},
         {k, "Kilo Time Zone", +10},
         {l, "Lima Time Zone", +11},
         {m, "Mike Time Zone", +12},
         {n, "November Time Zone", -1},
         {o, "Oscar Time Zone", -2},
         {p, "Papa Time Zone", -3},
         {q, "Quebec Time Zone", -4},
         {r, "Romeo Time Zone", -5},
         {s, "Sierra Time Zone", -6},
         {t, "Tango Time Zone", -7},
         {u, "Uniform Time Zone", -8},
         {v, "Victor Time Zone", -9},
         {w, "Whiskey Time Zone", -10},
         {x, "X-ray Time Zone", -11},
         {y, "Yankee Time Zone", -12},
         {z, "Zulu Time Zone", 0},

         {acdt, "Australian Central Daylight Time", +10.50},
         {acst, "Australian Central Standard Time", +09.50},
         {act, "ASEAN Common Time", +08},
         {adt, "Atlantic Daylight Time", -03},
         {aedt, "Australian Eastern Daylight Time", +11},
         {aest, "Australian Eastern Standard Time", +10},
         {aft, "Afghanistan Time", +04.50},
         {akdt, "Alaska Daylight Time", -08},
         {akst, "Alaska Standard Time", -09},
         {amst, "Armenia Summer Time", +05},
         {amt, "Armenia Time", +04},
         {art, "Argentina Time", -03},
         {ast, "Arab Standard Time (Kuwait, Riyadh)", +03},
         {ast, "Arabian Standard Time (Abu Dhabi, Muscat)", +04},
         {ast, "Arabic Standard Time(Baghdad)", +03},
         {ast, "Atlantic Standard Time", -04},
         {awdt, "Australian Western Daylight Time", +09},
         {awst, "Australian Western Standard Time", +08},
         {azost, "Azores Standard Time", -01},
         {azt, "Azerbaijan Time", +04},
         {bdt, "Brunei Time", +08},
         {biot, "British Indian Ocean Time", +06},
         {bit, "Baker Island Time", -12},
         {bot, "Bolivia Time", -04},
         {brt, "Brasilia Time", -03},
         {bst, "Bangladesh Standard Time", +06},
         {bst, "British Summer Time", +01},
         {btt, "Bhutan Time", +06},
         {cat, "Central Africa Time", +02},
         {cct, "Cocos Islands Time", +06.50},
         {cdt, "Central Daylight Time (North America)", -05},
         {cedt, "Central European Daylight Time", +02},
         {cest, "Central European Summer Time (Cf. HAEC)", +02},
         {cet, "Central European Time", +01},
         {chadt, "Chatham Daylight Time", +13.75},
         {chast, "Chatham Standard Time", +12.75},
         {cist, "Clipperton Island Standard Time", -08},
         {ckt, "Cook Island Time", -10},
         {clst, "Chile Summer Time", -03},
         {clt, "Chile Standard Time", -04},
         {cost, "Colombia Summer Time", -04},
         {cot, "Colombia Time", -05},
         {cst, "Central Standard Time (North America)", -06},
         {cst, "China Standard Time", +08},
         {cst, "Central Standard Time (Australia)", +09.50},
         {ct, "China Time", +08},
         {cvt, "Cape Verde Time", -01},
         {cxt, "Christmas Island Time", +07},
         {chst, "Chamorro Standard Time", +10},
         {dft, "AIX specific equivalent of Central European Time", +01},
         {east, "Easter Island Standard Time", -06},
         {eat, "East Africa Time", +03},
         {ect, "Eastern Caribbean Time (does not recognise DST)", -04},
         {ect, "Ecuador Time", -05},
         {edt, "Eastern Daylight Time (North America)", -04},
         {eedt, "Eastern European Daylight Time", +03},
         {eest, "Eastern European Summer Time", +03},
         {eet, "Eastern European Time", +02},
         {est, "Eastern Standard Time (North America)", -05},
         {fjt, "Fiji Time", +12},
         {fkst, "Falkland Islands Summer Time", -03},
         {fkt, "Falkland Islands Time", -04},
         {galt, "Galapagos Time", -06},
         {get, "Georgia Standard Time", +04},
         {gft, "French Guiana Time", -03},
         {gilt, "Gilbert Island Time", +12},
         {git, "Gambier Island Time", -09},
         {gmt, "Greenwich Mean Time", 0},
         {gst, "South Georgia and the South Sandwich Islands", -2},
         {gst, "Gulf Standard Time", +04},
         {gyt, "Guyana Time", -04},
         {hadt, "Hawaii-Aleutian Daylight Time", -09},
         {haec, "Heure Avancée d'Europe Centrale francised name for CEST", +02},
         {hast, "Hawaii-Aleutian Standard Time", -10},
         {hkt, "Hong Kong Time", +08},
         {hmt, "Heard and McDonald Islands Time", +05},
         {hst, "Hawaii Standard Time", -10},
         {ict, "Indochina Time", +07},
         {idt, "Israeli Daylight Time", +03},
         {irkt, "Irkutsk Time", +08},
         {irst, "Iran Standard Time", +03.50},
         {ist, "Indian Standard Time", +05.50},
         {ist, "Irish Summer Time", +01},
         {ist, "Israel Standard Time", +02},
         {jst, "Japan Standard Time", +09},
         {krat, "Krasnoyarsk Time", +07},
         {kst, "Korea Standard Time", +09},
         {lhst, "Lord Howe Standard Time", +10.50},
         {lint, "Line Islands Time", +14},
         {magt, "Magadan Time", +11},
         {mdt, "Mountain Daylight Time (North America)", -06},
         {met, "Middle European Time Same zone as CET", +02},
         {mest, "Middle European Saving Time Same zone as CEST", +02},
         {mit, "Marquesas Islands Time", -09.50},
         {msd, "Moscow Summer Time", +04},
         {msk, "Moscow Standard Time", +03},
         {mst, "Malaysian Standard Time", +08},
         {mst, "Mountain Standard Time (North America)", -07},
         {mst, "Myanmar Standard Time", +06.50},
         {mut, "Mauritius Time", +04},
         {myt, "Malaysia Time", +08},
         {ndt, "Newfoundland Daylight Time", -02.50},
         {nft, "Norfolk Time", +11.50},
         {npt, "Nepal Time", +05.75},
         {nst, "Newfoundland Standard Time", -03.50},
         {nt, "Newfoundland Time", -03.50},
         {nzdt, "New Zealand Daylight Time", +13},
         {nzst, "New Zealand Standard Time", +12},
         {omst, "Omsk Time", +06},
         {pdt, "Pacific Daylight Time (North America)", -07},
         {pett, "Kamchatka Time", +12},
         {phot, "Phoenix Island Time", +13},
         {pkt, "Pakistan Standard Time", +05},
         {pst, "Pacific Standard Time (North America)", -08},
         {pst, "Philippine Standard Time", +08},
         {ret, "Réunion Time", +04},
         {samt, "Samara Time", +04},
         {sast, "South African Standard Time", +02},
         {sbt, "Solomon Islands Time", +11},
         {sct, "Seychelles Time", +04},
         {sgt, "Singapore Time", +08},
         {slt, "Sri Lanka Time", +05.50},
         {sst, "Samoa Standard Time", -11},
         {sst, "Singapore Standard Time", +08},
         {taht, "Tahiti Time", -10},
         {tha, "Thailand Standard Time", +07},
         {utc, "Coordinated Universal Time", 0},
         {uyst, "Uruguay Summer Time", -02},
         {uyt, "Uruguay Standard Time", -03},
         {vet, "Venezuelan Standard Time", -04.50},
         {vlat, "Vladivostok Time", +10},
         {wat, "West Africa Time", +01},
         {wedt, "Western European Daylight Time", +01},
         {west, "Western European Summer Time", +01},
         {wet, "Western European Time", 0},
         {wst, "Western Standard Time", +08},
         {yakt, "Yakutsk Time", +09},
         {yekt, "Yekaterinburg Time", +05}]).

tz_offset(missing) -> missing;
tz_offset(Tz) ->
    case lists:keysearch(Tz, 1, ?TZ_OFFSET) of
        {value, {Tz, _String, Offset}} -> -Offset * ?SECONDS_PER_HOUR;
        false -> 0
    end.

tokenize(String) ->
    tokenize(String, []).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% TOKENIZE
tokenize([], Acc) -> lists:reverse(Acc);

tokenize([Digit | Rest], Acc) when ?is_num(Digit) ->
    {N, Rest1} = tokenize_num(Rest, [Digit]),
    tokenize(Rest1, [N | Acc]);

tokenize("january" ++ Rest, Acc)   -> tokenize(Rest, [jan | Acc]);
tokenize("jan" ++ Rest, Acc)       -> tokenize(Rest, [jan | Acc]);
tokenize("febuary" ++ Rest, Acc)   -> tokenize(Rest, [feb | Acc]);
tokenize("feb" ++ Rest, Acc)       -> tokenize(Rest, [feb | Acc]);
tokenize("march" ++ Rest, Acc)     -> tokenize(Rest, [mar | Acc]);
tokenize("mar" ++ Rest, Acc)       -> tokenize(Rest, [mar | Acc]);
tokenize("april" ++ Rest, Acc)     -> tokenize(Rest, [apr | Acc]);
tokenize("apr" ++ Rest, Acc)       -> tokenize(Rest, [apr | Acc]);
tokenize("may" ++ Rest, Acc)       -> tokenize(Rest, [may | Acc]);
tokenize("june" ++ Rest, Acc)      -> tokenize(Rest, [jun | Acc]);
tokenize("jun" ++ Rest, Acc)       -> tokenize(Rest, [jun | Acc]);
tokenize("july" ++ Rest, Acc)      -> tokenize(Rest, [jul | Acc]);
tokenize("jul" ++ Rest, Acc)       -> tokenize(Rest, [jul | Acc]);
tokenize("august" ++ Rest, Acc)    -> tokenize(Rest, [aug | Acc]);
tokenize("aug" ++ Rest, Acc)       -> tokenize(Rest, [aug | Acc]);
tokenize("september" ++ Rest, Acc) -> tokenize(Rest, [sep | Acc]);
tokenize("sept" ++ Rest, Acc)      -> tokenize(Rest, [sep | Acc]);
tokenize("sep" ++ Rest, Acc)       -> tokenize(Rest, [sep | Acc]);
tokenize("october" ++ Rest, Acc)   -> tokenize(Rest, [oct | Acc]);
tokenize("oct" ++ Rest, Acc)       -> tokenize(Rest, [oct | Acc]);
tokenize("november" ++ Rest, Acc)  -> tokenize(Rest, [nov | Acc]);
tokenize("novem" ++ Rest, Acc)     -> tokenize(Rest, [nov | Acc]);
tokenize("nov" ++ Rest, Acc)       -> tokenize(Rest, [nov | Acc]);
tokenize("december" ++ Rest, Acc)  -> tokenize(Rest, [dec | Acc]);
tokenize("decem" ++ Rest, Acc)     -> tokenize(Rest, [dec | Acc]);
tokenize("dec" ++ Rest, Acc)       -> tokenize(Rest, [dec | Acc]);

%% http://en.wikipedia.org/wiki/List_of_time_zone_abbreviations
tokenize("acdt" ++ Rest, Acc) -> tokenize(Rest, [acdt | Acc]);
tokenize("acst" ++ Rest, Acc) -> tokenize(Rest, [acst | Acc]);
tokenize("act" ++ Rest, Acc) -> tokenize(Rest, [act | Acc]);
tokenize("adt" ++ Rest, Acc) -> tokenize(Rest, [adt | Acc]);
tokenize("aedt" ++ Rest, Acc) -> tokenize(Rest, [aedt | Acc]);
tokenize("aest" ++ Rest, Acc) -> tokenize(Rest, [aest | Acc]);
tokenize("aft" ++ Rest, Acc) -> tokenize(Rest, [aft | Acc]);
tokenize("akdt" ++ Rest, Acc) -> tokenize(Rest, [akdt | Acc]);
tokenize("akst" ++ Rest, Acc) -> tokenize(Rest, [akst | Acc]);
tokenize("amst" ++ Rest, Acc) -> tokenize(Rest, [amst | Acc]);
tokenize("amt" ++ Rest, Acc) -> tokenize(Rest, [amt | Acc]);
tokenize("art" ++ Rest, Acc) -> tokenize(Rest, [art | Acc]);
tokenize("ast" ++ Rest, Acc) -> tokenize(Rest, [ast | Acc]);
%% tokenize("ast" ++ Rest, Acc) -> tokenize(Rest, [ast | Acc]);
%% tokenize("ast" ++ Rest, Acc) -> tokenize(Rest, [ast | Acc]);
%% tokenize("ast" ++ Rest, Acc) -> tokenize(Rest, [ast | Acc]);
tokenize("awdt" ++ Rest, Acc) -> tokenize(Rest, [awdt | Acc]);
tokenize("awst" ++ Rest, Acc) -> tokenize(Rest, [awst | Acc]);
tokenize("azost" ++ Rest, Acc) -> tokenize(Rest, [azost | Acc]);
tokenize("azt" ++ Rest, Acc) -> tokenize(Rest, [azt | Acc]);
tokenize("bdt" ++ Rest, Acc) -> tokenize(Rest, [bdt | Acc]);
tokenize("biot" ++ Rest, Acc) -> tokenize(Rest, [biot | Acc]);
tokenize("bit" ++ Rest, Acc) -> tokenize(Rest, [bit | Acc]);
tokenize("bot" ++ Rest, Acc) -> tokenize(Rest, [bot | Acc]);
tokenize("brt" ++ Rest, Acc) -> tokenize(Rest, [brt | Acc]);
tokenize("bst" ++ Rest, Acc) -> tokenize(Rest, [bst | Acc]);
%% tokenize("bst" ++ Rest, Acc) -> tokenize(Rest, [bst | Acc]);
tokenize("btt" ++ Rest, Acc) -> tokenize(Rest, [btt | Acc]);
tokenize("cat" ++ Rest, Acc) -> tokenize(Rest, [cat | Acc]);
tokenize("cct" ++ Rest, Acc) -> tokenize(Rest, [cct | Acc]);
tokenize("cdt" ++ Rest, Acc) -> tokenize(Rest, [cdt | Acc]);
tokenize("cedt" ++ Rest, Acc) -> tokenize(Rest, [cedt | Acc]);
tokenize("cest" ++ Rest, Acc) -> tokenize(Rest, [cest | Acc]);
tokenize("cet" ++ Rest, Acc) -> tokenize(Rest, [cet | Acc]);
tokenize("chadt" ++ Rest, Acc) -> tokenize(Rest, [chadt | Acc]);
tokenize("chast" ++ Rest, Acc) -> tokenize(Rest, [chast | Acc]);
tokenize("cist" ++ Rest, Acc) -> tokenize(Rest, [cist | Acc]);
tokenize("ckt" ++ Rest, Acc) -> tokenize(Rest, [ckt | Acc]);
tokenize("clst" ++ Rest, Acc) -> tokenize(Rest, [clst | Acc]);
tokenize("clt" ++ Rest, Acc) -> tokenize(Rest, [clt | Acc]);
tokenize("cost" ++ Rest, Acc) -> tokenize(Rest, [cost | Acc]);
tokenize("cot" ++ Rest, Acc) -> tokenize(Rest, [cot | Acc]);
tokenize("cst" ++ Rest, Acc) -> tokenize(Rest, [cst | Acc]);
%% tokenize("cst" ++ Rest, Acc) -> tokenize(Rest, [cst | Acc]);
%% tokenize("cst" ++ Rest, Acc) -> tokenize(Rest, [cst | Acc]);
tokenize("ct" ++ Rest, Acc) -> tokenize(Rest, [ct | Acc]);
tokenize("cvt" ++ Rest, Acc) -> tokenize(Rest, [cvt | Acc]);
tokenize("cxt" ++ Rest, Acc) -> tokenize(Rest, [cxt | Acc]);
tokenize("chst" ++ Rest, Acc) -> tokenize(Rest, [chst | Acc]);
tokenize("dft" ++ Rest, Acc) -> tokenize(Rest, [dft | Acc]);
tokenize("east" ++ Rest, Acc) -> tokenize(Rest, [east | Acc]);
tokenize("eat" ++ Rest, Acc) -> tokenize(Rest, [eat | Acc]);
tokenize("ect" ++ Rest, Acc) -> tokenize(Rest, [ect | Acc]);
%% tokenize("ect" ++ Rest, Acc) -> tokenize(Rest, [ect | Acc]);
tokenize("edt" ++ Rest, Acc) -> tokenize(Rest, [edt | Acc]);
tokenize("eedt" ++ Rest, Acc) -> tokenize(Rest, [eedt | Acc]);
tokenize("eest" ++ Rest, Acc) -> tokenize(Rest, [eest | Acc]);
tokenize("eet" ++ Rest, Acc) -> tokenize(Rest, [eet | Acc]);
tokenize("est" ++ Rest, Acc) -> tokenize(Rest, [est | Acc]);
tokenize("fjt" ++ Rest, Acc) -> tokenize(Rest, [fjt | Acc]);
tokenize("fkst" ++ Rest, Acc) -> tokenize(Rest, [fkst | Acc]);
tokenize("fkt" ++ Rest, Acc) -> tokenize(Rest, [fkt | Acc]);
tokenize("galt" ++ Rest, Acc) -> tokenize(Rest, [galt | Acc]);
tokenize("get" ++ Rest, Acc) -> tokenize(Rest, [get | Acc]);
tokenize("gft" ++ Rest, Acc) -> tokenize(Rest, [gft | Acc]);
tokenize("gilt" ++ Rest, Acc) -> tokenize(Rest, [gilt | Acc]);
tokenize("git" ++ Rest, Acc) -> tokenize(Rest, [git | Acc]);
tokenize("gmt" ++ Rest, Acc) -> tokenize(Rest, [gmt | Acc]);
tokenize("gst" ++ Rest, Acc) -> tokenize(Rest, [gst | Acc]);
%% tokenize("gst" ++ Rest, Acc) -> tokenize(Rest, [gst | Acc]);
tokenize("gyt" ++ Rest, Acc) -> tokenize(Rest, [gyt | Acc]);
tokenize("hadt" ++ Rest, Acc) -> tokenize(Rest, [hadt | Acc]);
tokenize("haec" ++ Rest, Acc) -> tokenize(Rest, [haec | Acc]);
tokenize("hast" ++ Rest, Acc) -> tokenize(Rest, [hast | Acc]);
tokenize("hkt" ++ Rest, Acc) -> tokenize(Rest, [hkt | Acc]);
tokenize("hmt" ++ Rest, Acc) -> tokenize(Rest, [hmt | Acc]);
tokenize("hst" ++ Rest, Acc) -> tokenize(Rest, [hst | Acc]);
tokenize("ict" ++ Rest, Acc) -> tokenize(Rest, [ict | Acc]);
tokenize("idt" ++ Rest, Acc) -> tokenize(Rest, [idt | Acc]);
tokenize("irkt" ++ Rest, Acc) -> tokenize(Rest, [irkt | Acc]);
tokenize("irst" ++ Rest, Acc) -> tokenize(Rest, [irst | Acc]);
tokenize("ist" ++ Rest, Acc) -> tokenize(Rest, [ist | Acc]);
%% tokenize("ist" ++ Rest, Acc) -> tokenize(Rest, [ist | Acc]);
%% tokenize("ist" ++ Rest, Acc) -> tokenize(Rest, [ist | Acc]);
tokenize("jst" ++ Rest, Acc) -> tokenize(Rest, [jst | Acc]);
tokenize("krat" ++ Rest, Acc) -> tokenize(Rest, [krat | Acc]);
tokenize("kst" ++ Rest, Acc) -> tokenize(Rest, [kst | Acc]);
tokenize("lhst" ++ Rest, Acc) -> tokenize(Rest, [lhst | Acc]);
tokenize("lint" ++ Rest, Acc) -> tokenize(Rest, [lint | Acc]);
tokenize("magt" ++ Rest, Acc) -> tokenize(Rest, [magt | Acc]);
tokenize("mdt" ++ Rest, Acc) -> tokenize(Rest, [mdt | Acc]);
tokenize("met" ++ Rest, Acc) -> tokenize(Rest, [met | Acc]);
tokenize("mest" ++ Rest, Acc) -> tokenize(Rest, [mest | Acc]);
tokenize("mit" ++ Rest, Acc) -> tokenize(Rest, [mit | Acc]);
tokenize("msd" ++ Rest, Acc) -> tokenize(Rest, [msd | Acc]);
tokenize("msk" ++ Rest, Acc) -> tokenize(Rest, [msk | Acc]);
tokenize("mst" ++ Rest, Acc) -> tokenize(Rest, [mst | Acc]);
%% tokenize("mst" ++ Rest, Acc) -> tokenize(Rest, [mst | Acc]);
%% tokenize("mst" ++ Rest, Acc) -> tokenize(Rest, [mst | Acc]);
tokenize("mut" ++ Rest, Acc) -> tokenize(Rest, [mut | Acc]);
tokenize("myt" ++ Rest, Acc) -> tokenize(Rest, [myt | Acc]);
tokenize("ndt" ++ Rest, Acc) -> tokenize(Rest, [ndt | Acc]);
tokenize("nft" ++ Rest, Acc) -> tokenize(Rest, [nft | Acc]);
tokenize("npt" ++ Rest, Acc) -> tokenize(Rest, [npt | Acc]);
tokenize("nst" ++ Rest, Acc) -> tokenize(Rest, [nst | Acc]);
tokenize("nt" ++ Rest, Acc) -> tokenize(Rest, [nt | Acc]);
tokenize("nzdt" ++ Rest, Acc) -> tokenize(Rest, [nzdt | Acc]);
tokenize("nzst" ++ Rest, Acc) -> tokenize(Rest, [nzst | Acc]);
tokenize("omst" ++ Rest, Acc) -> tokenize(Rest, [omst | Acc]);
tokenize("pdt" ++ Rest, Acc) -> tokenize(Rest, [pdt | Acc]);
tokenize("pett" ++ Rest, Acc) -> tokenize(Rest, [pett | Acc]);
tokenize("phot" ++ Rest, Acc) -> tokenize(Rest, [phot | Acc]);
tokenize("pkt" ++ Rest, Acc) -> tokenize(Rest, [pkt | Acc]);
tokenize("pst" ++ Rest, Acc) -> tokenize(Rest, [pst | Acc]);
%% tokenize("pst" ++ Rest, Acc) -> tokenize(Rest, [pst | Acc]);
tokenize("ret" ++ Rest, Acc) -> tokenize(Rest, [ret | Acc]);
tokenize("samt" ++ Rest, Acc) -> tokenize(Rest, [samt | Acc]);
tokenize("sast" ++ Rest, Acc) -> tokenize(Rest, [sast | Acc]);
tokenize("sbt" ++ Rest, Acc) -> tokenize(Rest, [sbt | Acc]);
tokenize("sct" ++ Rest, Acc) -> tokenize(Rest, [sct | Acc]);
tokenize("sgt" ++ Rest, Acc) -> tokenize(Rest, [sgt | Acc]);
tokenize("slt" ++ Rest, Acc) -> tokenize(Rest, [slt | Acc]);
tokenize("sst" ++ Rest, Acc) -> tokenize(Rest, [sst | Acc]);
%% tokenize("sst" ++ Rest, Acc) -> tokenize(Rest, [sst | Acc]);
tokenize("taht" ++ Rest, Acc) -> tokenize(Rest, [taht | Acc]);
tokenize("tha" ++ Rest, Acc) -> tokenize(Rest, [tha | Acc]);
tokenize("utc" ++ Rest, Acc) -> tokenize(Rest, [utc | Acc]);
tokenize("ut" ++ Rest, Acc) -> tokenize(Rest, [utc | Acc]);
tokenize("uyst" ++ Rest, Acc) -> tokenize(Rest, [uyst | Acc]);
tokenize("uyt" ++ Rest, Acc) -> tokenize(Rest, [uyt | Acc]);
tokenize("vet" ++ Rest, Acc) -> tokenize(Rest, [vet | Acc]);
tokenize("vlat" ++ Rest, Acc) -> tokenize(Rest, [vlat | Acc]);
tokenize("wat" ++ Rest, Acc) -> tokenize(Rest, [wat | Acc]);
tokenize("wedt" ++ Rest, Acc) -> tokenize(Rest, [wedt | Acc]);
tokenize("west" ++ Rest, Acc) -> tokenize(Rest, [west | Acc]);
tokenize("wet" ++ Rest, Acc) -> tokenize(Rest, [wet | Acc]);
tokenize("wst" ++ Rest, Acc) -> tokenize(Rest, [wst | Acc]);
tokenize("yakt" ++ Rest, Acc) -> tokenize(Rest, [yakt | Acc]);
tokenize("yekt" ++ Rest, Acc) -> tokenize(Rest, [yekt | Acc]);

tokenize("monday" ++ Rest, Acc)    -> tokenize(Rest, Acc);
tokenize("mon" ++ Rest, Acc)       -> tokenize(Rest, Acc);
tokenize("tuesday" ++ Rest, Acc)   -> tokenize(Rest, Acc);
tokenize("tues" ++ Rest, Acc)      -> tokenize(Rest, Acc);
tokenize("tue" ++ Rest, Acc)       -> tokenize(Rest, Acc);
tokenize("wednesday" ++ Rest, Acc) -> tokenize(Rest, Acc);
tokenize("weds" ++ Rest, Acc)      -> tokenize(Rest, Acc);
tokenize("wed" ++ Rest, Acc)       -> tokenize(Rest, Acc);
tokenize("thursday" ++ Rest, Acc)  -> tokenize(Rest, Acc);
tokenize("thurs" ++ Rest, Acc)     -> tokenize(Rest, Acc);
tokenize("thur" ++ Rest, Acc)      -> tokenize(Rest, Acc);
tokenize("thu" ++ Rest, Acc)       -> tokenize(Rest, Acc);
tokenize("friday" ++ Rest, Acc)    -> tokenize(Rest, Acc);
tokenize("fri" ++ Rest, Acc)       -> tokenize(Rest, Acc);
tokenize("saturday" ++ Rest, Acc)  -> tokenize(Rest, Acc);
tokenize("sat" ++ Rest, Acc)       -> tokenize(Rest, Acc);
tokenize("sunday" ++ Rest, Acc)    -> tokenize(Rest, Acc);
tokenize("sun" ++ Rest, Acc)       -> tokenize(Rest, Acc);

tokenize("th" ++ Rest, Acc)  -> tokenize(Rest, Acc);
tokenize("nd" ++ Rest, Acc)  -> tokenize(Rest, Acc);
tokenize("st" ++ Rest, Acc)  -> tokenize(Rest, Acc);
tokenize("of" ++ Rest, Acc)  -> tokenize(Rest, Acc);
tokenize("midnight" ++ Rest, Acc)  -> tokenize(Rest, [am | Acc]);
tokenize("noon" ++ Rest, Acc)  -> tokenize(Rest, [pm | Acc]);
tokenize("a.m." ++ Rest, Acc)  -> tokenize(Rest, [am | Acc]);
tokenize("p.m." ++ Rest, Acc)  -> tokenize(Rest, [pm | Acc]);
tokenize("am" ++ Rest, Acc)  -> tokenize(Rest, [am | Acc]);
tokenize("pm" ++ Rest, Acc)  -> tokenize(Rest, [pm | Acc]);

tokenize("a" ++ Rest, Acc) -> tokenize(Rest, [a | Acc]);
tokenize("b" ++ Rest, Acc) -> tokenize(Rest, [b | Acc]);
tokenize("c" ++ Rest, Acc) -> tokenize(Rest, [c | Acc]);
tokenize("d" ++ Rest, Acc) -> tokenize(Rest, [d | Acc]);
tokenize("e" ++ Rest, Acc) -> tokenize(Rest, [e | Acc]);
tokenize("f" ++ Rest, Acc) -> tokenize(Rest, [f | Acc]);
tokenize("g" ++ Rest, Acc) -> tokenize(Rest, [g | Acc]);
tokenize("h" ++ Rest, Acc) -> tokenize(Rest, [h | Acc]);
tokenize("i" ++ Rest, Acc) -> tokenize(Rest, [i | Acc]);
tokenize("k" ++ Rest, Acc) -> tokenize(Rest, [k | Acc]);
tokenize("l" ++ Rest, Acc) -> tokenize(Rest, [l | Acc]);
tokenize("m" ++ Rest, Acc) -> tokenize(Rest, [m | Acc]);
tokenize("n" ++ Rest, Acc) -> tokenize(Rest, [n | Acc]);
tokenize("o" ++ Rest, Acc) -> tokenize(Rest, [o | Acc]);
tokenize("p" ++ Rest, Acc) -> tokenize(Rest, [p | Acc]);
tokenize("q" ++ Rest, Acc) -> tokenize(Rest, [q | Acc]);
tokenize("r" ++ Rest, Acc) -> tokenize(Rest, [r | Acc]);
tokenize("s" ++ Rest, Acc) -> tokenize(Rest, [s | Acc]);
tokenize("t" ++ Rest, Acc) -> tokenize(Rest, [t | Acc]);
tokenize("u" ++ Rest, Acc) -> tokenize(Rest, [u | Acc]);
tokenize("v" ++ Rest, Acc) -> tokenize(Rest, [v | Acc]);
tokenize("w" ++ Rest, Acc) -> tokenize(Rest, [w | Acc]);
tokenize("x" ++ Rest, Acc) -> tokenize(Rest, [x | Acc]);
tokenize("y" ++ Rest, Acc) -> tokenize(Rest, [y | Acc]);
tokenize("z" ++ Rest, Acc) -> tokenize(Rest, [z | Acc]);

tokenize([$, | Rest], Acc) -> tokenize(Rest, Acc);
tokenize([$. | Rest], Acc) -> tokenize(Rest, Acc);
tokenize([$  | Rest], Acc) -> tokenize(Rest, Acc);
tokenize([$: | Rest], Acc) -> tokenize(Rest, [':' | Acc]);
tokenize([$/ | Rest], Acc) -> tokenize(Rest, ['/' | Acc]);
tokenize([$- | Rest], Acc) -> tokenize(Rest, ['-' | Acc]);
tokenize([$+ | Rest], Acc) -> tokenize(Rest, ['+' | Acc]);

tokenize([Else | Rest], Acc) -> tokenize(Rest, [{bad_token, Else} | Acc]).

%% End of TOKENIZE
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

tokenize_num([N | Rest], Acc) when ?is_num(N) ->
    tokenize_num(Rest, [N | Acc]);
tokenize_num(String, Acc) ->
    {list_to_integer(lists:reverse(Acc)), String}.


test() ->
    lists:all(
      fun test/1,
      [{"1976-07-04T00:01:02Z", "1976-07-04T00:01:02Z"},
       {"1976-07-04T00:01:02", "1976-07-04T00:01:02Z"},
       {"July 4, 1976 12:01:02 am", "1976-07-04T00:01:02Z"},
       {"  July 4, 1976 12:01:02 am  ", "1976-07-04T00:01:02Z"},
       {"2011-11-30T16:22:33-0400", "2011-11-30T20:22:33Z"},
       %%{"1978", "1978-01-01T00:00:00Z"},
       {"1316462543", "2011-09-19T20:02:23Z"},
       {"01-May-2010 00:57:12 BST", "2010-04-30T18:57:12Z"},
       {"05/01 2010 00:57:12 BST", "2010-04-30T18:57:12Z"},
       {"Mon Sep 19 16:06:10 EDT 2011", "2011-09-19T20:06:10Z"}]).

test({String, Expected}) ->
    Result =
        case datetime(String, gmt, ts_mdy) of
            {ok, Normalized} -> Normalized;
            {error, _} -> String
        end,
    TestResult = Expected =:= Result,
    io:format("~5w ~s -> ~s~n", [TestResult, String, Result]),
    TestResult.

test_goread() ->
    Results = [{X, datetime(X, gmt, ts_mdy)}
               || X <- goread_examples()],
    %% Returns {ProbablyBad, ProbablyGood}, so at some point
    %% ProbablyBad should be empty.
    lists:partition(fun({_, {ok, {{2006,1,2}, _}}}) -> false;
                       (_) -> true
                    end, Results).

%% blatantly stolen from @source:
%% https://github.com/mjibson/goread/blob/0387db10bd9/goapp/utils.go#L129-L196
%% which is released under an ISC license (BSD equivalent).
goread_examples() ->
    ["01-02-2006",
     "01.02.06",
     "01/02/2006",
     "02 Jan 2006 15:04:05 -0700",
     "02 Jan 2006 15:04:05 MST",
     "02 Jan 2006 15:04:05 UT",
     "02 Jan 2006",
     "1/2/2006 3:04:05 PM",
     "2 Jan 2006 15:04:05 MST",
     "2 January 2006",
     "2006-01-02 15:04",
     "2006-01-02 15:04:05 -0700",
     "2006-01-02 15:04:05 MST",
     "2006-01-02",
     "2006-01-02T15:04-07:00",
     "2006-01-02T15:04:05 -0700",
     "2006-01-02T15:04:05",
     "2006-01-02T15:04:05-0700",
     "2006-01-02T15:04:05-07:00",
     "2006-01-02T15:04:05-07:00:00",
     "2006-01-02T15:04:05Z",
     "2006-1-2 15:04:05",
     "2006-1-2",
     "Jan 2, 2006 15:04:05 MST",
     "Jan 2, 2006 3:04:05 PM MST",
     "January 02, 2006 15:04:05 MST",
     "January 2, 2006 15:04:05 MST",
     "Mon, 02 Jan 06 15:04:05 MST",
     "Mon, 02 Jan 2006 15:04 MST",
     "Mon, 02 Jan 2006 15:04:05 -0700",
     "Mon, 02 Jan 2006 15:04:05 -07:00",
     "Mon, 02 Jan 2006 15:04:05 MST",
     "Mon, 02 Jan 2006 15:04:05 UT",
     "Mon, 02 Jan 2006 15:04:05 Z",
     "Mon, 02 Jan 2006 15:04:05",
     "Mon, 02 Jan 2006 15:04:05MST",
     "Mon, 02 Jan 2006 3:04:05 PM MST",
     "Mon, 02 Jan 2006",
     "Mon, 02 January 2006",
     "Mon, 2 Jan 06 15:04:05 -0700",
     "Mon, 2 Jan 2006 15:04:05 -0700",
     "Mon, 2 Jan 2006 15:04:05 MST",
     "Mon, 2 Jan 2006 15:04:05 UT",
     "Mon, 2 Jan 2006",
     "Mon, 2 Jan 2006, 15:04 -0700",
     "Mon, 2 January 2006 15:04:05 -0700",
     "Mon, 2 January 2006 15:04:05 MST",
     "Mon, 2 January 2006, 15:04 -0700",
     "Mon, 2 January 2006, 15:04:05 MST",
     "Mon, Jan 2 2006 15:04:05 -0700",
     "Mon, Jan 2 2006 15:04:05 -700",
     "Mon, January 2 2006 15:04:05 -0700",
     "Monday, 02 January 2006 15:04:05 -0700",
     "Monday, 02 January 2006 15:04:05 MST",
     "Monday, 2 Jan 2006 15:04:05 -0700",
     "Monday, 2 Jan 2006 15:04:05 MST",
     "Monday, 2 January 2006 15:04:05 -0700",
     "Monday, January 02, 2006"
     %% time.ANSIC,
     %% time.RFC1123,
     %% time.RFC1123Z,
     %% time.RFC3339,
     %% time.RFC822,
     %% time.RFC822Z,
     %% time.RFC850,
     %% time.RubyDate,
     %% time.UnixDate
    ].
