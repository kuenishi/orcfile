%%%-------------------------------------------------------------------
%%% @author UENISHI Kota <kuenishi@gmail.com>
%%% @copyright (C) 2014, UENISHI Kota
%%% @doc
%%%
%%% @end
%%% Created :  4 Jul 2014 by UENISHI Kota <kuenishi@gmail.com>
%%%-------------------------------------------------------------------
-module(orcfile_stripe).

-export([new/1, push/2, to_iolist/1]).

-record(stripe, {schema, columns, count}).

new(Schema) ->
    Cols = lists:map(fun({Name, Type}) ->
                             {ok, Col} = orcfile_column:new(Name, Type),
                             {Name, Col}
                     end, Schema),
    #stripe{schema=orddict:from_list(Schema),
            columns=orddict:from_list(Cols),
            count=0}.

push(#stripe{count=C} = Stripe, []) -> {ok, Stripe#stripe{count=C+1}};
push(#stripe{columns=Cols, count=C} = Stripe, [{Key, Value}|Rest]) ->
    case orddict:find(Key, Cols) of
        {ok, Col} ->
            NewCol = orcfile_column:append(Col, Value, C),
            NewCols = orddict:store(Key, NewCol, Cols),
            push(Stripe#stripe{columns=NewCols},
                 Rest);
        error ->
            %% unknown property which is not in schema;
            %% do debug log or other?
            push(Stripe, Rest)
    end.

to_iolist(#stripe{columns=Cols, count=C} = _Stripe) ->
    Binaries0 = orddict:map(fun(_, Col) ->
                                   {Col, orcfile_column:to_iolist(Col, C)}
                           end, Cols),
    Header = build_header(Binaries0, 0, []),
    Binaries = build_body(Binaries0, []),
    [Header, Binaries].

build_body([], Body) -> lists:reverse(Body);
build_body([{_Name,{_Col, Binary}}|Rest], Body0) ->    
    build_body(Rest, [Binary|Body0]).

build_header([], _, Headers) ->  lists:reverse(Headers);
build_header([{_Name,{Col, Binary}}|Rest], Offset, Headers) ->
    build_header(Rest, Offset + byte_size(iolist_to_binary(Binary)),
                 [<<(orcfile_column:name(Col))/binary, Offset/integer>>|Headers]).
