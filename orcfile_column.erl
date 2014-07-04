%%%-------------------------------------------------------------------
%%% @author UENISHI Kota <kota@basho.com>
%%% @copyright (C) 2014, UENISHI Kota
%%% @doc
%%%
%%% @end
%%% Created :  4 Jul 2014 by UENISHI Kota <kota@basho.com>
%%%-------------------------------------------------------------------
-module(orcfile_column).

-export([new/2, append/3, to_iolist/2, name/1]).

-record(column, {name, type, data}).

new(Name, Type) when Type =:= integer orelse Type =:= string ->
    Column = #column{name=Name, type=Type,
                     data=queue:new()},
    {ok, Column}.

append(#column{type=integer, data=Q} = Column, I, Seq) when is_integer(I) ->
    Column#column{data=queue:in({Seq,I}, Q)};
append(#column{type=string, data=Q} = Column, S, Seq) when is_binary(S) ->
    Column#column{data=queue:in({Seq,S}, Q)}.

to_iolist(#column{data=Q, type=integer}, C) ->
    orcfile_integer:to_iolist(Q, C, []);
to_iolist(#column{data=Q, type=string}, C) ->
    orcfile_integer:to_iolist(Q, C, []).
    %% orcfile_string:to_ioist(Q, C, []).

name(#column{name=Name}) ->
    Name.
