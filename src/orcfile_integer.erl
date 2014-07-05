%%%-------------------------------------------------------------------
%%% @author UENISHI Kota <kota@basho.com>
%%% @copyright (C) 2014, UENISHI Kota
%%% @doc
%%%
%%% @end
%%% Created :  4 Jul 2014 by UENISHI Kota <kota@basho.com>
%%%-------------------------------------------------------------------
-module(orcfile_integer).

-export([to_iolist/3]).

-include_lib("eunit/include/eunit.hrl").

to_iolist(Q, Total, _) ->
    Acc = to_iolist(Q, Total, [], {empty, 0}),
    lists:reverse(Acc).

to_iolist(Q, Total, Acc, State) ->
    case queue:out(Q) of
        {{value, {Seq, I}}, Q1} ->
            handle_value(Q1, Total, Seq, I, Acc, State);
        {empty, Q} ->
            handle_empty(Total, Acc, State)
    end.

handle_empty(Total, Acc, {empty, N}) ->
    Bin = <<"emp", (Total-N)/integer>>,
    [Bin|Acc];
handle_empty(Total, Acc, {elem, {PrevSeq, PrevValue}, C}) ->
    Bin0 = [<<"elm", C/integer>>, term_to_binary(PrevValue)],
    Bin1 = <<"emp", (Total-PrevSeq)/integer>>,
    [Bin1|[Bin0|[Acc]]].

handle_value(Q, Total, 0, Value, Acc, {empty, 0}) ->
    State={elem, {0, Value}, 1},
    to_iolist(Q, Total, Acc, State);
handle_value(Q, Total, Seq, Value, Acc, {empty, PrevSeq}) ->
    Bin = <<"emp", (Seq-PrevSeq)/integer>>,
    State={elem, {Seq, Value}, 1},
    to_iolist(Q, Total, [Bin|Acc], State);

handle_value(Q, Total, Seq, Value, Acc, {elem, {PrevSeq, PrevValue}, C}) ->
    %% ?debugVal(Seq),
    case {PrevSeq+1, (Value =:= PrevValue)} of
        {Seq, true} ->
            %% cont
            State = {elem, {Seq, Value}, C+1},
            to_iolist(Q, Total, Acc, State);
        {Seq, false} ->
            %% not cont
            Bin = [<<"elm">>, <<C/integer>>, term_to_binary(PrevValue)],
            State = {elem, {Seq, Value}, 1},
            to_iolist(Q, Total, [Bin|Acc], State);

        {_, _} ->
            %% empty lines exists
            Bin0 = [<<"elm">>, <<C/integer>>, term_to_binary(PrevValue)],
            Bin1 = <<"emp", (PrevSeq-Seq)/integer>>,
            State = {elem, {Seq, Value}, 1},
            to_iolist(Q, Total, [Bin1|[Bin0|Acc]], State)
    end.
