%%%-------------------------------------------------------------------
%%% @author UENISHI Kota <kuenishi@gmail.com>
%%% @copyright (C) 2014, UENISHI Kota
%%% @doc
%%%  http://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.0.0.2/ds_Hive/orcfile.html
%%%  open, scan, query, min/max/count, close
%%%  create, write, close

%%% @end
%%% Created :  4 Jul 2014 by UENISHI Kota <kuenishi@gmail.com>
%%%-------------------------------------------------------------------
-module(orcfile).

-export([open/1]).
-export([create/1, write/1]).

-include_lib("eunit/include/eunit.hrl").

-record(params, {
          %%  Key 	Default 	Notes
          %% orc.compress 	ZLIB 	high level compression (one of NONE, ZLIB, SNAPPY)
          compress = 'ZLIB' :: 'NONE' | 'ZLIB' | 'SNAPPY',
          %% orc.compress.size 	262,144 	number of bytes in each compression chunk
          compress_size = 262144 :: non_neg_integer(),
          %% orc.stripe.size 	268435456 	number of bytes in each stripe
          stripe_size = 268435456 :: non_neg_integer(),
          %% orc.row.index.stride 	10,000 	number of rows between index entries (must be >= 1000)
          row_index_stride = 10000 :: non_neg_integer(),
          %% orc.create.index 	true 	whether to create row indexes 
          create_index = true :: boolean()
         }).

-record(orcfile, {
          file :: filename:filename(),
          fd :: file:io_device(),
          params :: #params{}
         }).

open(Filename) ->
    New = #orcfile{},
    New#orcfile{file=Filename}.

create(Filename) ->
    open(Filename).

write(P) ->
    ?debugVal(P),
    io:format("foobar, ~p", [P]).

-ifdef(TEST).

generate() ->
    I = random:uniform(1024),
    J = random:uniform(65536),
    S = [ $0 + random:uniform(78) || _ <- lists:seq(1, 255) ],
    ?debugVal(S),
    [{<<"i">>, I}, {<<"j">>, J}, {<<"s">>, list_to_binary(S)}].

orcfile_test() ->
    random:seed(erlang:now()),
    ?debugVal(generate()),
    orcfile:write(generate()).

-endif.
