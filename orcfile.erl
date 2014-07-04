%%%-------------------------------------------------------------------
%%%  @author UENISHI Kota <kuenishi@gmail.com>
%%%  @copyright (C) 2014, UENISHI Kota
%%%  @doc
%%%
%%%  This is still Proof of Concept, and never tested connectivity
%%%  with real ORCFile - due to the network connectivity.
%%%
%%%  http://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.0.0.2/ds_Hive/orcfile.html
%%%  open, scan, query, min/max/count, close
%%%  create, write, close
%%%
%%% @end
%%% Created :  4 Jul 2014 by UENISHI Kota <kuenishi@gmail.com>
%%%-------------------------------------------------------------------
-module(orcfile).

-export([open/1]).
-export([create/3, push/2, write/2, close/1]).

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
          schema = [] :: proplists:proplist(),
          tblproperties :: #params{},
          stripes = [] :: list(), %% Stripes to be written down; just as a buffer
          stripe  :: term() %% Current hot stripe
         }).

open(Filename) ->
    New = #orcfile{},
    New#orcfile{file=Filename}.

create(Filename, Schema, _Options) ->
    %% exclusive open required
    File0 = #orcfile{},
    {ok, Fd} = file:open(Filename, [write, exclusive, binary]),
    File = File0#orcfile{file=Filename, fd=Fd, schema=Schema,
                         stripe=orcfile_stripe:new(Schema)},
    {ok, File}.

push(ORCFile, []) -> {ok, ORCFile};
push(ORCFile0, [H|T]) ->
    {ok, ORCFile} = push(ORCFile0, H),
    push(ORCFile, T);
push(#orcfile{stripe=Stripe0} = ORCFile,
     {Record}) ->
    case orcfile_stripe:push(Stripe0, Record) of
        {ok, Stripe} -> %% when size > 1024??
            %% Stripes0 = ORCFile#orcfile.stripes,
            %% EncodedStripe = orcfile_stripe:encode(Stripe),
            %% {ok, ORCFile#orcfile{stripes=[EncodedStripe|Stripes0],
            %%                      stripe=orcfile_stripe:new()}};
            {ok, ORCFile#orcfile{stripe=Stripe}};
        Error ->
            Error
    end.

%% does nothing
write(ORCFile, P) ->
    ?debugVal(P),
    io:format("foobar, ~p", [ORCFile]),
    ok.

flush(#orcfile{fd=Fd, stripe=Stripe} = _ORCFile) ->
    Binary = orcfile_stripe:to_iolist(Stripe),
    ?debugVal(Binary),
    ok = file:write(Fd, Binary),
    {ok, byte_size(iolist_to_binary(Binary))}.

%% actually writes and flushes to the file
close(#orcfile{fd=Fd} = ORCFile) ->
    %% TODO: Flush all data, or else
    {ok, Size} = flush(ORCFile),
    ?debugVal(ORCFile),
    ?debugVal({size, Size}),
    ok = file:close(Fd).

-ifdef(TEST).

generate() ->
    I = random:uniform(1),
    J = random:uniform(1),
    %%S = [ $0 + random:uniform(78) || _ <- lists:seq(1, 2) ],
    S = [ $0 || _ <- lists:seq(1, 2) ],
    {[{<<"i">>, I}, {<<"j">>, J}, {<<"s">>, list_to_binary(S)}]}.

schema() ->
    [{<<"i">>, integer}, {<<"j">>, integer}, {<<"s">>, string}].

orcfile_test() ->
    random:seed(erlang:now()),
    ?debugVal(generate()),
    _ = file:delete("Testfile"),
    {ok, ORCFile0} = orcfile:create("Testfile", schema(), []),
    Data = [ generate() || _ <- lists:seq(0, 255) ],
    {ok, ORCFile} = orcfile:push(ORCFile0, Data),
    ok = orcfile:close(ORCFile).

-endif.
