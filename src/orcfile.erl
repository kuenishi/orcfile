%%%-------------------------------------------------------------------
%%%  @author UENISHI Kota <kuenishi@gmail.com>
%%%  @copyright (C) 2014, UENISHI Kota
%%%  @doc
%%%
%%%  This is still Proof of Concept, and never tested connectivity
%%%  with real ORCFile - due to the network connectivity.
%%%
%%%  https://cwiki.apache.org/confluence/display/Hive/LanguageManual+ORC
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
-include_lib("kernel/include/file.hrl").
%% -include("orc_proto_pb.hrl").

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
          mode = read :: read|write,
          schema = [] :: proplists:proplist(),
          tblproperties :: #params{},
          stripes = [] :: list(), %% Stripes to be written down; just as a buffer
          stripe  :: term() %% Current hot stripe
         }).

open(Filename) ->
    {ok, Fd} = file:open(Filename, [read,binary]),
    %% check header magic number
    %% seek from backward
    New = #orcfile{},
    %% read postscript,
    %% read footer
    %% read streams
    {ok, New#orcfile{file=Filename, fd=Fd}}.

create(Filename, Schema, _Options) ->
    %% exclusive open required
    File0 = #orcfile{},
    {ok, Fd} = file:open(Filename, [write, exclusive, binary]),
    ok = file:write(Fd, <<"ORC">>),
    File = File0#orcfile{file=Filename, fd=Fd, schema=Schema,
                         mode=write,
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
write(ORCFile, _P) ->
    %% ?debugVal(P),
    io:format("foobar, ~p", [ORCFile]),
    ok.

flush(#orcfile{fd=Fd, stripe=Stripe} = _ORCFile) ->
    %% Stripes:
    _Binary = orcfile_stripe:to_iolist(Stripe),
    %% ?debugVal(Binary),
    %% ok = file:write(Fd, Binary),
    %% File footer:
    %%Footer = orcfile_footer:to_iolist(tmp),
    %% ?debugVal(Footer),
    %% ok = file:write(Fd, Footer),
    %% postscript:
    %%FooterLen = byte_size(iolist_to_binary(Footer)),
    %%?debugVal(FooterLen),
    Postscript = orcfile_footer:postscript(tmp,8),
                                           
    ok = file:write(Fd, Postscript),
    {ok, byte_size(iolist_to_binary(Postscript))}.

%% actually writes and flushes to the file
close(#orcfile{mode=Mode, fd=Fd} = ORCFile) ->
    %% TODO: Flush all data, or else
    case Mode of
        write ->
            {ok, Size} = flush(ORCFile),
            ?debugVal(ORCFile),
            ?debugVal({size, Size});
        read ->
            ok
    end,
    ok = file:close(Fd).

-ifdef(TEST).

generate() ->
    I = random:uniform(1),
    J = random:uniform(1),
    %%S = [ $0 + random:uniform(78) || _ <- lists:seq(1, 2) ],
    S = [ $0 || _ <- lists:seq(1, 4) ],
    K = case random:uniform(10) of
            1 -> [{<<"k">>, random:uniform(100)}];
            _ -> []
        end,
    L = case random:uniform(100) of
            1 -> [{<<"l">>, list_to_binary([ $0 + random:uniform(78) || _ <- lists:seq(1, 255) ])}];
            _ -> []
        end,
    {L ++ K ++ [{<<"i">>, I}, {<<"j">>, J}, {<<"s">>, list_to_binary(S)}]}.

schema() ->
    [{<<"i">>, integer}, {<<"j">>, integer}, {<<"s">>, string},
     {<<"k">>, integer}, {<<"l">>, string}].

orcfile_write_test() ->
    random:seed(erlang:now()),
    %% ?debugVal(generate()),
    _ = file:delete("Testfile"),
    {ok, ORCFile0} = orcfile:create("Testfile", schema(), []),
    Data = [ generate() || _ <- lists:seq(0, 1024) ],
    {ok, ORCFile} = orcfile:push(ORCFile0, Data),
    ok = orcfile:close(ORCFile),
    BERTSize = byte_size(term_to_binary(Data)),
    ?debugVal({bertsize, BERTSize}).

%% TODO: read from "Testfile" and compare the query result, queries against Data itself


orcfile_write2_test() ->
    file:delete("111111"),
    {ok, ORCFile0} = orcfile:create("111111", [{<<"testname">>,string}, {<<"testnumber">>, integer}], []),
    Data = [ {[{<<"testname">>, <<"foobar">>}, {<<"testnumber">>, 12}]},
             {[{<<"testname">>, <<"aasdf">>}, {<<"testnumber">>, 235}]}],
    {ok, ORCFile} = orcfile:push(ORCFile0, Data),
    ok = orcfile:close(ORCFile),
    BERTSize = byte_size(term_to_binary(Data)),
    ?debugVal({bertsize, BERTSize}).
    

orcfile_read_test() ->
    Filename = "../test/000000_0",
    {ok, FileInfo} = file:read_file_info(Filename),
    Size = FileInfo#file_info.size,

    {ok, Fd} = file:open(Filename, [read,binary]),
    {ok, [<<"ORC">>, Bin]} = file:pread(Fd, [{0,3}, {3, Size-3}]),
    ?debugVal(Bin),

    try
        PBObj = orc_proto_pb:decode_rowindex(Bin),
        ?debugVal(PBObj)
    
    %% {ok, ORCFile} = orcfile:open("../test/000000_0"),
    %% ?debugVal(ORCFile),
    %%ok = orcfile:close(ORCFile).
    catch _:E ->
            ?debugVal(E)
    after
        file:close(Fd)
    end.

-endif.
