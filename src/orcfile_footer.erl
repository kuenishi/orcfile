%%%-------------------------------------------------------------------
%%% @author UENISHI Kota <kota@basho.com>
%%% @copyright (C) 2014, UENISHI Kota
%%% @doc
%%%
%%% @end
%%% Created :  5 Jul 2014 by UENISHI Kota <kota@basho.com>
%%%-------------------------------------------------------------------
-module(orcfile_footer).

-export([to_iolist/1, postscript/2]).

-include("orc_proto_pb.hrl").

to_iolist(_) ->
    Footer = #footer{headerlength = 16#dead,
                     contentlength = 16#beef,
                     stripes = [#stripeinformation{}],
                     types = [#type{kind= 'BOOLEAN'}],
                     metadata = [#usermetadataitem{
                                    name = <<"hoge">>,
                                    value = <<"value">>}],
                     numberofrows = 2,
                     statistics = [#columnstatistics{
                                      numberofvalues=2},
                                   #columnstatistics{
                                      numberofvalues=2,
                                      stringstatistics=
                                          #stringstatistics{
                                             minimum="\"aasdf\"",
                                             maximum="\"foobar\""}},
                                   #columnstatistics{
                                      numberofvalues=2,
                                      intstatistics=
                                          #integerstatistics{
                                             minimum=12,
                                             maximum=235,
                                             sum=247}}
                                  ]},
                     %%rowindexstride = 0},
    orc_proto_pb:encode(Footer).

postscript(_, FooterLength) ->
    PS = #postscript{footerlength = FooterLength,
                     %% compression = 'NONE',
                     %% compressionblocksize = 262144,
                     compressionblocksize = 16,
                     version = [0,11],
                     metadatalength = 12,
                     magic = [$O,$R,$C,16#F]},
    orc_proto_pb:encode(PS).
