%%%-------------------------------------------------------------------
%%% @author UENISHI Kota <kota@basho.com>
%%% @copyright (C) 2014, UENISHI Kota
%%% @doc
%%%
%%% @end
%%% Created :  5 Jul 2014 by UENISHI Kota <kota@basho.com>
%%%-------------------------------------------------------------------
-module(orcfile_footer).

-export([to_iolist/1, postscript/1]).

-include("orc_proto_pb.hrl").

to_iolist(_) ->
    Footer = #footer{headerlength = 0,
                     contentlength = 0,
                     stripes = [#stripeinformation{}],
                     types = [#type{kind= 'BOOLEAN'}],
                     metadata = [#usermetadataitem{
                                    name = <<"hoge">>,
                                    value = <<"value">>}],
                     numberofrows = 0,
                     statistics = [#columnstatistics{}],
                     rowindexstride = 0},
    orc_proto_pb:encode(Footer).

postscript(_) ->
    PS = #postscript{footerlength = 0,
                     %% compression = 'ZLIB',
                     compressionblocksize = 0,
                     version = [1],
                     metadatalength = 1,
                     magic = "ORC"},
    orc_proto_pb:encode(PS).
