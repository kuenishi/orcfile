
all: src/orc_proto.erl

src/orc_proto.proto:
	wget https://raw.githubusercontent.com/apache/hive/release-0.13.1/ql/src/protobuf/org/apache/hadoop/hive/ql/io/orc/orc_proto.proto -O src

src/orc_proto.erl: src/orc_proto.proto get-deps
	cd src && ERL_LIBS=../deps ../deps/protobuffs/bin/protoc-erl orc_proto.proto

get-deps:
	./rebar get-deps

compile: get-deps
	./rebar compile

test: compile
	./rebar eunit skip_deps=true

clean:
	./rebar clean
	rm -rf .eunit
