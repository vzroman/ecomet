REBAR3 := $(shell which rebar3)

compile:
	$(REBAR3) compile

ct: compile
	$(REBAR3) ct --spec=./test/module/test.spec

clean_build:
	@rm -rf _build

clean_db:
	@rm -rf DB

clean_logs:
	@rm -rf logs

clean_all:clean_db clean_logs clean_build

shell:
	ERL_FLAGS="-args_file config/vm.args -config config/sys.config" ./rebar3 shell
