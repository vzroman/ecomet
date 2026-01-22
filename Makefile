REBAR3 := ./rebar3

compile:
	$(REBAR3) compile

tests: compile
	$(REBAR3) ct --spec=./test/module/test.spec

clean_build:
	@rm -rf _build
	@rm -rf rebar.lock

clean_db:
	@rm -rf DB

clean_logs:
	@rm -rf logs
	@rm -rf _build/test/logs

clean_test:
	@rm -rf _build/test

clean_all:clean_db clean_logs clean_build

shell:
	ERL_FLAGS="-args_file config/vm.args -config config/sys.config" ./rebar3 shell
