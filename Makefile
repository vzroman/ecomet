REBAR3 := $(shell which rebar3)

compile:
	$(REBAR3) compile

ct: compile
	$(REBAR3) ct --spec=./test/module/test.spec

clean:
	@rm -f _build
