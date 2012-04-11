.PHONY: deps test dialyzer clean distclean doc

all: deps
	@./rebar compile

deps:
	@./rebar get-deps

test:
	@./rebar eunit skip_deps=true

dialyzer: all
	@dialyzer -Wrace_conditions -Wunderspecs -r ebin

doc:
	@./rebar doc skip_deps=true

clean:
	@./rebar clean

distclean: clean
	@./rebar delete-deps
