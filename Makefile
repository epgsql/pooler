.PHONY: deps test analyze clean distclean doc

all: deps
	@./rebar compile

deps:
	@./rebar get-deps

test:
	@./rebar eunit skip_deps=true

analyze:
	@./rebar analyze skip_deps=true

doc:
	@./rebar doc skip_deps=true

clean:
	@./rebar clean

distclean: clean
	@./rebar delete-deps

