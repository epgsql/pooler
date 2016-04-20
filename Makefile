.PHONY: all compile run test doc clean

REBAR=$(shell which rebar3 || echo ./rebar3)

DIALYZER_APPS = asn1 compiler crypto erts inets kernel public_key sasl ssl stdlib syntax_tools tools

all: $(REBAR) compile

compile:
		$(REBAR) as dev compile

run:
		erl -pa _build/dev/lib/*/ebin -boot start_sasl -config config/demo.config -run pooler

test:
		$(REBAR) eunit skip_deps=true verbose=3

doc:
		$(REBAR) as dev edoc

clean:
		$(REBAR) as dev clean
		rm -rf ./erl_crash.dump

dialyzer:
		$(REBAR) as dev dialyzer

# Get rebar if it doesn't exist

REBAR_URL=https://s3.amazonaws.com/rebar3/rebar3

./rebar3:
		erl -noinput -noshell -s inets -s ssl  -eval '{ok, _} = httpc:request(get, {"${REBAR_URL}", []}, [], [{stream, "${REBAR}"}])' -s init stop
		chmod +x ${REBAR}

