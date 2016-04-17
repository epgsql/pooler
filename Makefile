.PHONY: all compile run test doc clean
.PHONY: build_plt add_to_plt dialyzer

REBAR=./rebar3

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
		$(REBAR) clean
		rm -rf ./erl_crash.dump

build_plt: clean compile
ifneq ("$(wildcard erlang.plt)","")
		@echo "Erlang plt file already exists"
else
		dialyzer --build_plt --output_plt erlang.plt --apps $(DIALYZER_APPS)
endif
ifneq ("$(wildcard pooler.plt)","")
		@echo "pooler plt file already exists"
else
		dialyzer --build_plt --output_plt pooler.plt _build/default/lib/*/ebin/
endif

add_to_plt: build_plt
		dialyzer --add_to_plt --plt erlang.plt --output_plt erlang.plt.new --apps $(DIALYZER_APPS)
		dialyzer --add_to_plt --plt pooler.plt --output_plt pooler.plt.new _build/default/lib/*/ebin/
		mv erlang.plt.new erlang.plt
		mv pooler.plt.new pooler.plt

dialyzer:
		dialyzer --src src -r src --plts erlang.plt pooler.plt -Wno_return -Werror_handling -Wrace_conditions -Wunderspecs | fgrep -v -f ./dialyzer.ignore-warnings
