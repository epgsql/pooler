ERLFLAGS= -pa $(CURDIR)/.eunit -pa $(CURDIR)/ebin -pa $(CURDIR)/deps/*/ebin

DEPS_PLT=$(CURDIR)/.deps_plt

# =============================================================================
# Verify that the programs we need to run are installed on this system
# =============================================================================
ERL = $(shell which erl)

ifeq ($(ERL),)
$(error "Erlang not available on this system")
endif

REBAR=$(shell which rebar)

# If building on travis, use the rebar in the current directory
ifeq ($(TRAVIS),true)
REBAR=$(CURDIR)/rebar
endif

ifeq ($(REBAR),)
REBAR=$(CURDIR)/rebar
endif

.PHONY: all compile test dialyzer clean distclean doc

all: compile test dialyzer

REBAR_URL=https://github.com/rebar/rebar/wiki/rebar
$(REBAR):
	curl -Lo rebar $(REBAR_URL) || wget $(REBAR_URL)
	chmod a+x rebar

get-rebar: $(REBAR)

compile: $(REBAR)
	$(REBAR) compile

eunit: compile clean-common-test-data
	$(REBAR) skip_deps=true eunit

ct: compile clean-common-test-data
	mkdir -p $(CURDIR) logs
	ct_run -pa $(CURDIR)/ebin \
	-pa $(CURDIR)/deps/*/ebin \
	-logdir $(CURDIR)/logs \
	-dir $(CURDIR)/test/ \
	-cover cover.spec

test: compile dialyzer eunit ct

$(DEPS_PLT): compile
	@echo Building local erts plt at $(DEPS_PLT)
	@echo
	$(DIALYZER) --output_plt $(DEPS_PLT) --build_plt \
	   --apps erts kernel stdlib -r deps

dialyzer: compile $(DEPS_PLT)
	@dialyzer -Wunderspecs -r ebin

doc:
	$(REBAR) doc skip_deps=true

clean-common-test-data:
# We have to do this because of the unique way we generate test
# data. Without this rebar eunit gets very confused
	- rm -rf $(CURDIR)/test/*_SUITE_data

clean: clean-common-test-data $(REBAR)
	- rm -rf $(CURDIR)/test/*.beam
	- rm -rf $(CURDIR)/logs
	- rm -rf $(CURDIR)/ebin
	$(REBAR) skip_deps=true clean

distclean: clean
	- rm -rf $(DEPS_PLT)
	$(REBAR) delete-deps

demo_shell: compile test
	@erl -pa .eunit ebin -config pooler-example -s pooler manual_start
