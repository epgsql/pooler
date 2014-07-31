# This Makefile written by concrete
#
# {concrete_makefile_version, 2}
#
# ANY CHANGES TO THIS FILE WILL BE OVERWRITTEN on `concrete update`
# IF YOU WANT TO CHANGE ANY OF THESE LINES BELOW, COPY THEM INTO
# custom.mk FIRST

# Use this to override concrete's default dialyzer options of
# -Wunderspecs
# DIALYZER_OPTS = ...

# List dependencies that you do NOT want to be included in the
# dialyzer PLT for the project here.  Typically, you would list a
# dependency here if it isn't spec'd well and doesn't play nice with
# dialyzer or otherwise mucks things up.
#
# DIALYZER_SKIP_DEPS = bad_dep_1 \
#                      bad_dep_2

# If you want to add dependencies to the default "all" target provided
# by concrete, add them here (along with make rules to build them if needed)
# ALL_HOOK = ...

# custom.mk is totally optional
custom_rules_file = $(wildcard custom.mk)
ifeq ($(custom_rules_file),custom.mk)
    include custom.mk
endif

concrete_rules_file = $(wildcard concrete.mk)
ifeq ($(concrete_rules_file),concrete.mk)
    include concrete.mk
else
    all:
	@echo "ERROR: missing concrete.mk"
	@echo "  run: concrete update"
endif
