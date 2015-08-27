PROJECT = pooler

ERLC_OPTS = -Werror +debug_info +warn_export_all +warn_export_vars \
	+warn_shadow_vars +warn_obsolete_guard -Dnamespaced_types

TEST_ERLC_OPTS := +debug_info +warn_export_vars +warn_shadow_vars \
	+warn_obsolete_guard -Dnamespaced_types -DTEST=1 -DEXTRA=1

DOC_DEPS = edown

dep_edown = git git://github.com/uwiger/edown.git 0.5

include erlang.mk
