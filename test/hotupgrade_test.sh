#!/bin/sh
#
# Script to test release hot code upgrade
#
# Usage:
#   # to generate and launch previous release, then generate a new release and upgrade files
#   ./test/hotupgrade_test.sh setup <revision-to-upgrade-from>
#   # to upgrade the release and perform sanity checks
#   ./test/hotupgrade_test.sh check

set -e
set -x

REBAR=`which rebar3 || echo ./rebar3`
COMMAND=$1
BASE_REV=$2

do_setup() {
    CUR_VERSION=`git log -1 --format='%H'`   #`git rev-parse --abbrev-ref HEAD`
    # Building the release for the OLD version
    if [ ! -d ebin ]; then mkdir ebin; fi
    cp test/relx-base.config ./relx.config
    cp src/pooler.appup.src ebin/pooler.appup
    git checkout $BASE_REV
    if [ ! -e src/pooled_gs.erl ]; then cp test/pooled_gs.erl src/; fi
    $REBAR as test release

    ./_build/test/rel/pooler_test/bin/pooler_test daemon

    git clean -df

    # Building the release and relup file with the NEW version
    git checkout $CUR_VERSION
    if [ ! -e src/pooled_gs.erl ]; then cp test/pooled_gs.erl src/; fi
    $REBAR as test release --config test/relx-current.config
    $REBAR as test relup   --config test/relx-current.config --relname pooler_test --relvsn=2.0.0 --upfrom=1.0.0
    $REBAR as test tar     --config test/relx-current.config
    cp _build/test/rel/pooler_test/pooler_test-2.0.0.tar.gz _build/test/rel/pooler_test/releases/
    ./_build/test/rel/pooler_test/bin/pooler_test-1.0.0 unpack 2.0.0
}

TEST='Taken=[begin P = pooler:take_member(pool1, 1000), is_pid(P) orelse error(P), P end || _ <- lists:seq(1, 5)], [pooler:return_member(pool1, P, fail) || P <- Taken], ok.'

do_check() {
    RES1=`./_build/test/rel/pooler_test/bin/pooler_test-1.0.0 eval "${TEST}"`
    if [ "$RES1" != "ok" ]; then
        echo "Before upgrade checkout failed" >&2
        echo $RES1 >&2
        exit 1
    fi
    ./_build/test/rel/pooler_test/bin/pooler_test-1.0.0 upgrade 2.0.0
    RES2=`./_build/test/rel/pooler_test/bin/pooler_test eval "${TEST}"`
    if [ "$RES2" != "ok" ]; then
        echo "After upgrade checkout failed" >&2
        echo $RES2 >&2
        exit 1
    fi
    ./_build/test/rel/pooler_test/bin/pooler_test stop
}

"do_$COMMAND"
