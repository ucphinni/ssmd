#!/bin/ash
which miniperl > /dev/null || apk add miniperl
export SETUP_DOCKER=0
/usr/local/bin/setup.pl
