#!/bin/ash

d=/tmp/ssmd_install
mkdir -p "$d"
cd "$d"

which miniperl > /dev/null 
let w_miniperl=$?
which git > /dev/null 
let w_git=$?

if [ "$w_miniperl" -ne 0 -a "$w_git"  -ne 0 ]; then
    apk add miniperl git
else 
    [ "$w_miniperl" -ne 0 ] && apk add miniperl
    [ "$w_git"  -ne 0 ] && apk add git
fi
export SETUP_DOCKER=0
if [ -d ssmd ]; then
   git clone https://github.com/ucphinni/ssmd.git
fi
cd ssmd
git checkout dev
git pull
cd src
miniperl setup.pl

if [ "$w_miniperl" -ne 0 -a "$w_git"  -ne 0 ]; then
    apk del miniperl git
else 
    [ "$w_miniperl" -ne 0 ] && apk del miniperl
    [ "$w_git"  -ne 0 ] && apk del git
fi
