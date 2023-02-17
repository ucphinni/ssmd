#!/bin/ash

d=/tmp/ssmd_install
mkdir -p "$d"
cd "$d"

which miniperl > /dev/null 
let w_miniperl=$?
which git > /dev/null 
let w_git=$?
[ "$w_miniperl" -ne 0 ] && apk add miniperl
[ "$w_git" -ne 0 ] && apk add git
export SETUP_DOCKER=0
if [ ! -d ssmd ]; then
   git clone https://github.com/ucphinni/ssmd.git
fi
git checkout dev
cd ssmd
git pull
cd src
miniperl setup.pl
[ "$w_miniperl" -ne 0 ] && apk del miniperl
[ "$w_git"  -ne 0 ] && apk del git
