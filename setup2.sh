#!/bin/ash

d=/tmp/ssmd_install
mkdir -p "$d"
cd "$d"

which miniperl > /dev/null 
let w_miniperl=$?
which git > /dev/null 
let w_git=$?
which perl > /dev/null
let w_perl=$?

if [ "$w_perl" -eq 0 ]; then
    # we dont need mini perl
    w_miniperl= 1
fi

if [ "$w_miniperl" -eq 0 -a "$w_git"  -eq 0 ]; then
    apk add miniperl git
else 
    [ "$w_miniperl" -eq 0 ] && apk add miniperl
    [ "$w_git"  -eq 0 ] && apk add git
fi
export SETUP_DOCKER=0
if [ ! -d ssmd ]; then
    git clone https://github.com/ucphinni/ssmd.git
fi

cd ssmd
git checkout dev
git pull
cd src

if [ "$w_miniperl" -eq 0 ]; then
    miniperl setup.pl
    apk del miniperl
else
    perl setup.pl
fi

[ "$w_git"  -eq 0 ] && apk del git

