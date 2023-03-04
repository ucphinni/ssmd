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

if [ "$w_miniperl" -ne 0 -a "$w_git"  -ne 0 -a "$w_perl" -ne 0 ]; then
    apk add miniperl git
else 
    [ "$w_miniperl" -ne 0 -a "$w_perl" -ne 0 ] && apk add miniperl
    [ "$w_git"  -ne 0 ] && apk add git
fi
export SETUP_DOCKER=0
if [ ! -d ssmd ]; then
    git clone https://github.com/ucphinni/ssmd.git
fi

cd ssmd || exit 1
git checkout dev
git pull
cd src

if [ "$w_perl" -eq 0 ]; then
    perl setup.pl
else
    miniperl setup.pl
    apk del miniperl
fi

[ "$w_git"  -eq 0 ] && apk del git

