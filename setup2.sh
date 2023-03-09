#!/bin/ash

export SSMD_INSTALL_DIR=$HOME/ssmd_install
# rm -rf $SSMD_INSTALL_DIR
mkdir -p "$SSMD_INSTALL_DIR"
cd "$SSMD_INSTALL_DIR"
f="$SSMD_INSTALL_DIR"
while [[ $f != / ]]; do chmod +x "$f"; f=$(dirname "$f"); done;

chmod a+x "$SSMD_INSTALL_DIR"
rm -rf iso pkg
mkdir -p iso pkg
chmod 777 iso pkg .

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
git checkout dev || exit 1
git pull || exit 1
cd src || exit 1

if [ "$w_perl" -eq 0 ]; then
    perl setup.pl
else
    miniperl setup.pl
    apk del miniperl
fi

[ "$w_git"  -ne 0 ] && apk del git

