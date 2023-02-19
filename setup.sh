#!/bin/ash

d=/tmp/ssmd2_install
mkdir -p "$d"
cd "$d"

which miniperl > /dev/null 
let w_miniperl=$?
which git > /dev/null 
let w_git=$?

mount -o remount,size=128K    /run
mount -o remount,size=360000K /
apk add git

git clone --depth=1 https://gitlab.alpinelinux.org/alpine/aports.git
git clone --depth=1 https://github.com/ucphinni/ssmd.git
find . -type d -name .git | xargs rm -rf
apk add  alpine-sdk build-base apk-tools alpine-conf \
    busybox fakeroot syslinux xorriso squashfs-tools tar
abuild-keygen -i -a
