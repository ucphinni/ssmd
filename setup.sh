#!/bin/ash

d=/tmp/ssmd2_install
mkdir -p "$d"
cd "$d"

which miniperl > /dev/null 
let w_miniperl=$?
which git > /dev/null 
let w_git=$?

mount -o remount,size=128K    /run
mount -o remount,size=350000K /
apk add miniperl git alpine-sdk build-base apk-tools alpine-conf \
    busybox fakeroot syslinux xorriso squashfs-tools
abuild-keygen -i -a
git clone https://github.com/ucphinni/ssmd.git
git clone --depth=1 https://gitlab.alpinelinux.org/alpine/aports.git
