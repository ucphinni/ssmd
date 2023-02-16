#!/usr/bin/miniperl
system qw(
  apk add atop sqlite busybox-ifupdown shadowsocks-libev
  ssl_client py3-psutil vsftpd python3 py3-aiofiles rng-tools
  cifs-utils py3-rfc3986 aria2-daemon py3-wheel  py3-pip
  py3-httpx py3-python-socks transmission-daemon py3-pip py3-async-timeout
);
open(FH,'>','/tmp/alpine_setup.cfg') or die $!;
print FH <<END;
# Example answer file for setup-alpine script
# If you don't want to use a certain option, then comment it out

# Use US layout with US variant
KEYMAPOPTS="us us"

# Set hostname to 'alpine'
HOSTNAMEOPTS=alpine

# Set device manager to mdev
DEVDOPTS=mdev

# Contents of /etc/network/interfaces
INTERFACESOPTS="auto lo
iface lo inet loopback

auto eth0
iface eth0 inet dhcp
    hostname media-pull
"

# Search domain of example.com, Google public nameserver
DNSOPTS="-d example.com 8.8.8.8"

# Set timezone 
TIMEZONEOPTS="America/New_York"

# set http/ftp proxy
#PROXYOPTS="http://webproxy:8080"
PROXYOPTS=none

# Add first mirror (CDN)
APKREPOSOPTS="-1"

# Create admin user
USEROPTS="-a -u -g audio,video,netdev juser"
#USERSSHKEY="ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOIiHcbg/7ytfLFHUNLRgEAubFz/13SwXBOM/05GNZe4 juser@example.com"
#USERSSHKEY="https://example.com/juser.keys"

# Install Openssh
SSHDOPTS=dropbear
#ROOTSSHKEY="ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOIiHcbg/7ytfLFHUNLRgEAubFz/13SwXBOM/05GNZe4 juser@example.com"
#ROOTSSHKEY="https://example.com/juser.keys"

# Use openntpd
# NTPOPTS="openntpd"
NTPOPTS=chronyd

# Use /dev/sda as a sys disk
# DISKOPTS="-m sys /dev/sda"
DISKOPTS=none

# Setup storage with label APKOVL for config storage
#LBUOPTS="LABEL=APKOVL"
LBUOPTS=none

#APKCACHEOPTS="/media/LABEL=APKOVL/cache"
APKCACHEOPTS=none
END
