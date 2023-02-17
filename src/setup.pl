#!/usr/bin/miniperl
sub get_repo_url_line() {
    my @ret;
    open F,"/etc/apk/repositories" or die $!;
    while (<F>) {
	if (/^\s*(https?\:\/\/.*\/)([^\/]+)\/(?:main|community|testing)\s*$/) {
	    @ret = ($1,$2);
	    last;
	}
    }
    close F or die $!;    
    @ret or die "bad /etc/apk/repositories file";
    return @ret;
}

sub set_repo_file_and_upgrade($$) {
    my ($url,$ver) = @_;
    my @ret = ();
    open F,"/etc/apk/repositories" or die $!;
    while (<F>) {
	/^\s*\#?\s*https?\:/ and next;
       push @ret, $_;
    }
    close F or die $!;
    open F, '>',"/etc/apk/repositories" or die $!;
    print F @ret;
    print F "$url/$ver/main\n";
    $ver eq 'edge' and print F "$url/$ver/community\n";
    $ver eq 'edge' and print F "$url/$ver/testing\n";
    close F or die $!;
    system qw(apk upgrade);
}

sub check_repo_version_valid() {
    my ($url,$ver) = get_repo_url_line();
    my $urlstr = "$url/$ver/main";
    print("$urlstr\n")
    `wget -qO '$urlstr'  > /dev/null`;
    $? != 0 and return undef;
    return 1;
}
sub inc_major_version($) {
    my ($ver) = @_;
    my ($major,$minor) = /(\d+)\.(\d+)/;
    $major = int($major);
    $minor = int($minor);
    ++$major;
    "${major}.0"
}

sub inc_minor_version($){
    my ($ver) = @_;
    my ($major,$minor) = /(\d+)\.(\d+)/;
    $major = int($major);
    $minor = int($minor);
    ++$minor;
    "${major}.${minor}"
}

sub get_to_edge() {
    my ($url,$ver) = get_repo_url_line;
    while (check_repo_version_valid) {
	set_repo_file_and_upgrade($url,$ver);
	if (inc_minor_version($ver)) {
	    $ver = inc_minor_version($ver);
	    next;
	}
	if (inc_major_version($ver)) {
	    $ver = inc_major_version($ver);
	    next;
	}
	last;
    }
    set_repo_file_and_upgrade($url,'edge');
    
}

get_to_edge();
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
