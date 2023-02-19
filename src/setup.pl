#!/usr/bin/miniperl
sub get_repo_url_line() {
    my @ret;
    open F,"/etc/apk/repositories" or die $!;
    while (<F>) {
	if (/^\s*(https?\:\/\/.*)\/([^\/]+)\/(?:main|community|testing)\s*$/) {
	    @ret = ($1,$2);
	    last;
	}
    }
    close F or die $!;    
    @ret or die "bad /etc/apk/repositories file";
    return @ret;
}

sub set_repo_file($$) {
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
    $ver eq 'edge' and print F "\@testing $url/$ver/testing\n";
    close F or die $!;
}

sub check_repo_version_valid($$) {
    my ($url,$ver) = @_;
    my $urlstr = "$url/$ver/main";
    $urlstr =~ s/'/'"'"'/g;
    my $cmd = "wget -qO- '$urlstr'  > /dev/null";
    qx/$cmd/;
    $? != 0 and return undef;
    return 1;
}
sub inc_major_version($) {
    local ($_) = @_;
    my ($major,$minor) = /(\d+)\.(\d+)/;
    $major = int($major);
    ++$major; $minor=0;
    s/\d+\.\d+/${major}.${minor}/;
    $_;
}

sub inc_minor_version($){
    local ($_) = @_;
    my ($major,$minor) = /(\d+)\.(\d+)/;
    $major = int($major);
    $minor = int($minor);
    ++$minor;
    s/\d+\.\d+/${major}.${minor}/;
    $_;
}

sub get_to_edge() {
    my ($url,$ver) = get_repo_url_line;
    for (;;) {
	system qw(apk upgrade);

	$ver ne 'edge' and check_repo_version_valid($url,$ver) or last;
	set_repo_file($url,$ver);
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
    set_repo_file($url,'edge');
    
}

get_to_edge();
system qw(mount -o remount,size=4096K   /run);
system qw(mount -o remount,size=300000K / );
system qw(
  apk add shadowsocks-libev@testing iptables ip6tables
  ssl_client py3-psutil vsftpd python3 py3-aiofiles rng-tools
  cifs-utils aria2-daemon atop py3-babel py3-sqlalchemy py3-sphinx
    py3-httpx py3-python-socks transmission-daemon  py3-transmission-rpc
    flexget
    );

sub rmflexgetui() {
    print qx"rm -rf /usr/lib/python3.*/site-packages/flexget/ui/v1";
}
sub mvpypkg($) {
    my ($pkg) = @_;
    my $cmd;
    $cmd = "cp -pr /usr/lib/python3.*/site-packages/$pkg /run/extra/python/site-packages";
    print qx/$cmd/;
    print qx"rm -rf /usr/lib/python3.*/site-packages/$pkg";
    $cmd =  "ln -s /run/extra/python/site-packages/$pkg ";
    $cmd .= '$(ls -d /usr/lib/python3.*)/site-packages/';
    $cmd .= $pkg;
    print qx"$cmd";
}
sub setup_iptables(){
    qx(iptables -t mangle -F SSREDIR);
    qx(iptables -t mangle -X SSREDIR);
    system qw(iptables -t mangle -F OUTPUT);
    system qw(iptables -t mangle -X OUTPUT);
    system qw(iptables -t mangle -F PREROUTING);
    system qw(iptables -t mangle -X PREROUTING);
    system qw(iptables -t mangle -F SSREDIR);
    system qw(iptables -t mangle -X SSREDIR);
    system qw(iptables -t mangle -N SSREDIR) and die $!;
    # connection-mark -> packet-mark
    system qw(iptables -t mangle -A SSREDIR -j CONNMARK --restore-mark) and die $!;
    system qw(iptables -t mangle -A SSREDIR -m mark --mark 0x2333 -j RETURN) and die $!;
    for my $ip (qw(0.0.0.0/8 10.0.0.0/8 100.64.0.0/10 127.0.0/8 169.254.0.0/16 172.16.0.0/12 192.0.0.0/24
		  192.0.2.0/24 192.88.99.0/24 192.168.0.0/16 198.18.0.0/15 198.51.100.0/24 203.0.113.0/24
	         224.0.0.0/4 240.0.0.0/4 255.255.255.255/32)) {
	system qw(iptables -t mangle -A SSREDIR -d), $ip and die $!;

    }
    system qw"iptables -t mangle -A SSREDIR -p tcp --syn -j MARK --set-mark 0x2333" and die $!;
    system qw"iptables -t mangle -A SSREDIR -p udp -m conntrack --ctstate NEW -j MARK --set-mark 0x2333"
	and die $!;
    system qw"iptables -t mangle -A SSREDIR -j CONNMARK --save-mark" and die $!;
    system qw(iptables -t mangle -A OUTPUT -m owner --uid-owner root -j RETURN ) and die $!;    
    system qw(iptables -t mangle -A OUTPUT -p tcp -m addrtype --src-type LOCAL ! --dst-type LOCAL -j SSREDIR)
	and die $!;
    system qw(iptables -t mangle -A OUTPUT -p udp -m addrtype --src-type LOCAL ! --dst-type LOCAL -j SSREDIR)
	and die $!;

     # proxy traffic passing through this machine (other->other)
    system qw(iptables -t mangle -A PREROUTING -p tcp -m addrtype ! --src-type LOCAL ! --dst-type LOCAL -j SSREDIR)
	and die $!;
    system qw(iptables -t mangle -A PREROUTING -p udp -m addrtype ! --src-type LOCAL ! --dst-type LOCAL -j SSREDIR)
	and die $!;

    # hand over the marked package to TPROXY for processing
    system qw(iptables -t mangle -A PREROUTING -p tcp -m mark --mark 0x2333 -j TPROXY --on-ip 127.0.0.1 --on-port 1088)
	and die $!;
    system qw(iptables -t mangle -A PREROUTING -p udp -m mark --mark 0x2333 -j TPROXY --on-ip 127.0.0.1 --on-port 1088)
	and die $!;


}
setup_iptables;

rmflexgetui;
open(FH,'>','/etc/network/if-up.d/f0') or die $!;
print FH <<END;
#!/bin/ash
[ "$IFACE" = "lo" ] || exit 0
ip rule add fwmark 9011 table 100
ip route add local default dev lo table 100
END
    
close FH or die $!;
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
chdir '/run';

system qw(
  python3 -m venv env --system-site-packages --symlinks
);

system qw(
  /run/env/bin/pip install -U httpx-socks[asyncio] aiosqlite aioftp
);

system qw(
  rc-update add transmission-daemon
);

system qw(
  rc-service transmission-daemon start
);

system qw(
  rc-update add aria2
);

system qw(
  rc-service aria2 start
);
