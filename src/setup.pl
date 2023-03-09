#!perl
my $START_CWD=$ENV{'PWD'};
sub fn_print($$) {
    my $fn = shift;
    my $str = shift;
    open(FH,'>',$fn) or die $!;
    print FH $str or die $!;
    close FH or die $!;
}

sub fn_exe($$) {
    my $fn = shift;
    my $str = shift;
    fn_print $fn, $str;
    chmod 0755, $fn or die $!;
}

$uid = getpwuid($<);
if ($uid eq 'build') {
    print("build running\n");
    my $SSMD_INSTALL_DIR=$ENV{'SSMD_INSTALL_DIR'};
    chdir $SSMD_INSTALL_DIR or die $!;
    if (-d "aports") {
	chdir "aports" or die $!;
	system qw(git pull);
	    
    }
    else {
	system qw(git clone --depth=1 https://gitlab.alpinelinux.org/alpine/aports.git);
    }

    chdir "$SSMD_INSTALL_DIR/pkg" or die "$SSMD_INSTALL_DIR/pkg:$!";
    qx'SUDO=sudo $( yes "" | abuild-keygen -i -a )';
    system qw(abuild checksum);
    system qw(abuild -r);
    system qw(sudo apk update);
    qx(mkdir -pv ~/tmp);
    my $arch=chomp qx(uname -m);
    my $res = qw(
	TMPDIR=~/tmp aports/scripts/mkimage.sh --tag edge
      --outdir $SSMD_INSTALL_DIR/iso
      --profile ssmd
      --repository https://http://dl-cdn.alpinelinux.org/alpine/edge/main
      --repository https://http://dl-cdn.alpinelinux.org/alpine/edge/community
      --repository https://http://dl-cdn.alpinelinux.org/alpine/edge/test
      --arch $arch
	);
    qx( $res );

    
    fn_exe 'aports/scripts/mkimg.ssmd.sh',<<'END';
    profile_ssmd() {
        profile_standard
        kernel_cmdline="unionfs_size=512M console=tty0 console=ttyS0,115200"
        syslinux_serial="0 115200"
        kernel_addons="zfs"
        apks="\$apks
                "
        local _k _a
        for _k in \$kernel_flavors; do
                apks="\$apks linux-\$_k"
                for _a in \$kernel_addons; do
                        apks="\$apks \$_a-\$_k"
                done
        done
        apks="\$apks linux-firmware"
    }
END

    
    exit 0;
}
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
	/^\s*(?:\@\S+)?\s*\#?\s*https?\:/ and next;
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
system qw(mount -o remount,size=128K   /run);
system qw(mount -o remount,size=310000K / );
system qw(
    # apk add shadowsocks-libev@testing iptables
    ip6tables py3-aiohttp py3-aiohttp-socks
    ssl_client py3-psutil python3 py3-aiofiles rng-tools
    cifs-utils aria2-daemon atop 
    py3-python-socks transmission-daemon  py3-transmission-rpc
    flexget py3-pip nss freetype harfbuzz ca-certificates
    ttf-freefont nodejs yarn chromium perl
    );
system qw(
    apk add alpine-sdk build-base apk-tools alpine-conf
    busybox fakeroot syslinux xorriso squashfs-tools sudo
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
sub setup_iptables_str(){
    my $cmd ='';
    for my $i (qw(OUTPUT PREROUTING)) {
	$cmd .= "iptables -t mangle -F $i || exit 2\n";
    }
    $cmd .= "if iptables -t mangle -n -L SSREDIR > /dev/null 2>&1 ; then\n";
    for my $i (qw(F X)) {
	$cmd .= "  iptables -t mangle -$i SSREDIR || exit 3\n";
    }
    $cmd .= "fi\n";
    
    my $eol = "&& \\\n";
    my $iptbsol = "iptables -t mangle -A SSREDIR ";
    $cmd .= "iptables -t mangle -N SSREDIR $eol";
    $cmd .= "$iptbsol -j CONNMARK --restore-mark $eol";
    # connection-mark -> packet-mark
    $cmd .= "$iptbsol -m mark --mark 0x2333 -j RETURN $eol";
    for my $ip (qw(0.0.0.0/8 10.0.0.0/8 100.64.0.0/10
	 127.0.0/8 169.254.0.0/16 172.16.0.0/12 192.0.0.0/24
	 192.0.2.0/24 192.88.99.0/24 192.168.0.0/16
	 198.18.0.0/15 198.51.100.0/24 203.0.113.0/24
	 224.0.0.0/4 240.0.0.0/4 255.255.255.255/32)) {
	$cmd .= "$iptbsol -d $ip $eol";
    }
    $cmd .= "$iptbsol -p tcp --syn -j MARK --set-mark 0x2333 $eol";
    $cmd .= "$iptbsol -p udp -m conntrack --ctstate NEW -j MARK --set-mark 0x2333 $eol";
    $cmd .= "$iptbsol -j CONNMARK --save-mark $eol";
    $iptbsol = 'iptables -t mangle -A OUTPUT'; # chg ip tbls start of line
    $cmd .= "$iptbsol -m owner --uid-owner root -j RETURN $eol";    
    $cmd .= "$iptbsol -p tcp -m addrtype --src-type LOCAL ! --dst-type LOCAL -j SSREDIR $eol";
    $cmd .= "$iptbsol -p udp -m addrtype --src-type LOCAL ! --dst-type LOCAL -j SSREDIR $eol";
    $iptbsol = 'iptables -t mangle -A PREROUTING';
    # proxy traffic passing through this machine (other->other)
    $cmd .= "$iptbsol -p tcp -m addrtype ! --src-type LOCAL ! --dst-type LOCAL -j SSREDIR $eol";
    $cmd .="$iptbsol -p udp -m addrtype ! --src-type LOCAL ! --dst-type LOCAL -j SSREDIR $eol";
    $cmd .="$iptbsol  -p tcp -m mark --mark 0x2333 -j TPROXY --on-ip 127.0.0.1 --on-port 1088 $eol";
    $cmd .="$iptbsol -p udp -m mark --mark 0x2333 -j TPROXY --on-ip 127.0.0.1 --on-port 1088 $eol";
    "$cmd exit 0";
}
rmflexgetui;
my $SSMD_INSTALL_DIR=$ENV{'SSMD_INSTALL_DIR'};

chdir $SSMD_INSTALL_DIR or die $!;

system qw(rc-update add local) and die $!;

fn_exe 'pkg/f0', <<'END';
#!/bin/ash
[ "$IFACE" = "lo" ] || exit 0
ip rule add fwmark 9011 table 100
ip route add local default dev lo table 100
END

$sis = setup_iptables_str;
fn_exe 'pkg/iptables.start', <<END;
modprobe -v ip_tables
modprobe -v ip6_tables
modprobe -v iptable_nat
$sis
echo "failed setup iptables"
exit 1
END

fn_print('pkg/answerfile',<<END);
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

fn_exe 'pkg/post-install', << 'END';
END
 
fn_print 'pkg/APKBUILD', << 'END';
# Maintainer: ucphinni <ucphinni@gmail.com>

pkgname=ssmd
pkgver=0.1.0
pkgrel=1
pkgdesc="Setup user defined pkage"
url="http://kutt.it/bssmd"
arch="noarch"
license="Artistic-2.0"
source="answerfile post-install f0 iptables.start"
options="!check"

package() {
    installation_path="$pkgdir"/usr/bin
    mkdir -p "$installation_path"
    cp -pf answerfile post-install "$installation_path"

    installation_path="$pkgdir"/etc/network/if-up.d/
    mkdir -p "$installation_path"
    cp -pf f0 "$installation_path"

    installation_path="$pkgdir"/etc/local.d/
    mkdir -p "$installation_path"
    cp -pf iptables.start "$installation_path"
}
END

system qw(adduser build -G abuild);
qx(echo "%abuild ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/abuild);

system 'su','build','-c',$^X. " " .$SSMD_INSTALL_DIR."/ssmd/src/setup.pl";

exit 0;
# qx(export IFACE='LO'; sh /etc/network/if-up.d/f0);
# system qw(/etc/local.d/iptables.start) and die $!;

    
chdir '/';


system qw(
  pip install -U aiosqlite aioftp
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
