"""
@Project   : 
@Module    : 
@Author    : ucphinni [ucphinni@gmail.com]
@Created   : 2023/1/12 16:36
@Desc      :
"""
import base64
import os
import re
import sqlite3
from sqlite3 import Error
import json
import aiosqlite
import asyncio
import aiohttp
from aiohttp_socks import ProxyConnector
import time
import socket
import aiofiles
import aiofiles.os

PTN_IPV4 = re.compile(
    r'^((25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(25[0-5]|2[0-4]\d|[01]?\d\d?)$')
SSRBINPATTERN = re.compile(
    r'^([a-zA-Z0-9_=-]+)(.*)')
PORTPATTERN = re.compile(
    r'^(\d+)(.*)')
PLUGINPATTERN = re.compile(
    r'\?(plugin=.*(?:\#.+)?)')
PLUGIN2PATTERN = re.compile(
    r'plugin=([^\;]+)\;([^#]*?)(?:#(.*))')
REMARKPATTERN = re.compile(
    r'(?:#(.*))$')
def create_connection(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


conn = create_connection('/tmp/conn.db')
sql_create_table = """CREATE TABLE IF NOT EXISTS connurl (
                            id integer PRIMARY KEY AUTOINCREMENT NOT NULL,
                            url text NOT NULL
                        );"""
create_table(conn,sql_create_table)

sql_create_table = """
CREATE TABLE conncli_locsrv (id integer PRIMARY KEY AUTOINCREMENT NOT NULL, server text NOT NULL,
server_port integer NOT NULL, pid integer, connurl_id integer NOT NULL REFERENCES connurl (id)
ON DELETE CASCADE ON UPDATE CASCADE, local_port integer NOT NULL, UNIQUE (server, server_port) ON CONFLICT ROLLBACK);
"""
create_table(conn,sql_create_table)
sql_create_table = """
CREATE TABLE conncli_health (id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, conncli_locsrv_id
INTEGER REFERENCES conncli_locsrv (id) ON DELETE CASCADE ON UPDATE CASCADE NOT NULL, tstmp DATETIME DEFAULT
(CURRENT_TIMESTAMP) NOT NULL, status CHAR (3) NOT NULL DEFAULT ok,exception TEXT,duration INTEGER NOT NULL);
"""
create_table(conn,sql_create_table)
sql_create_table = """
CREATE TABLE conn_locsrv_type (id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, tstmp DATETIME DEFAULT
(CURRENT_TIMESTAMP) NOT NULL, conncli_locsrv_id INTEGER REFERENCES conncli_locsrv (id) ON DELETE
CASCADE ON UPDATE CASCADE, type CHAR CHECK (type IN ('P', 'S')) NOT NULL);
"""
create_table(conn,sql_create_table)

sql_create_table = """
create view if not exists conn_ss_redir as
with b as ( select 'P' type union all select 'S' )
select b.type, f.tstmp,f.conncli_locsrv_id from b
left join (
select g.* from conn_locsrv_type g,(select type, max(id) id from conn_locsrv_type group by 1) f
where g.id = f.id and f.type = g.type
) f on (f.type = b.type)
"""
create_table(conn,sql_create_table)

sql_create_table = """
create view if not exists conncli_health_last as
select 'ssc'||q.conncli_locsrv_id procname, q.* from (select ifnull(g.type,'L') type,f.* from
conncli_locsrv h
left join conn_ss_redir g on (h.id = g.conncli_locsrv_id)
left join (select f.id conncli_locsrv_id,g.duration,f.pid,f.local_port,g.tstmp,g.status,
g.exception from conncli_locsrv f
left join (select g.tstmp,g.conncli_locsrv_id,g.duration,g.status,g.exception from conncli_health g,
(select conncli_locsrv_id, max(tstmp) tstmp from conncli_health group by 1) f
where g.tstmp = f.tstmp and g.conncli_locsrv_id = f.conncli_locsrv_id) g on (f.id = g.conncli_locsrv_id))
f on (f.conncli_locsrv_id = h.id)) q
"""
create_table(conn,sql_create_table)
async def create_json_file(db,fn,server,port,sport = None):
    cur = await db.execute('''
    select c.url url,
    cu.local_port local_port
    from  conncli_locsrv cu,connurl c
    where cu.server = ? and cu.server_port = ?
    and c.id = cu.connurl_id ''',(server,port))
    row = await cur.fetchone()
    if row is None:
        return
    cfg = parse(row['url'])

    if sport is None:
        cfg['local_port'] = row['local_port']
    else:
        cfg['local_port'] = sport
        
    cfg['local_address'] = '127.0.0.1'
    
    with open(fn, 'w') as f:
        json.dump(cfg, f)


def _fill_missing(string: str):
    """Fill base64 decoded string with ="""
    missing_padding = 4 - len(string) % 4
    if missing_padding:
        return string + '=' * missing_padding
    return string


def check_ipv4(ip: str):
    """Check if the ip is valid IPV4 format"""
    if PTN_IPV4.match(ip):
        return True
    else:
        return False


def ssr_parse(url_body: str, local_address: str = '127.0.0.1', local_port: int = 1080):
    """Parse the ssr_url into dict"""
    result = {}
    try:
        url_body = _fill_missing(url_body)
        url_body = base64.urlsafe_b64decode(url_body).decode('utf8')

        config = re.split(r'[:/?&]', url_body)
        
        ip = config[0]
        port = config[1]
        protocol = config[2]
        method = config[3]
        obfs = config[4]
        password_raw = config[5]
        password_corrected = _fill_missing(password_raw)
        password_decoded = base64.urlsafe_b64decode(
            password_corrected).decode('utf8')

        # get extra param in ssr string params
        for param in config:
            matches = re.match(r"^(\w+)=(.+)$", param)
            if matches:
                key, value = matches[1],  matches[2]

                if key == "obfsparam":
                    value_decoded = base64.urlsafe_b64decode(
                        _fill_missing(value)).decode('utf8')
                    result['obfs_param'] = value_decoded
                elif key == "protoparam":
                    value_decoded = base64.urlsafe_b64decode(
                        _fill_missing(value)).decode('utf8')
                    result['protocol_param'] = value_decoded
                else:
                    result[key] = value

    except Exception as err:
        raise
    else:
        result.update({'server': ip,
                       'method': method,
                       'obfs': obfs,
                       'password': password_decoded,
                       'server_port': port,
                       'protocol': protocol,
                       'local_address': local_address,
                       'local_port': local_port,
                       })
        return result
def clear_ss(deb64):
    pos = deb64.rfind('#')
    return deb64[:pos] if pos > 0 else deb64

def fill(b64):
    return b64 + "=" * (4 - len(b64) % 4)
def ss_parse(txt: str, local_address: str = '127.0.0.1', local_port: int = 1088):
    # method:password@server:port
    result = SSRBINPATTERN.search(txt)
    encodedstr = result.group(1)
    rest = result.group(2)
    
    conf = clear_ss(bytes.decode(base64.urlsafe_b64decode(fill(encodedstr))))
    conf_list = []
    conf += rest

    for part in conf.split('@'):
        conf_list += part.split(':')
    conf_dict = dict()
    conf_dict["method"] = conf_list[0]
    conf_dict["password"] = conf_list[1]
    conf_dict["server"] = conf_list[2]
    conf_dict["server_port"] = conf_list[3]
    result = PORTPATTERN.search(conf_dict["server_port"])
    encodedstr = result.group(1)
    rest = result.group(2)
    conf_dict["server_port"] = int(encodedstr)
    result = PLUGINPATTERN.search(txt)
    if result is not None:
        txt = result.group(1)
        result = PLUGIN2PATTERN.search(txt)
        conf_dict["plugin"] = result.group(1)
        conf_dict["plugin_opts"] = result.group(2)
        conf_dict["remark"] = result.group(3)
    else:
        result = REMARKPATTERN.search(rest)
        if result is not None:
            conf_dict["remark"] =result.group(1)
    conf_dict["local_address"] = local_address
    conf_dict["local_port"] = local_port
    
    return conf_dict

def parse(txt):
    local_address='127.0.0.1'
    local_port = 0
    
    if 'ssr://' in txt:
        return ssr_parse(txt.replace('ssr://', ''),local_address,local_port)
    if 'ss://' in txt:
        return ss_parse(txt.replace('ss://', ''),local_address,local_port)
    raise Exception('ss url or ssr url format error.')

async def get_unused_socket():
    # loop = asyncio.get_running_loop()
    # server = await loop.create_server(asyncio.Protocol,'127.0.0.1,0);
    # await server.wait_closed()
    sock = socket.socket()
    sock.bind(('', 0))
    ret = sock.getsockname()[1]
    sock.close()
    return ret

START_STOP_DAEMON='/sbin/start-stop-daemon'
import psutil
ss_startid = 1
ss_lock = asyncio.Lock()
async def killbyname(name):
    for proc in psutil.process_iter():
       if proc.name() == name and proc.pid != os.getpid():
            proc.kill()

async def killbynames(names):
    for proc in psutil.process_iter():
       if proc.name() in names and proc.pid != os.getpid():
            proc.kill()

async def killbypid(pid):
    if psutil.pid_exists(pid):
        proc = psutil.Process(pid)
        proc.kill()


async def proc_reload():
    proc = await asyncio.create_subprocess_exec(
        '/etc/init.d/transmission-daemon', 'restart'
        )
    await proc.wait()

async def load_db_conn_strs_from_file(db):
    chg = False
    async with aiofiles.open("./conn.cfg","r") as fp:
        async for line in fp:
            url = line.strip()
            if (await sql_rc(db,'''select c.server,c.local_port
                 from  connurl cu, conncli_locsrv c where cu.url = trim(?)
                 and cu.id = c.connurl_id''',(line,))) is not None:
                continue # already in database.

            chg = True
            repl = False
            cfg = parse(line)
            cclsrow = await sql_rc(db,'''select * from  conncli_locsrv
                 where server = ? and server_port = ?''',
                             (cfg['server'],cfg['server_port']))
            if cclsrow is not None and cclsrow['pid'] is not None:
                await connproc_stop(db,cfg['server'],cfg['server_port'])
            if cclsrow is not None:
                # delete as it will be recreated later.
                print (f"deleted {cclsrow['id']}")
                await db.execute ('''delete from conncli_locsrv where id = ? ''',(cclsrow['id'],))
                
            url = line
            cur = await db.execute('''select 1 as res from  connurl where url = ?''',
                                   (line,))
            row = await cur.fetchone()
            if row is not None:
                continue # already there... skip
            
            await db.execute('''insert into connurl (url) VALUES(?)''',(line,))

            cur = await db.execute('''select id from  connurl where url = ?''',
                                   (line,))
            row = await cur.fetchone()
            if row is None:
                raise Exception("no connurl")
            connid = row['id']
            cfg['local_port'] = await get_unused_socket()
            
            await db.execute('''insert into conncli_locsrv (connurl_id,server,server_port,local_port)
            VALUES(?,?,?,?)''',(connid,cfg['server'],cfg['server_port'],cfg['local_port']))
    return chg            


async def log_conn_health(db,id, status,dur,exception=None):
    exception = str(exception) if exception is not None else None
    await db.execute ('''
       insert into conncli_health (conncli_locsrv_id,status,exception,duration)
       VALUES (?,?,?,?)
    ''', (id,status,exception,dur))
    
async def ss_socks_health(db,id,isp_extip = None,force = False):
    global mins_per_redir
    if force:
        cur = await db.execute('''
select 1  res,ccls.local_port from conncli_health_last ch,conncli_locsrv ccls
        where ch.conncli_locsrv_id = ccls.id and
        ccls.id = ? order by 1 desc limit 1
''', (id,))
    else:
        cur = await db.execute('''
with b as (
select min(duration)*1.2 dur from conncli_health_last
)
select current_timestamp > datetime(ch.tstmp,case when 
(ch.duration < b.dur or (select count(*) < 2 from conncli_health_last,b where duration < b.dur limit 2))
and ch.status != 'ok' and 2 > ifnull((select count(*) from conncli_health_last where status == 'ok'
        limit 2),0) or ch.status == 'ok' then 
 case when exists (
     with a as (select ? mins_interval,10000000 random_steps, 
        CURRENT_TIMESTAMP now, DATETIME(tstmp) start from conncli_health_last where type = 'P'),
     c as (select (cos((julianday(a.now) - julianday(a.start))*(24*60.0/a.mins_interval)* 2 * pi()) +1)/2 prob from a)
     select 1 from a,c where c.prob >= ((abs(random())) % (  a.random_steps)*1.0)/a.random_steps 
) then
    '+1 second'
 else '+15 minutes'
 end 
 else '+2 hour' end)
 res,ccls.local_port from conncli_health_last ch,b,conncli_locsrv ccls
        where ch.conncli_locsrv_id = ccls.id and ccls.id = ? order by 1 desc limit 1
''', (mins_per_redir,id))
    row = await cur.fetchone()

    if row is None or row['res'] != 0:
        pass
    else:
        return
    port = row['local_port']

    t = time.time()
    try:

        print (f"health check {id}")
        connector = ProxyConnector.from_url(f'socks5://127.0.0.1:{port}')
        async with aiohttp.ClientSession(connector=connector) as session:
            response = await session.get('http://ifconfig.me')
            ret = await response.text()
            if ret is None or ret == '':
                await log_conn_health(db,id,"nip",time.time()- t,"No IP from external host")
                return
            if isp_extip is not None and ret == isp_extip:
                await log_conn_health(db,id,"eip",time.time()- t,"Exposed IP")
                return
            await log_conn_health(db,id,"ok",time.time()- t,None)
    except Exception as e:
        await log_conn_health(db,id,"err",time.time()- t,str(e))
    finally:
        print (f"health check {id} done")

async def perform_sock_health_checks(db,force):

    cur = await db.execute('''
        select id
        from conncli_locsrv ccls where ccls.pid is not NULL
         and not exists (
            select 1 from conn_ss_redir where ccls.id = conncli_locsrv_id)''')
    rows = await cur.fetchall()
    tasks = map(lambda x:
                asyncio.create_task(ss_socks_health(
                    db,x['id'],force=force)),
                rows)
    await asyncio.gather(*tasks)
async def perform_db_cleanups(db):
    await db.execute('''
    delete from conncli_health where id in (
    select g.id from conncli_health g, (
    select id,conncli_locsrv_id,tstmp,
    row_number() over (partition by conncli_locsrv_id
    order by tstmp desc) recnum
    from conncli_health
    ) f where f.recnum > 20 and g.id = f.id
    )
    ''')

async def choose_new_redir(db,mins):
    sql = f"""
    select * from conncli_health_last where status = 'ok'
    """
    cur = await db.execute(sql)
    rows = await cur.fetchone()
    ret = not( rows is not None and len(rows) >= 2)
    sql = f"""
with b as ( select 'P' type union all select 'S' )
select b.type,current_timestamp now,
    datetime(f.tstmp,'+{mins} minutes') future,
    ifnull(current_timestamp > datetime(f.tstmp, 
    '+{mins} minutes') ,2) need_another, f.tstmp,
    f.conncli_locsrv_id from b
    left join (
    select g.* from conn_locsrv_type g,(select type, max(tstmp) tstmp
    from conn_locsrv_type group by 1) f
    where g.tstmp = f.tstmp and f.type = g.type
    ) f on (f.type = b.type)
"""
    cur = await db.execute(sql)
    rows = await cur.fetchall()
    if not any(map(lambda r:r['need_another'],rows)):
        return False,False
    sql = '''
        select * from conncli_health_last 
            where status = 'ok' order by type desc limit 3;
'''
    cur = await db.execute(sql)
    rows = await cur.fetchall()
    rows.reverse()
    x,y = None,None
    if rows is None or len(rows) == 0:
        pass
    elif len(rows) == 3 or len(rows) == 2:
        x = next(filter(lambda r:r['type'] != 'P' ,rows),None)
        y = next(filter(lambda r:r['conncli_locsrv_id'] != x['conncli_locsrv_id'],rows),None)
    elif len(h) == 1:
        x,y = rows[0],None
    sql = '''
        select * from conncli_health_last where type in ('P','S') order by type
'''
    if x is not None:
        x = x['conncli_locsrv_id']
    if y is not None:
        y = y['conncli_locsrv_id']
    await db.commit()
    cur = await db.execute(sql)
    oldrows = await cur.fetchall()
    await db.execute('insert into conn_locsrv_type(conncli_locsrv_id,type) values (?,?)',
        (x,'P'))
    await db.execute('insert into conn_locsrv_type(conncli_locsrv_id,type) values (?,?)',
        (y,'S'))
    if len(oldrows) == 0:
        return True,True
    pna = oldrows[0]['conncli_locsrv_id'] != x
    sna = oldrows[1]['conncli_locsrv_id'] != y
    return pna,sna 

async def sql_rc(db,sql,t=None):
    cur = await db.execute(sql,t)
    if cur is None:
        return None
    return await cur.fetchone()

def check_ss_type_from_pid(pid):
    proc = psutil.Process(pid)
    ss_redir_flg = 'ss-redir' in proc.exe()
    ss_local_flg = 'ss-local' in proc.exe()
    type = None
    if ss_redir_flg:
        type = 'ss-redir'
    if ss_local_flg:
        type = 'ss-local'
    return type


async def check_procs(db):
    cur = await db.execute('''
              select u.url,c.* from conncli_health_last c,
                connurl u,conncli_locsrv cl
              where c.conncli_locsrv_id = cl.id and cl.connurl_id = u.id
    ''')
    if cur is None:
        return []
    n2p={}
    for proc in psutil.process_iter():
       if not proc.cmdline() or not proc.name().startswith('ss-'):
           continue
       n2p[proc.cmdline()[0]] = proc.pid
    

    rows = await cur.fetchall()
    ret = []
    for r in rows:
        proctype = None
        type = 'ss-redir' if r['type'] != 'L' else 'ss-local'
        procnm = r['procname']
        if procnm in n2p:
            proctype = check_ss_type_from_pid( n2p[procnm])
        procnm_pid = n2p[procnm] if procnm in n2p else None

        # print ("::",procnm,type,proctype, procnm_pid)
        if type != proctype or procnm not in n2p:
            ret.append((procnm,procnm_pid,type,r))
    
    return ret

async def ss_start2(db,url,id):
    global ss_startid
    startid = None

    cur = await db.execute('''
select c.procname,c.type,cl.server,cl.server_port,case when c.type = 'P' then 1088 when c.type = 'S'
    then 1089 else cl.local_port end local_port from conncli_health_last c,conncli_locsrv cl
    where c.conncli_locsrv_id = cl.id and cl.connurl_id = ?
 ''',(id,))
    cclsrow = await cur.fetchone()
    if cclsrow is None:
        raise Exception("No srv set")
    ss_type = 'ss-local' if cclsrow['type'] == 'L' else 'ss-redir'

    async with ss_lock:
        startid = ss_startid
        ss_startid += 1

    pidfile=f"/tmp/sss{startid}.pid"
    jsonfn=f"/tmp/sss{startid}.json"
    server = cclsrow['server']
    port   = cclsrow['server_port']
    sport  = cclsrow['local_port']
    pname  = cclsrow['procname']
    
    await create_json_file(db,jsonfn,server,port,sport)
    if cclsrow['type'] == 'L':
        ssexe = '/usr/bin/ss-local'
        proc = await asyncio.create_subprocess_exec(
            START_STOP_DAEMON,'-Sqbo',
            '-p', pidfile,
            '-n', pname,
            '-a', ssexe,
            '--','-uc',jsonfn,'-f',pidfile)
    else:
        ssexe = '/usr/bin/ss-redir'
        proc = await asyncio.create_subprocess_exec(
            START_STOP_DAEMON,'-Sqbo',
            '-p',pidfile,
            '-n', pname,
            '-a',ssexe,'--',
            '-uT','-c',jsonfn,'-f',pidfile)
    
    await proc.wait()
    await asyncio.sleep(2)
    async with aiofiles.open(pidfile,"r") as fp:
        pid = int(await fp.readline())
    await db.execute('''update  conncli_locsrv set pid = ? where id = ? ''',(pid,id))
    await db.commit()
    print(f"ss_start {server} {port} {pid}")
    await aiofiles.os.remove(pidfile)
    await aiofiles.os.remove(jsonfn)


async def connprocs_start(db,urlids):
    tasks = map(lambda r:
                asyncio.create_task(ss_start2(db,r[0],r[1])),
                urlids)
    await asyncio.gather(*tasks)
mins_per_redir = 30

async def main():
    global mins_per_redir
    async with  aiosqlite.connect("/tmp/conn.db") as db:
        db.row_factory = aiosqlite.Row
        await db.execute("PRAGMA forien_keys = ON")
        ot = time.time()
        while True:
            
            await db.commit()
            need_force = await load_db_conn_strs_from_file(db)
            procnmset=set()
            urlids = []
            for r in (await check_procs(db)):
                procnm,pid,type,row = r
                if pid is not None:
                    await killbypid(pid)
                procnmset.add(procnm)
                urlids.append((row['url'],row['conncli_locsrv_id']))
            await killbynames(procnmset)
            await connprocs_start(db,urlids)
            await perform_sock_health_checks(db,force = False)
            primary_redir_chg,_ = await choose_new_redir(db,mins_per_redir)
            if primary_redir_chg:
                print("changing primary")
                await connprocs_start(db,urlids)
                await proc_reload()
            retry_locsrv_fails = False
            prows = await perform_db_cleanups(db)
            t = time.time()
            delta = t - ot
            SECS = 10
            if delta > SECS:
                ot = t
                continue
            delta = SECS - delta
            if delta < .5:
                delta = SECS
            await asyncio.sleep( delta)
            ot = t  + delta
        
if __name__ == '__main__':
    loop = asyncio.new_event_loop()

    loop.run_until_complete(main())

"""
@Project   : 
@Module    : 
@Author    : ucphinni [ucphinni@gmail.com]
@Created   : 2023/1/12 16:36
@Desc      :
"""
import base64
import os
import re
import sqlite3
from sqlite3 import Error
import json
import aiosqlite
import asyncio
import httpx
import time
import socket
import aiofiles
import aiofiles.os
from transmission_rpc import Client
from httpx_socks import AsyncProxyTransport

PTN_IPV4 = re.compile(
    r'^((25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(25[0-5]|2[0-4]\d|[01]?\d\d?)$')
SSRBINPATTERN = re.compile(
    r'^([a-zA-Z0-9_=-]+)(.*)')
PORTPATTERN = re.compile(
    r'^(\d+)(.*)')
PLUGINPATTERN = re.compile(
    r'\?(plugin=.*(?:\#.+)?)')
PLUGIN2PATTERN = re.compile(
    r'plugin=([^\;]+)\;([^#]*?)(?:#(.*))')
REMARKPATTERN = re.compile(
    r'(?:#(.*))$')
def create_connection(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


conn = create_connection('/tmp/conn.db')
sql_create_table = """CREATE TABLE IF NOT EXISTS connurl (
                            id integer PRIMARY KEY AUTOINCREMENT NOT NULL,
                            url text NOT NULL
                        );"""
create_table(conn,sql_create_table)

sql_create_table = """
CREATE TABLE conncli_locsrv (id integer PRIMARY KEY AUTOINCREMENT NOT NULL, server text NOT NULL,
server_port integer NOT NULL, pid integer, connurl_id integer NOT NULL REFERENCES connurl (id)
ON DELETE CASCADE ON UPDATE CASCADE, local_port integer NOT NULL, UNIQUE (server, server_port) ON CONFLICT ROLLBACK);
"""
create_table(conn,sql_create_table)
sql_create_table = """
CREATE TABLE conncli_health (id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, conncli_locsrv_id
INTEGER REFERENCES conncli_locsrv (id) ON DELETE CASCADE ON UPDATE CASCADE NOT NULL, tstmp DATETIME DEFAULT
(CURRENT_TIMESTAMP) NOT NULL, status CHAR (3) NOT NULL DEFAULT ok,exception TEXT,duration INTEGER NOT NULL);
"""
create_table(conn,sql_create_table)
sql_create_table = """
CREATE TABLE conn_locsrv_type (id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, tstmp DATETIME DEFAULT
(CURRENT_TIMESTAMP) NOT NULL, conncli_locsrv_id INTEGER REFERENCES conncli_locsrv (id) ON DELETE
CASCADE ON UPDATE CASCADE, type CHAR CHECK (type IN ('P', 'S')) NOT NULL);
"""
create_table(conn,sql_create_table)

sql_create_table = """
create view if not exists conn_ss_redir as
with b as ( select 'P' type union all select 'S' )
select b.type, f.tstmp,f.conncli_locsrv_id from b
left join (
select g.* from conn_locsrv_type g,(select type, max(id) id from conn_locsrv_type group by 1) f
where g.id = f.id and f.type = g.type
) f on (f.type = b.type)
"""
create_table(conn,sql_create_table)

sql_create_table = """
create view if not exists conncli_health_last as
select 'ssc'||q.conncli_locsrv_id procname, q.* from (select ifnull(g.type,'L') type,f.* from
conncli_locsrv h
left join conn_ss_redir g on (h.id = g.conncli_locsrv_id)
left join (select f.id conncli_locsrv_id,g.duration,f.pid,f.local_port,g.tstmp,g.status,
g.exception from conncli_locsrv f
left join (select g.tstmp,g.conncli_locsrv_id,g.duration,g.status,g.exception from conncli_health g,
(select conncli_locsrv_id, max(tstmp) tstmp from conncli_health group by 1) f
where g.tstmp = f.tstmp and g.conncli_locsrv_id = f.conncli_locsrv_id) g on (f.id = g.conncli_locsrv_id))
f on (f.conncli_locsrv_id = h.id)) q
"""
create_table(conn,sql_create_table)
async def create_json_file(db,fn,server,port,sport = None):
    cur = await db.execute('''
    select c.url url,
    cu.local_port local_port
    from  conncli_locsrv cu,connurl c
    where cu.server = ? and cu.server_port = ?
    and c.id = cu.connurl_id ''',(server,port))
    row = await cur.fetchone()
    if row is None:
        return
    cfg = parse(row['url'])

    if sport is None:
        cfg['local_port'] = row['local_port']
    else:
        cfg['local_port'] = sport
        
    cfg['local_address'] = '127.0.0.1'
    
    with open(fn, 'w') as f:
        json.dump(cfg, f)


def _fill_missing(string: str):
    """Fill base64 decoded string with ="""
    missing_padding = 4 - len(string) % 4
    if missing_padding:
        return string + '=' * missing_padding
    return string


def check_ipv4(ip: str):
    """Check if the ip is valid IPV4 format"""
    if PTN_IPV4.match(ip):
        return True
    else:
        return False


def ssr_parse(url_body: str, local_address: str = '127.0.0.1', local_port: int = 1080):
    """Parse the ssr_url into dict"""
    result = {}
    try:
        url_body = _fill_missing(url_body)
        url_body = base64.urlsafe_b64decode(url_body).decode('utf8')

        config = re.split(r'[:/?&]', url_body)
        
        ip = config[0]
        port = config[1]
        protocol = config[2]
        method = config[3]
        obfs = config[4]
        password_raw = config[5]
        password_corrected = _fill_missing(password_raw)
        password_decoded = base64.urlsafe_b64decode(
            password_corrected).decode('utf8')

        # get extra param in ssr string params
        for param in config:
            matches = re.match(r"^(\w+)=(.+)$", param)
            if matches:
                key, value = matches[1],  matches[2]

                if key == "obfsparam":
                    value_decoded = base64.urlsafe_b64decode(
                        _fill_missing(value)).decode('utf8')
                    result['obfs_param'] = value_decoded
                elif key == "protoparam":
                    value_decoded = base64.urlsafe_b64decode(
                        _fill_missing(value)).decode('utf8')
                    result['protocol_param'] = value_decoded
                else:
                    result[key] = value

    except Exception as err:
        raise
    else:
        result.update({'server': ip,
                       'method': method,
                       'obfs': obfs,
                       'password': password_decoded,
                       'server_port': port,
                       'protocol': protocol,
                       'local_address': local_address,
                       'local_port': local_port,
                       })
        return result
def clear_ss(deb64):
    pos = deb64.rfind('#')
    return deb64[:pos] if pos > 0 else deb64

def fill(b64):
    return b64 + "=" * (4 - len(b64) % 4)
def ss_parse(txt: str, local_address: str = '127.0.0.1', local_port: int = 1088):
    # method:password@server:port
    result = SSRBINPATTERN.search(txt)
    encodedstr = result.group(1)
    rest = result.group(2)
    
    conf = clear_ss(bytes.decode(base64.urlsafe_b64decode(fill(encodedstr))))
    conf_list = []
    conf += rest

    for part in conf.split('@'):
        conf_list += part.split(':')
    conf_dict = dict()
    conf_dict["method"] = conf_list[0]
    conf_dict["password"] = conf_list[1]
    conf_dict["server"] = conf_list[2]
    conf_dict["server_port"] = conf_list[3]
    result = PORTPATTERN.search(conf_dict["server_port"])
    encodedstr = result.group(1)
    rest = result.group(2)
    conf_dict["server_port"] = int(encodedstr)
    result = PLUGINPATTERN.search(txt)
    if result is not None:
        txt = result.group(1)
        result = PLUGIN2PATTERN.search(txt)
        conf_dict["plugin"] = result.group(1)
        conf_dict["plugin_opts"] = result.group(2)
        conf_dict["remark"] = result.group(3)
    else:
        result = REMARKPATTERN.search(rest)
        if result is not None:
            conf_dict["remark"] =result.group(1)
    conf_dict["local_address"] = local_address
    conf_dict["local_port"] = local_port
    
    return conf_dict

def parse(txt):
    local_address='127.0.0.1'
    local_port = 0
    
    if 'ssr://' in txt:
        return ssr_parse(txt.replace('ssr://', ''),local_address,local_port)
    if 'ss://' in txt:
        return ss_parse(txt.replace('ss://', ''),local_address,local_port)
    raise Exception('ss url or ssr url format error.')

async def get_unused_socket():
    # loop = asyncio.get_running_loop()
    # server = await loop.create_server(asyncio.Protocol,'127.0.0.1,0);
    # await server.wait_closed()
    sock = socket.socket()
    sock.bind(('', 0))
    ret = sock.getsockname()[1]
    sock.close()
    return ret

START_STOP_DAEMON='/sbin/start-stop-daemon'
import psutil
ss_startid = 1
ss_lock = asyncio.Lock()
async def killbyname(name):
    for proc in psutil.process_iter():
       if proc.name() == name and proc.pid != os.getpid():
            proc.kill()

async def killbynames(names):
    for proc in psutil.process_iter():
       if proc.name() in names and proc.pid != os.getpid():
            proc.kill()

async def killbypid(pid):
    if psutil.pid_exists(pid):
        proc = psutil.Process(pid)
        proc.kill()

trc = None

async def setup_transmission_rpc():
    global trc
    settings_fn='/var/lib/transmission/config/settings.json'
    async with aiofiles.open(settings_fn) as f:
        d = json.loads(await f.read())
        username = d['rpc-username']
        password = d['rpc-password']
        trc = Client(username=username,password=password)

async def reannounce_torrents():
    global trc
    torrents = trc.get_torrents()
    for t in torrents:
        if not t.is_finished:
            trc.reannounce_torrent(t.id)

async def proc_reload():
    await reannounce_torrents()

async def proc_reload2():
    proc = await asyncio.create_subprocess_exec(
        '/etc/init.d/transmission-daemon', 'restart'
        )
    await proc.wait()

async def load_db_conn_strs_from_file(db):
    chg = False
    async with aiofiles.open("./conn.cfg","r") as fp:
        async for line in fp:
            url = line.strip()
            if (await sql_rc(db,'''select c.server,c.local_port
                 from  connurl cu, conncli_locsrv c where cu.url = trim(?)
                 and cu.id = c.connurl_id''',(line,))) is not None:
                continue # already in database.

            chg = True
            repl = False
            cfg = parse(line)
            cclsrow = await sql_rc(db,'''select * from  conncli_locsrv
                 where server = ? and server_port = ?''',
                             (cfg['server'],cfg['server_port']))
            if cclsrow is not None and cclsrow['pid'] is not None:
                await connproc_stop(db,cfg['server'],cfg['server_port'])
            if cclsrow is not None:
                # delete as it will be recreated later.
                print (f"deleted {cclsrow['id']}")
                await db.execute ('''delete from conncli_locsrv where id = ? ''',(cclsrow['id'],))
                
            url = line
            cur = await db.execute('''select 1 as res from  connurl where url = ?''',
                                   (line,))
            row = await cur.fetchone()
            if row is not None:
                continue # already there... skip
            
            await db.execute('''insert into connurl (url) VALUES(?)''',(line,))

            cur = await db.execute('''select id from  connurl where url = ?''',
                                   (line,))
            row = await cur.fetchone()
            if row is None:
                raise Exception("no connurl")
            connid = row['id']
            cfg['local_port'] = await get_unused_socket()
            
            await db.execute('''insert into conncli_locsrv (connurl_id,server,server_port,local_port)
            VALUES(?,?,?,?)''',(connid,cfg['server'],cfg['server_port'],cfg['local_port']))
    return chg            


async def log_conn_health(db,id, status,dur,exception=None):
    exception = str(exception) if exception is not None else None
    await db.execute ('''
       insert into conncli_health (conncli_locsrv_id,status,exception,duration)
       VALUES (?,?,?,?)
    ''', (id,status,exception,dur))
    
async def ss_socks_health(db,id,isp_extip = None,force = False):
    global mins_per_redir
    if force:
        cur = await db.execute('''
select 1  res,ccls.local_port from conncli_health_last ch,conncli_locsrv ccls
        where ch.conncli_locsrv_id = ccls.id and
        ccls.id = ? order by 1 desc limit 1
''', (id,))
    else:
        cur = await db.execute('''
with b as (
select min(duration)*1.2 dur from conncli_health_last
)
select current_timestamp > datetime(ch.tstmp,case when 
(ch.duration < b.dur or (select count(*) < 2 from conncli_health_last,b where duration < b.dur limit 2))
and ch.status != 'ok' and 2 > ifnull((select count(*) from conncli_health_last where status == 'ok'
        limit 2),0) or ch.status == 'ok' then 
 case when exists (
     with a as (select ? mins_interval,10000000 random_steps, 
        CURRENT_TIMESTAMP now, DATETIME(tstmp) start from conncli_health_last where type = 'P'),
     c as (select (cos((julianday(a.now) - julianday(a.start))*(24*60.0/a.mins_interval)* 2 * pi()) +1)/2 prob from a)
     select 1 from a,c where c.prob >= ((abs(random())) % (  a.random_steps)*1.0)/a.random_steps 
) then
    '+1 second'
 else '+15 minutes'
 end 
 else '+2 hour' end)
 res,ccls.local_port from conncli_health_last ch,b,conncli_locsrv ccls
        where ch.conncli_locsrv_id = ccls.id and ccls.id = ? order by 1 desc limit 1
''', (mins_per_redir,id))
    row = await cur.fetchone()

    if row is None or row['res'] != 0:
        pass
    else:
        return
    port = row['local_port']

    t = time.time()
    try:

        print (f"health check {id}")
        transport = AsyncProxyTransport.from_url(f'socks5://127.0.0.1:{port}')
        async with httpx.AsyncClient(transport=transport) as client:
            response = await client.get('http://ifconfig.me')
            ret = response.text
            if ret is None or ret == '':
                await log_conn_health(db,id,"nip",time.time()- t,"No IP from external host")
                return
            if isp_extip is not None and ret == isp_extip:
                await log_conn_health(db,id,"eip",time.time()- t,"Exposed IP")
                return
            await log_conn_health(db,id,"ok",time.time()- t,None)
    except Exception as e:
        await log_conn_health(db,id,"err",time.time()- t,str(e))
    finally:
        print (f"health check {id} done")

async def perform_sock_health_checks(db,force):

    cur = await db.execute('''
        select id
        from conncli_locsrv ccls where ccls.pid is not NULL
         and not exists (
            select 1 from conn_ss_redir where ccls.id = conncli_locsrv_id)''')
    rows = await cur.fetchall()
    tasks = map(lambda x:
                asyncio.create_task(ss_socks_health(
                    db,x['id'],force=force)),
                rows)
    await asyncio.gather(*tasks)
async def perform_db_cleanups(db):
    await db.execute('''
    delete from conncli_health where id in (
    select g.id from conncli_health g, (
    select id,conncli_locsrv_id,tstmp,
    row_number() over (partition by conncli_locsrv_id
    order by tstmp desc) recnum
    from conncli_health
    ) f where f.recnum > 20 and g.id = f.id
    )
    ''')

async def choose_new_redir(db,mins):
    sql = f"""
    select * from conncli_health_last where status = 'ok'
    """
    cur = await db.execute(sql)
    rows = await cur.fetchone()
    ret = not( rows is not None and len(rows) >= 2)
    sql = f"""
with b as ( select 'P' type union all select 'S' )
select b.type,current_timestamp now,
    datetime(f.tstmp,'+{mins} minutes') future,
    ifnull(current_timestamp > datetime(f.tstmp, 
    '+{mins} minutes') ,2) need_another, f.tstmp,
    f.conncli_locsrv_id from b
    left join (
    select g.* from conn_locsrv_type g,(select type, max(tstmp) tstmp
    from conn_locsrv_type group by 1) f
    where g.tstmp = f.tstmp and f.type = g.type
    ) f on (f.type = b.type)
"""
    cur = await db.execute(sql)
    rows = await cur.fetchall()
    if not any(map(lambda r:r['need_another'],rows)):
        return False,False
    sql = '''
        select * from conncli_health_last 
            where status = 'ok' order by type desc limit 3;
'''
    cur = await db.execute(sql)
    rows = await cur.fetchall()
    rows.reverse()
    x,y = None,None
    if rows is None or len(rows) == 0:
        pass
    elif len(rows) == 3 or len(rows) == 2:
        x = next(filter(lambda r:r['type'] != 'P' ,rows),None)
        y = next(filter(lambda r:r['conncli_locsrv_id'] != x['conncli_locsrv_id'],rows),None)
    elif len(h) == 1:
        x,y = rows[0],None
    sql = '''
        select * from conncli_health_last where type in ('P','S') order by type
'''
    if x is not None:
        x = x['conncli_locsrv_id']
    if y is not None:
        y = y['conncli_locsrv_id']
    await db.commit()
    cur = await db.execute(sql)
    oldrows = await cur.fetchall()
    await db.execute('insert into conn_locsrv_type(conncli_locsrv_id,type) values (?,?)',
        (x,'P'))
    await db.execute('insert into conn_locsrv_type(conncli_locsrv_id,type) values (?,?)',
        (y,'S'))
    if len(oldrows) == 0:
        return True,True
    pna = oldrows[0]['conncli_locsrv_id'] != x
    sna = oldrows[1]['conncli_locsrv_id'] != y
    return pna,sna 

async def sql_rc(db,sql,t=None):
    cur = await db.execute(sql,t)
    if cur is None:
        return None
    return await cur.fetchone()

def check_ss_type_from_pid(pid):
    proc = psutil.Process(pid)
    ss_redir_flg = 'ss-redir' in proc.exe()
    ss_local_flg = 'ss-local' in proc.exe()
    type = None
    if ss_redir_flg:
        type = 'ss-redir'
    if ss_local_flg:
        type = 'ss-local'
    return type


async def check_procs(db):
    cur = await db.execute('''
              select u.url,c.* from conncli_health_last c,
                connurl u,conncli_locsrv cl
              where c.conncli_locsrv_id = cl.id and cl.connurl_id = u.id
    ''')
    if cur is None:
        return []
    n2p={}
    for proc in psutil.process_iter():
       if not proc.cmdline() or not proc.name().startswith('ss-'):
           continue
       n2p[proc.cmdline()[0]] = proc.pid
    

    rows = await cur.fetchall()
    ret = []
    for r in rows:
        proctype = None
        type = 'ss-redir' if r['type'] != 'L' else 'ss-local'
        procnm = r['procname']
        if procnm in n2p:
            proctype = check_ss_type_from_pid( n2p[procnm])
        procnm_pid = n2p[procnm] if procnm in n2p else None

        # print ("::",procnm,type,proctype, procnm_pid)
        if type != proctype or procnm not in n2p:
            ret.append((procnm,procnm_pid,type,r))
    
    return ret

async def ss_start2(db,url,id):
    global ss_startid
    startid = None

    cur = await db.execute('''
select c.procname,c.type,cl.server,cl.server_port,case when c.type = 'P' then 1088 when c.type = 'S'
    then 1089 else cl.local_port end local_port from conncli_health_last c,conncli_locsrv cl
    where c.conncli_locsrv_id = cl.id and cl.connurl_id = ?
 ''',(id,))
    cclsrow = await cur.fetchone()
    if cclsrow is None:
        raise Exception("No srv set")
    ss_type = 'ss-local' if cclsrow['type'] == 'L' else 'ss-redir'

    async with ss_lock:
        startid = ss_startid
        ss_startid += 1

    pidfile=f"/tmp/sss{startid}.pid"
    jsonfn=f"/tmp/sss{startid}.json"
    server = cclsrow['server']
    port   = cclsrow['server_port']
    sport  = cclsrow['local_port']
    pname  = cclsrow['procname']
    
    await create_json_file(db,jsonfn,server,port,sport)
    if cclsrow['type'] == 'L':
        ssexe = '/usr/bin/ss-local'
        proc = await asyncio.create_subprocess_exec(
            START_STOP_DAEMON,'-Sqbo',
            '-p', pidfile,
            '-n', pname,
            '-a', ssexe,
            '--','-uc',jsonfn,'-f',pidfile)
    else:
        ssexe = '/usr/bin/ss-redir'
        proc = await asyncio.create_subprocess_exec(
            START_STOP_DAEMON,'-Sqbo',
            '-p',pidfile,
            '-n', pname,
            '-a',ssexe,'--',
            '-uT','-c',jsonfn,'-f',pidfile)
    
    await proc.wait()
    await asyncio.sleep(2)
    async with aiofiles.open(pidfile,"r") as fp:
        pid = int(await fp.readline())
    await db.execute('''update  conncli_locsrv set pid = ? where id = ? ''',(pid,id))
    await db.commit()
    print(f"ss_start {server} {port} {pid}")
    await aiofiles.os.remove(pidfile)
    await aiofiles.os.remove(jsonfn)


async def connprocs_start(db,urlids):
    tasks = map(lambda r:
                asyncio.create_task(ss_start2(db,r[0],r[1])),
                urlids)
    await asyncio.gather(*tasks)
mins_per_redir = 30

async def main():
    global mins_per_redir
    await setup_transmission_rpc()
    async with  aiosqlite.connect("/tmp/conn.db") as db:
        db.row_factory = aiosqlite.Row
        await db.execute("PRAGMA forien_keys = ON")
        ot = time.time()
        while True:
            
            await db.commit()
            need_force = await load_db_conn_strs_from_file(db)
            procnmset=set()
            urlids = []
            for r in (await check_procs(db)):
                procnm,pid,type,row = r
                if pid is not None:
                    await killbypid(pid)
                procnmset.add(procnm)
                urlids.append((row['url'],row['conncli_locsrv_id']))
            await killbynames(procnmset)
            await connprocs_start(db,urlids)
            await perform_sock_health_checks(db,force = False)
            primary_redir_chg,_ = await choose_new_redir(db,mins_per_redir)
            if primary_redir_chg:
                print("changing primary")
                await connprocs_start(db,urlids)
                await proc_reload()
            retry_locsrv_fails = False
            prows = await perform_db_cleanups(db)
            t = time.time()
            delta = t - ot
            SECS = 10
            if delta > SECS:
                ot = t
                continue
            delta = SECS - delta
            if delta < .5:
                delta = SECS
            await asyncio.sleep( delta)
            ot = t  + delta
        
if __name__ == '__main__':
    loop = asyncio.new_event_loop()

    loop.run_until_complete(main())
