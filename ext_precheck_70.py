#!/usr/bin/python
# v.1.2.002

import atexit, sys, os, shutil, codecs, deployment, install_routines, optparse, time, logging
from poaupdater import uConfig, uLogging, uSysDB, uPEM, uPrecheck, uUtil, openapi, uHCL, uBilling

def diskspace():
	if 'diskspace' in skip: return
	logging.info('\n\t============== Checking free space on all nodes (Free space > 1GB) ==============\n')
	
	free_space = 1 # GB

	cur.execute("select host_id, primary_name from hosts where pleskd_id>0 and htype not in ('w','e')")
	for row in cur.fetchall():
		host_id = row[0]
		name = row[1]
		logging.info("Checking free disk space on %s" % name)
		try:
			res = uPEM.check_free_disk_space(host_id, free_space)
			if res is None:
				logging.info("Result:\t[  OK  ]")
		except Exception, e:
			logging.info("%s\n" % str(e))
			continue

def ui_resources():
	if 'uires' in skip: return
	logging.info("\n\t============== Checking UI/MN nodes resources ==============")
	
	cur.execute("select p.host_id, primary_name from proxies p, hosts h where h.host_id=p.host_id and h.htype != 'e'")
	for row in cur.fetchall():
		host_id = row[0]
		name = row[1]
		logging.info("\nHost #%s %s:" % (str(host_id),name))
		request = uHCL.Request(host_id, user='root', group='root')
		request.command('grep -c processor /proc/cpuinfo', stdout='stdout', stderr='stderr', valid_exit_codes=[0])
		try:
			cpus = request.perform()['stdout'].rstrip()
			if int(cpus) < 4:
				logging.info("CPUs:\t%s Cores\t\t[  FAILED  ]\t Minimum requirement for UI 4 Cores and 8GB RAM, Please see the requirements for CPUs/RAM at http://download.automation.odin.com/oa/7.0/oapremium/portal/en/hardware_requirements/60434.htm" % cpus)
			else:
				logging.info("CPUs:\t%s Cores\t\t[  OK  ]" % cpus)
			request.command('grep MemTotal /proc/meminfo | grep -o [0-9]*', stdout='stdout', stderr='stderr', valid_exit_codes=[0])
			mem = request.perform()['stdout']
			mem = int(mem)/1000000
			if mem < 8:
				logging.info("RAM:\t%s GB\t\t[  FAILED  ]\t Minimum requirement for UI 4 Cores and 8GB RAM, Please see the requirements for CPUs/RAM at http://download.automation.odin.com/oa/7.0/oapremium/portal/en/hardware_requirements/60434.htm" % str(mem))
			else:
				logging.info("RAM:\t%s GB\t\t[  OK  ]" % str(mem))
		except Exception, e:
			logging.info("pa-agent failed...please check poa.log on the node\n %s\n" % str(e))
			continue

def mem_winnodes():
	if 'memwin' in skip: return
	logging.info('\n\t============== Checking free memory on WIN nodes (at risk if less 500MB )==============\n')

	cur.execute("select count(1) from hosts where pleskd_id>0 and htype = 'w'")
	logging.info("Number of Win Nodes: %s" % cur.fetchone()[0])
	cur.execute("select host_id, primary_name from hosts where pleskd_id>0 and htype = 'w'")
	for row in cur.fetchall():
		host_id = row[0]
		name = row[1]
		logging.info("Host #%s %s" % (str(host_id),name))
		request = uHCL.Request(host_id, user='root', group='root')
		request.command('systeminfo |find "Available Physical Memory"', stdout='stdout', stderr='stderr', valid_exit_codes=[0])
		try:
			logging.info(request.perform()['stdout'])
		except Exception, e:
			logging.info("pa-agent failed...please check poa.log on the node\n %s\n" % str(e))
			continue

def uiprox_misconf():
	if 'uiprox' in skip: return
	logging.info("\n\t============== Checking UI proxies misconfigs in oss DB ==============\n")
	
	cur.execute("select brand_id,proxy_id from brand_proxy_params")
	for row in cur.fetchall():
		brand_id = row[0]
		proxy_id = row[1]
		
		cur.execute("select 1 from proxies where proxy_id = "+str(proxy_id))
		if cur.fetchone() is None:
			logging.info("Checking Brand #%s:\tproxy #%s\t[  FAILED  ]" % (str(brand_id),str(proxy_id)))
		else:
			logging.info("Checking Brand #%s:\tproxy #%s\t[  OK  ]" % (str(brand_id),str(proxy_id)))

def rsync():
	if 'rsync' in skip: return
	logging.info("\n\t============== Checking rsync on NS nodes ==============\n")

	cur.execute("select s.host_id,primary_name from services s, hosts h where s.name = 'bind9' and h.host_id = s.host_id")
	for row in cur.fetchall():
		host_id = row[0]
		host_name = row[1]
		logging.info("Host #%s %s:" % (str(host_id),host_name))
		request = uHCL.Request(host_id, user='root', group='root')
		request.command("rpm -q rsync", stdout='stdout', stderr='stderr', valid_exit_codes=[0])
		try:
			logging.info(request.perform()['stdout'])
		except Exception, e:
			logging.info("pa-agent failed...please check poa.log on the node\n %s\n" % str(e))
			continue

def mess_bodies():
	if 'messg' in skip: return
	logging.info("\t============== Checking empty message_bodies ==============\n")
	
	cur.execute("select length(message_body) from message_bodies")
	for row in cur.fetchall():
		if row[0] is None:
			logging.info("Result:\t[  FAILED  ]\tempty message_bodies")
		else:
			logging.info("Result:\t[  OK  ]")
		
def yum_repos():
	if 'yum' in skip: return
	logging.info("\n\t============== Checking YUM repos on all nodes ==============\n")

	cur.execute("select host_id, primary_name from hosts where pleskd_id>0 and htype not in ('w','e')")

	for row in cur.fetchall():
		host_id = row[0]
		name = row[1]
		logging.info("Host #%s %s:" % (str(host_id),name))
		request = uHCL.Request(host_id, user='root', group='root')
		request.command('yum clean all && yum repolist', stdout='stdout', stderr='stderr', valid_exit_codes=[0])
		try:
			logging.info(request.perform()['stdout'])
		except Exception, e:
			logging.info("pa-agent failed...please check poa.log on the node\n %s\n" % str(e))
			continue

def num_resources():
	if 'numres' in skip: return
	logging.info("\n\t============== Checking number of accounts/users/subs/oa-db size ==============\n")
	
	cur.execute("select count(1) from accounts")
	logging.info("Number of Accounts: %s (If Num > 20K: add additional 1H for every 20K)" % cur.fetchone()[0])
	cur.execute("select count(1) from users")
	logging.info("Number of Users: %s" % cur.fetchone()[0])
	cur.execute("select count(1) from subscriptions")
	logging.info("Number of Subscriptions: %s (If Num > 20K: add additional 1H for every 20K)" % cur.fetchone()[0])
	cur.execute("SELECT rt.restype_name, count(sr.rt_id) as subs, sum(sr.curr_usage) as usage FROM subs_resources sr, resource_types rt WHERE rt.rt_id = sr.rt_id and sr.rt_id not in (select rt_id from resource_types where class_id in (select class_id from resource_classes where name in ('DNS Management','disc_space','traffic','ips','shared_hosting_php','apache_ssl_support','apache_name_based','apache_lve_cpu_usage','proftpd.res.ds','proftpd.res.name_based','rc.saas.resource','rc. saas.resource.mhz','rc.saas.resource.unit','rc .saas.resource.kbps','rc.saas.resource.mbh','rc.saas.resource.mhzh','rc.saas.resource.unith'))) GROUP by rt.restype_name having sum(sr.curr_usage) > 0 ORDER by 2 desc")
	logging.info("Resources Number:\n--------+---------+---------------------------\n subs	| usage	| restype_name\n--------+---------+---------------------------")
	for row in cur.fetchall():
		logging.info(" %s	| %s	| %s " % (row[1], row[2], row[0]))
	cur.execute("SELECT pg_size_pretty(pg_database_size(current_database()))")
	logging.info("\nOA DB size: %s (If size > 15GB: add additional 1H for every 10GB)\n\nShow most fragmented tables and last vacuum time:" % cur.fetchone()[0])
	cur.execute("select a.relname,n_dead_tup, to_char(last_vacuum, 'YYYY-MM-DD HH24:MI') as last_vacuum, to_char(last_autovacuum, 'YYYY-MM-DD HH24:MI') as last_autovacuum, pg_size_pretty(pg_table_size(b.oid)) as size from pg_stat_all_tables a, pg_class b where a.relname = b.relname order by n_dead_tup desc limit 10")
	logging.info("------------------------+---------------+-------------------------------+-------------------------------+---------\n relname             | n_dead_tup    | last_vacuum\t\t\t| last_autovacuum\t| size\n------------------------+---------------+-------------------------------+-------------------------------+---------")
	for row in cur.fetchall():
		tab1 = tab2 = tab3 = ''
		if      len(str(row[0])) < 7: tab1 = '\t\t'
		elif    len(str(row[0])) < 15: tab1 = '\t'
		if		len(str(row[2])) < 10: tab2 = '\t\t'
		if		len(str(row[3])) < 10: tab3 = '\t\t'
		logging.info(" %s%s    | %s            | %s%s  | %s%s  | %s " % (row[0],tab1, row[1], row[2],tab2, row[3],tab3, row[4]))
		
def ba_res():
	if 'ba' in skip: return
	logging.info('\n\t************************************ Checking BA resources ************************************\n')
	
	if not os.path.isfile("poaupdater.tgz"):
		os.system("tar -zcf poaupdater.tgz poaupdater")
	dir_path = os.path.dirname(os.path.realpath(__file__))
	lpath = dir_path+'/poaupdater.tgz'
	bmpath = dir_path+'/bmcheck.py'
	cur.execute("select host_id from hosts where host_id in (select host_id from components where pkg_id in (select pkg_id from packages where name='PBAApplication'))")
	ba_host_id = cur.fetchone()[0]
	
	request = uHCL.Request(ba_host_id, user='root', group='root')
	request.transfer('1', bmpath, '/usr/local/bm/tmp/')
	request.transfer('1', lpath, '/usr/local/bm/tmp/')
	request.extract('/usr/local/bm/tmp/poaupdater.tgz', '/usr/local/bm/tmp/')
	request.command('python /usr/local/bm/tmp/bmcheck.py', stdout='stdout', stderr='stderr', valid_exit_codes=[0,1])
	logging.info(request.perform()['stdout'])

parser = optparse.OptionParser()
parser.add_option("-s", "--skip", metavar="skip", help="phase to skip: diskspace,uires,uiprox,memwin,rsync,yum,numres,messg,ba")
parser.add_option("-l", "--log", metavar="log", help="path to log file, default: current dir")
opts, args = parser.parse_args()
skip = opts.skip or ''

filename = time.strftime("/ext_precheck-%Y-%m-%d-%H%M.txt", time.localtime())
logfile = opts.log or os.path.abspath(os.path.dirname(__file__)) + filename

logging.basicConfig(
	level=logging.DEBUG,
    format='%(message)s',
    datefmt='%m-%d %H:%M',
    filename=logfile,
    filemode='w')

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

con = uSysDB.connect()
cur = con.cursor()

num_resources()
diskspace()
ui_resources()
mem_winnodes()
uiprox_misconf()
rsync()
mess_bodies()
ba_res()
yum_repos()

logging.info("\nlog saved to: %s\n" % logfile)