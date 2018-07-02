##########################################################################################################################
#                                                                                                                        #
#                                                                                                                        #
#                                                                                                                        #
#                                                                                                                        #
#                                                                                                                        #
#                                                                                                                        #
#                                                                                                                        #
#                                                                                                                        #
#                                                                                                                        #
##########################################################################################################################

import os, sys, glob
import time, datetime
import re
import psycopg2
from sys import argv
from time import sleep

#######Globals##############
PARAMLIST={}
CMDLINEARGS={}

DBCONN=None
DBUSER="postgres"
DBPASS="postgres"
DBHOST="localhost"
DBNAME="postgres"
MODE="EXTRACT"
POLLINTERVAL=5
EXTRPATH=""
FILEPREFIX="EXTR"
REPSLOTNAME=""
TABLES=""


############################
def applyloop(pinterval):
	pintr=5
	curr_file = ""
	while 1:
		if not re.search("^[0-9]+$",pinterval):
			print (now(),"ERROR:[APPLYLOOP]", "POLLINTERVAL='"+pinterval+"'","should only be an integer, setting interval to 5 minutes",flush=True)
			pintr=5
		else:
			pintr=int(pinterval) 

		try:	
			if len(EXTRPATH) == 0:
				epath=os.getcwd()
			else:
				epath=EXTRPATH

			filelist=glob.glob(epath+ os.sep +FILEPREFIX+"*")
			
			dlines=[]
			for file in filelist:
				curr_file = file
				with open(file,'rt') as f:
					dlines=f.readlines()
				
				for line in dlines:
					if line[:3] not in  ('BEG','COM'):
						parsetree=parseline(line)
						SQL,datalist=buildsql(parsetree)
						runsql(SQL,datalist)
					#print("PARSE",line)
				
				filename=os.path.basename(file)
				dirname=os.path.dirname(file)
				os.rename(file,dirname + os.sep + ".applied."+filename)
			sleep(float(pintr * 60))
		except:
			e=sys.exc_info()
			print (now(),"ERROR:[APPLYLOOP]", e[1],flush=True)
			print (now(),"DEBUG:[APPLYLOOP]","EXTRFILE="+curr_file,flush=True)
			exit()
			
def extractloop(pinterval):
	while 1:
		Qsize = 0
		rep_slot = REPSLOTNAME

		if not re.search("^[0-9]+$",pinterval):
			print (now(),"ERROR:[EXTRACTLOOP]", "POLLINTERVAL='"+pinterval+"'","should only be an integer, setting interval to 5 minutes",flush=True)
			pintr=5
		else:
			pintr=int(pinterval) 

		Qsize = peekrepslot(rep_slot)
		
		if Qsize > 0:
			print (now(),"MESSAGE:[EXTRACTLOOP]","QSIZE="+str(Qsize)+", REPSLOTNAME="+REPSLOTNAME+", PINTR="+str(float(pintr * 60)),flush=True)
			extractandwrite(rep_slot)
			
		try:	
			sleep(float(pintr * 60))
		except:
			e=sys.exc_info()
			print (now(),"ERROR:[EXTRACTLOOP]", e[1],flush=True)
			exit()
			
def peekrepslot(rep_slot):
	try:
		dbconn = dbconnect(DBUSER,DBPASS,DBHOST,DBNAME)
		cursor = dbconn.cursor()
		cursor.execute("select distinct xid FROM pg_logical_slot_peek_changes('"+rep_slot+"', NULL, NULL)")
		records = cursor.fetchall()
		dbconn.close()
		return len(records)
	except:
		print (now(),"ERROR:[PEEKREPSLOT]",sys.exc_info()[1],flush=True)
		exit()
		
def extractandwrite(rep_slot):
	try:
		conn = dbconnect(DBUSER,DBPASS,DBHOST,DBNAME)
		
		cursor_xids = conn.cursor()
		cursor_xids.execute("select distinct xid FROM pg_logical_slot_peek_changes('"+rep_slot+"', NULL, NULL)")
		
		xids = cursor_xids.fetchall()
		print (now(),"MESSAGE:[EXTRACTANDWRITE] PEEKEDRECORDS=",len(xids),flush=True)
		cursor_xids.close()
		
		for xid in xids:
			
			cursor_peek = conn.cursor()
			cursor_peek.execute("select xid,data FROM pg_logical_slot_peek_changes('"+rep_slot+"', NULL, NULL) where xid = " + str(xid[0]))
			records = cursor_peek.fetchall()

			if len(EXTRPATH) == 0:
				epath=os.getcwd()
			else:
				epath=EXTRPATH

			with open(epath+ os.sep + FILEPREFIX + "." + str(xid[0]),"w") as f:	
				for row in records:
					ptree={}
					line=row[1]
					if line[:3] not in  ('BEG','COM'):
						ptree=parseline(line)
						###print("LINE:",line,"ROW:",row[1],"PTREE:",ptree)
						TBLLIST=TABLES.upper().strip().split(",")
						##print(ptree['OBJNAME'].upper().strip(),TBLLIST)
						if (ptree['OBJNAME'].upper().strip() in TBLLIST):
							f.write(row[1])
							f.write("\n")

			cursor_peek.close()
			
		cursor_get  = conn.cursor()
		cursor_get.execute("select xid,data  FROM pg_logical_slot_get_changes('"+rep_slot+"', NULL, NULL) where xid = " + str(xid[0]))
		print (now(),"MESSAGE:[EXTRACTANDWRITE] DEQUEUED XID=", xids,flush=True)
		cursor_get.close()
		
		conn.close()
	except:
		print (now(),"ERROR:[EXTRACTANDWRITE]",sys.exc_info()[1],flush=True)
		conn.close()
		exit()
		
def parseline(line):
	parsetree = {}
	try:	
		tokens = line.split(":")
		
		tokens.reverse()
		OBJTEMP=tokens.pop()
		OPNAME=tokens.pop()
		OBJ,OBJNAME=OBJTEMP.split()

		OBJTEMP=OBJTEMP.strip()
		OPNAME=OPNAME.strip()
		OBJ=OBJ.strip()
		OBJNAME=OBJNAME.strip()

		tokens.reverse()
		DATASTRING = ":".join(tokens)
		
		
		parsetree['OBJECT']=OBJ
		parsetree['OBJNAME']=OBJNAME
		parsetree['OPNAME']=OPNAME
		
		parsetree['DATASTRING']=DATASTRING
	except:
		print (now(),"ERROR:[PARSELINE]",sys.exc_info()[1],flush=True)
		pass
		
	return parsetree
	
		
def buildsql(parsetree):
	try:
		OPNAME=parsetree['OPNAME']
		OBJ=parsetree['OBJECT']
		OBJNAME=parsetree['OBJNAME']

		schema,table = OBJNAME.split(".")
		
		conn = dbconnect(DBUSER,DBPASS,DBHOST,DBNAME)
		cur=conn.cursor()
		cur.execute("select column_name || '[' || (case when data_type = 'USER-DEFINED' then udt_name else data_type end) || ']:' from information_schema.columns where table_name='"+table+"' and table_schema='"+schema+"'")
		coldefs = cur.fetchall()

		datastring = parsetree['DATASTRING']
		s=datastring
		
		datalist = {}
		for c in range(len(coldefs)):
			##print(c,coldefs[c])
			if c == (len(coldefs)-1):
					datalist[coldefs[c][0]]=s[s.find(" "+coldefs[c][0])+len(" "+coldefs[c][0]):]
					##print("S:",s[s.find(" "+coldefs[c][0])+len(" "+coldefs[c][0]):])
			else:
					datalist[coldefs[c][0]]=s[s.find(" "+coldefs[c][0])+len(" "+coldefs[c][0]):s.find(" "+coldefs[c+1][0])]
					##print("S:",s[s.find(" "+coldefs[c][0])+len(" "+coldefs[c][0]):s.find(" "+coldefs[c+1][0])])
		
		##print("DATALIST:",datalist)
		SQL,dlist=_buildstmt(OPNAME,OBJ,OBJNAME,datalist)	
		##print(SQL)
		return [SQL,dlist]
	except:
		print (now(),"ERROR:[BUILDSQL]",sys.exc_info()[1],flush=True)
		print (now(),"DEBUG:[BUILDSQL]",OBJ,OBJNAME+":",OPNAME+":",datastring,flush=True)
		pass

def _buildstmt(OPNAME,OBJ,OBJNAME,datalist):
	switcher = {
		"INSERT": _INSERT, "UPDATE": _UPDATE, "DELETE": _DELETE
	}
	func = switcher.get(OPNAME, lambda: "")
	if func is not None:
		SQL=func(OBJ,OBJNAME,datalist)
		return SQL
	
def _INSERT(OBJ,OBJNAME,datalist):
	try:
		schema,table = OBJNAME.split(".")
		
		conn = dbconnect(DBUSER,DBPASS,DBHOST,DBNAME)
		cur=conn.cursor()
		cur.execute("select column_name,(case when data_type = 'USER-DEFINED' then udt_name else data_type end) data_type from information_schema.columns where table_name='"+table+"' and table_schema='"+schema+"'")
		coldefs = cur.fetchall()
		
		dlist = {}
		datastr="("
		colstr="("
		for col in range(len(coldefs)):
			##print("{INS}DATAITM:",datalist[coldefs[col][0] + "[" + coldefs[col][1] + "]:"].strip())
			dlist[coldefs[col][0]]=datalist[coldefs[col][0] + "[" + coldefs[col][1] + "]:"].strip()
			
			if dlist[coldefs[col][0]][0]==dlist[coldefs[col][0]][len(dlist[coldefs[col][0]])-1]=="'":
				tmpstr=dlist[coldefs[col][0]]
				dlist[coldefs[col][0]]=tmpstr[1:len(tmpstr)-1]
			
			##print("{INS}DATAITM:",dlist)
			
			if dlist[coldefs[col][0]].lower()=='null':
				dlist[coldefs[col][0]]=None
			
			if col == (len(coldefs)-1):
				datastr += "%("+coldefs[col][0]+")s"
				colstr += coldefs[col][0]
			else:
				datastr += "%("+coldefs[col][0]+")s,"
				colstr += coldefs[col][0] + ","
		
		#print("DLIST=",dlist)
		######input("DEBUG:")
		datastr += ")"
		colstr += ")"
		
		SQL = "INSERT into " + OBJNAME + " " + colstr + " values " + datastr
		##print (now(),"ERROR:[_INSERT]",SQL,dlist,flush=True)
		return [SQL,dlist]
		
	except:
		print (now(),"ERROR:[_INSERT]",sys.exc_info()[1],flush=True)
		return None
		
def _UPDATE(OBJ,OBJNAME,datalist):
	try:
		schema,table = OBJNAME.split(".")
		
		conn = dbconnect(DBUSER,DBPASS,DBHOST,DBNAME)
		cur=conn.cursor()
		cur.execute("select column_name,(case when data_type = 'USER-DEFINED' then udt_name else data_type end) data_type  from information_schema.columns where table_name='"+table+"' and table_schema='"+schema+"'")
		coldefs = cur.fetchall()
		
		dlist = {}
		dtyplst = {}
		found=0
		datastr=""
		for col in range(len(coldefs)):
			found=datalist[coldefs[col][0] + "[" + coldefs[col][1] + "]:"].strip().find(" new-tuple: "+coldefs[col][0] + "[" + coldefs[col][1] + "]:")
			##print("{UPD}DATAITM:",datalist[coldefs[col][0] + "[" + coldefs[col][1] + "]:"].strip())
			dlist[coldefs[col][0]]=datalist[coldefs[col][0] + "[" + coldefs[col][1] + "]:"].strip()
			dtyplst[coldefs[col][0]]=coldefs[col][1]

			dlist[coldefs[col][0]]=datalist[coldefs[col][0] + "[" + coldefs[col][1] + "]:"].strip()
			
			if dlist[coldefs[col][0]][0]==dlist[coldefs[col][0]][len(dlist[coldefs[col][0]])-1]=="'":
				tmpstr=dlist[coldefs[col][0]]
				dlist[coldefs[col][0]]=tmpstr[1:len(tmpstr)-1]
			
			##print("{UPD}DATAITM:",dlist)
			
			if dlist[coldefs[col][0]].lower()=='null':
				dlist[coldefs[col][0]]=None  ## Setting value to None will automatically set the bind variable to NULL

			if col == (len(coldefs)-1):
				if found > 0:
					datastr += coldefs[col][0] + " = %(NEWKEYVAL"+coldefs[col][0]+")s"
				else:
					datastr += coldefs[col][0] + " = %("+coldefs[col][0]+")s"
			else:
				if found > 0:
					datastr += coldefs[col][0] + " = %(NEWKEYVAL"+coldefs[col][0]+")s,"
				else:
					datastr += coldefs[col][0] + " = %("+coldefs[col][0]+")s,"
		
		cur.execute("""
		select tc.table_schema, tc.table_name, kc.column_name
		from
			information_schema.table_constraints tc,
			information_schema.key_column_usage kc
		where
			tc.constraint_type = 'PRIMARY KEY'
			and kc.table_name = tc.table_name and kc.table_schema = tc.table_schema
			and kc.constraint_name = tc.constraint_name
			and tc.table_name=%s
			and tc.table_schema=%s
		order by 1,2
		""",
		(table,schema)
		)

		row = cur.fetchall()
		
		pkcol = row[0][2]
		
		found=datalist[pkcol + "[" + dtyplst[pkcol] + "]:"].strip().find(" new-tuple: "+pkcol + "[" + dtyplst[pkcol] + "]:")
		pkeyval=datalist[pkcol + "[" + dtyplst[pkcol] + "]:"].strip()[:datalist[pkcol + "[" + dtyplst[pkcol] + "]:"].strip().find(" new-tuple: "+pkcol + "[" + dtyplst[pkcol] + "]:")]
		tmpstr=dlist[pkcol]
		pknewval=tmpstr[len(pkeyval + " new-tuple: "+pkcol + "[" + dtyplst[pkcol] + "]:"):]
		
		
		
		if (len(pkeyval.strip())>0) and (found > 0):
			dlist[pkcol]=pkeyval
			dlist["NEWKEYVAL"+pkcol]=pknewval
		
		whereclause = pkcol + "=" + "%("+ pkcol +")s"
		
		
		SQL = "UPDATE " + OBJNAME + " SET " + datastr + " WHERE " + whereclause
		##print (now(),"ERROR:[_UPDATE]",SQL,dlist,flush=True)
		##SQL=""
		return [SQL,dlist]
		
	except:
		print (now(),"ERROR:[_UPDATE]",sys.exc_info()[1],flush=True)
		return None

def _DELETE(OBJ,OBJNAME,datalist):
	try:
		schema,table = OBJNAME.split(".")

		conn = dbconnect(DBUSER,DBPASS,DBHOST,DBNAME)
		cur=conn.cursor()
		cur.execute("select column_name,(case when data_type = 'USER-DEFINED' then udt_name else data_type end) data_type  from information_schema.columns where table_name='"+table+"' and table_schema='"+schema+"'")
		coldefs = cur.fetchall()
		
		dlist = {}
		datastr=""
		for col in range(len(coldefs)):
			##print("{DEL}DATAITM:",datalist[coldefs[col][0] + "[" + coldefs[col][1] + "]:"].strip())
			dlist[coldefs[col][0]]=datalist[coldefs[col][0] + "[" + coldefs[col][1] + "]:"].strip()
			
			if dlist[coldefs[col][0]][0]==dlist[coldefs[col][0]][len(dlist[coldefs[col][0]])-1]=="'":
				tmpstr=dlist[coldefs[col][0]]
				dlist[coldefs[col][0]]=tmpstr[1:len(tmpstr)-1]
			
			##print("{DEL}DATAITM:",dlist)
			
			#if dlist[coldefs[col][0]].lower()=='null':
			#	dlist[coldefs[col][0]]=None  ## Setting value to None will automatically set the bind variable to NULL
			#if col == (len(coldefs)-1):
			#	datastr += coldefs[col][0] + " = %("+coldefs[col][0]+")s"
			#else:
			#	datastr += coldefs[col][0] + " = %("+coldefs[col][0]+")s and "
		
		cur.execute("""
		select tc.table_schema, tc.table_name, kc.column_name
		from
			information_schema.table_constraints tc,
			information_schema.key_column_usage kc
		where
			tc.constraint_type = 'PRIMARY KEY'
			and kc.table_name = tc.table_name and kc.table_schema = tc.table_schema
			and kc.constraint_name = tc.constraint_name
			and tc.table_name=%s
			and tc.table_schema=%s
		order by 1,2
		""",
		(table,schema)
		)

		row = cur.fetchall()
		
		pkcol = row[0][2]
		
		whereclause = pkcol + "=" + "%("+ pkcol +")s"
		
		SQL = "DELETE FROM " + OBJNAME + " WHERE " + whereclause

		return [SQL,dlist]
	except:
		print (now(),"ERROR:[_DELETE]",sys.exc_info()[1],flush=True)
		return None
	
def runsql(SQL,datalist):
	try:
		conn = dbconnect(DBUSER,DBPASS,DBHOST,DBNAME)
		cur=conn.cursor()
		cur.execute(SQL,datalist)
		###print("GSF:RUNSQL",SQL,datalist)
		cur.execute("COMMIT")
		###input("DEBUG")
	except:
		print (now(),"ERROR:[RUNSQL]",sys.exc_info()[1],flush=True)
		print (now(),"DEBUG:[RUNSQL]",SQL,flush=True)
		pass
	
def dbconnect(dbuser,dbpass,dbhost,dbname):
	dbconn=None

	h=""
	if dbhost.find(":",1) <= 0:
		h=dbhost + ":"
	else:
		h=dbhost
		
	host,port = h.split(":")
	##print (now(),"dbconnect", dbuser,dbpass,dbhost,dbname,flush=True)
	try:	
		dbconn = psycopg2.connect("dbname='"+dbname+"' user='"+dbuser+"' host='"+host+"' port='"+port+"' password='"+dbpass+"'")
		##print (now(),"MESSAGE:[DBCONNECT] Connected to database", dbname, "on host", dbhost, "as" , dbuser,flush=True)
		return dbconn 
	except:
		print (now(),"ERROR:[DBCONNECT]",sys.exc_info()[1],"while trying to connect to database", dbname, "on host", dbhost, "as" , dbuser, flush=True)
		return None

def now():
	return datetime.datetime.fromtimestamp(time.time()).isoformat()
	
def cmdlineparams(cmdline):
	plist = {}
	try:
		for n in range(1,len(cmdline)):
			(key, val) = cmdline[n].split('=')
			#print(key,"=",val)
			plist[key.strip().lower()]=val.strip()
		return plist
	except:
		print("USAGE:",cmdline[0],"PARFILE=<parameter-file-name>","LOGFILE=<log-file-name>","DEBUG=<Y|N>")
		#print (now(),"ERROR:[CMDLINEPARAMS]",sys.exc_info(),flush=True)
		exit()
		
		
def readparams(pfile):
	plist = {}
	try:
		f = open(pfile)
		for line in f:
			(key, val) = line.split('=')
			plist[key.strip().lower()]=val.strip()
		return plist
	except:
		print (now(),"ERROR:[READPARAMS]",sys.exc_info()[1],flush=True)
		return None
   

def loadparams(plist):
	global DBUSER,DBPASS,DBHOST,DBNAME,MODE, REPSLOTNAME, POLLINTERVAL, FILEPREFIX, EXTRPATH,TABLES
	if plist.get('dbuser') != None: DBUSER = plist.get('dbuser')
	if plist.get('dbpass') != None: DBPASS = plist.get('dbpass')
	if plist.get('dbhost') != None: DBHOST = plist.get('dbhost')
	if plist.get('dbname') != None: DBNAME = plist.get('dbname')
	if plist.get('mode') != None: MODE = plist.get('mode')
	if plist.get('pollinterval') != None: POLLINTERVAL = plist.get('pollinterval')
	if plist.get('fileprefix') != None: FILEPREFIX = plist.get('fileprefix')
	if plist.get('extrpath') != None: EXTRPATH = plist.get('extrpath')
	if plist.get('repslotname') != None: REPSLOTNAME = plist.get('repslotname')
	if plist.get('tables') != None: TABLES = plist.get('tables')

def appmain(cmdline):
	'''This is the main section of the app'''

	if len(cmdline) <= 1:
		print("USAGE:",cmdline[0],"PARFILE=<parameter-file-name>","LOGFILE=<log-file-name>","DEBUG=<Y|N>")
		exit()	
	
	CMDLINEARGS=cmdlineparams(cmdline)
	if CMDLINEARGS is None:
		print("ERROR:[appmain]","Invalid or empty command line specified")
		exit()
	else:
		pfile=CMDLINEARGS.get('parfile')
		logfile=CMDLINEARGS.get('logfile')
	
	PARAMLIST=readparams(pfile)

	if PARAMLIST is None:
		print("ERROR:[appmain]","Empty parameter list")
		exit()
		
	if logfile is not None:
		sys.stdout = open(logfile,"a+")
	
	loadparams(PARAMLIST)
	
	if MODE.upper()=="EXTRACT":
		extractloop(POLLINTERVAL)
	elif MODE.upper()=="APPLY":
		applyloop(POLLINTERVAL)
	
##MAIN SECTION###
if __name__ == "__main__":
	appmain(argv)

