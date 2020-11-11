'''
shuibao.yu@pyxis-lab.com.cn
2016.10.10
'''
import socket
import time 
from datetime import datetime
import pdb
import json
import os
import pdb
import sys
import os.path
import logging
import zlib
import base64
#from db import DBProxy
import threading
import glob
import queue #thread-safe 
import shutil
from RC import RC 
from Helper import Helper
from mdslave import TKSlave
from mdmaster import TKMaster
from mbtcpmaster import modbusTcpMaster
import requests
import traceback
import subprocess
import time

os.system("mkdir ./logs")

helper= Helper()
helper.setLog()
conf=helper.getconf()

startime = time.time()
#Helper.init_flag() possible conflict with guard when write in the same time
class Kernel:
    #db = DBProxy()
    
    def initAll(self,conf):
        self.AutoIndex=0;
        path='/media/sdcard/data/'
        if os.path.exists(path):
             self.df = path +'sqlite2.db'
        else:
             self.df = 'sqlite2.db'
        #Kernel.db.opendb(df)
        self.datapath='./data/'
        if not os.path.exists(self.datapath):
            os.mkdir(self.datapath)
        self.conf = conf
        self.reportdata={"data": {"type": "report", "devid": conf['devid'],
                                  "maskconf": conf.get('maskconf',0), "value":[]}}
        self.circledata= {'timestamp':'', "value":[]}
        self.client = None

        self.Q = queue.LifoQueue()
        t=threading.Thread(target=self.SendReportThread)
        t.start()
        self.master = None
        self.slave = None
        self.rc = None

    def checkreport(self):
        realdata = self.reportdata['data']['value']
        if not realdata: 
            logging.info('no realdata')
            return
        #Kernel.db.safesave(self.df, realdata)
        #info =  Helper.getsiteinfo()
        #self.reportdata.update(info)
        txt=json.dumps(self.reportdata)
		
        #开机1分钟静默期，不发送信息		
        span = time.time()-startime
        span = Helper.uptime()  # 开机相对时间，绝对时间会同步，会出问题
        #if span>120:
        self.Q.put(txt)

        self.reportdata['data']['value'] = []
        self.circledata= {'timestamp':'', "value":[]}
        pass

    def check_3g(self):
        logging.info("check_3g")

        now = Helper.uptime()
        cloud = Helper.get_flag_timestamp("3g")
        dif = abs(now - cloud)
        if dif> 10*60 and (self.master or self.slave):
            # PLC check the flag , and then reset it after poweroff
            if self.master:
                self.master.poweroff()
            if self.slave:
                self.slave.poweroff()

            logging.error("poweroff")
            Helper.set_flag_timestamp("3g")
        
    def __safeDoReport(self):
        try:
            qlen=self.Q.qsize()
            logging.info('******thread loop, qlen: {}'.format(qlen))
            if qlen==0:
                path, msg=self.loadmsg()
                if msg is not None:
                    bRet = self.sendReport(msg)
                    if bRet: os.remove(path)
            else:
                msg = self.Q.get()
                utf8 = msg.encode('utf-8')
                b64 = base64.b64encode(zlib.compress(utf8))
                path = self.savemsg(b64)
                
                bRet=self.sendReport(b64)
                if bRet: os.remove(path)
            time.sleep(5)
            
            # why time.clock() could not work
            now = datetime.now()
            dif = (now-self.lasttime).total_seconds()
            dif = abs(dif)
            if dif<0: #the time is ajusted
                self.lasttime = now
            logging.info("check work {}".format(dif))
            if dif >= 15:
                logging.info("CheckWork")
                self.lasttime = now
                self.CheckWork()
            
            dif = (now-self.lastVPN).total_seconds()
            dif = abs(dif)
            logging.info("lastvpn {}".format(dif))
            if dif>15*60:
                #os.system("killall pppd")
                self.lastVPN=now 
        except Exception as e:
            logging.error(e)
            traceback.print_exc(file=sys.stdout)

    def SendReportThread(self):
        self.lasttime = datetime.now()
        self.lastworktime = datetime(2000,1,1)
        self.lastVPN=datetime.now()
        while True:

            self.__safeDoReport()
        pass
    def GetPPPIp(self):
        #cmd = "ifconfig | awk '/inet addr/{print substr($2,6)}'"
        cmd ="ifconfig"
        p= os.popen(cmd)
        alllines = p.read().split("\n")
        p.close()
        ips = []
        for it in alllines:
            pos_start = it.find(":")
            pos_end = it.find("P-t-P")
            if pos_start >0 and pos_end>0:
                p = it[pos_start+1:pos_end]
                ips.append(p.rstrip())
        return ips

    def HandleCommand(self, msg):
        obj = {}
        try:
            obj = json.loads(msg)
        except Exception as e:
            obj = {"cmd":"vpn"} # old versin, msg:"vpn 6003"
        cmd = obj["cmd"]

        if cmd in ["vpn", 'VPN']:
            self.lastVPN = datetime.now()
            sn = socket.gethostname()
            if sn.startswith("196"):
                sn = socket.gethostname()[-5:]
            else:
                sn = socket.gethostname()[-4:]

            ips = helper.get_control_servers()
            logging.info("ips {}".format(ips))
            for ip in ips:
                cmd = "ssh -NfR {}:localhost:22 root@{}&".format(sn, ip)
                logging.info("COMMAND: {}".format(cmd))
                subprocess.Popen(cmd, shell=True)

        if cmd in ["killvpn","KILLVPN"]:
            os.system("killall ssh")

        if cmd == "control":
            # RC {"pH_WarningLower":3,"pH_Warningupper":4}
            self.slave.execute_rc_cmd(obj)
        pass

    def sendRC(self, bRec):
        ret=""

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM);
        try:
            sock.settimeout(10.0)
            ips = helper.get_report_servers()

            logging.info("sendRC {}".format(ips))
            ip = ips[0]
            sock.connect((ip, 7789))
            sn = socket.gethostname()
            ips = self.GetPPPIp()
            ip = ips[0] if ips else ""
            rcret=""
            if self.rc: rcret=self.rc.rcret
            rcobj = {"SN":sn,"IP":ip,"rcret":rcret}
            if self.slave:
                tmp = self.slave.rc_states.copy()
                rcobj.update(tmp)
            txt=json.dumps(rcobj)
            sock.send(bytes(txt,encoding='utf-8'));
            if bRec:
                sock.settimeout(1.0)
                try:
                    r=sock.recv(300)
                    ret=str(r,'utf-8')

                except Exception as e:
                    pass
        except Exception as e:
            traceback.print_exc(file=sys.stdout)
        finally:
            sock.close()
        return ret    
    def CheckWork(self):
        try:
            r = self.sendRC(True)
            
            if r:
                logging.info("getwork {}".format(r))
                self.HandleCommand(r)
                self.lastworktime=datetime.now()
                self.sendRC(False)
        except Exception as e:
            traceback.print_exc(file=sys.stdout)

            print(e)
              
    def safeSend(self, client, bins):
        packlen=1024
        nsend=0
        while nsend<len(bins):
            pack=bins[nsend: nsend+packlen]
            nsend+=len(pack)
            client.send(pack)

    def savemsg(self, m):
        total, used, free = shutil.disk_usage(__file__)
        mfree=free/1024/1024
        if mfree<200: # disk/memory is not enough
            fs=glob.glob(self.datapath+"*.bin")
            fs.sort(key=os.path.getmtime)
            h=int(len(fs)/2)
            for it in fs[:h]: #删除日期靠前的
                try:
                    os.remove(it)
                except:
                    pass
            pass
            
        name = datetime.now().strftime("%Y%m%d-%H%M%S")
        path=self.datapath+name+'.bin'
        with open(path, 'bw') as f:
            f.write(m)
        return path

    def loadmsg(self):
        fs=glob.glob(self.datapath+"*.bin")
        fs.sort(key=os.path.getmtime, reverse=True) 
        if not fs: return '', None
        ret = None
        with open(fs[0], 'br+') as f:
            ret = f.read(100*1024)
        return fs[0],ret

    def sendReport(self, bintxt):
        for host in helper.get_report_servers():
            port = self.conf['server']['port']
            logging.info('report: host %s, port %d', host,port)
            #logging.info('report: '+bintt)
            logging.info('report: len {}: '.format(len(bintxt)))
            bConn = False
            bSuccess = False
            try:
                logging.info('report: connect socket')
                
                client = socket.socket(socket.AF_INET,socket.SOCK_STREAM)  
                client.setblocking(True)
                client.settimeout(30)
                client.connect((host,port))
                bConn = True
                logging.info('report: finish connect')
                client.setblocking(True)
                self.safeSend(client, bintxt)#utf8)
                bSuccess =True
                logging.info('report: successfully')

                Helper.set_flag_timestamp("cloud")
                Helper.set_flag_timestamp("3g")
            except Exception as e:
                traceback.print_exc(file=sys.stdout)
                
                bSuccess = False
                logging.info('report: error:')
                logging.error(e)
                logging.info('socket shutdown and close')
                os.system("dhclient eth0 -v")
                Helper.ping("cloud2.pyxis-lab.com.cn")
                Helper.ping("192.168.10.1")
            finally:

                logging.info('close socket')
                if bConn:
                    client.shutdown(socket.SHUT_RDWR)
                client.close()
                client = None
        return bSuccess

    def writebin(self,addr, arr):
        if self.slave:
           self.slave.writebin(addr,arr)

    def _AutoQuery(self):
        os.system("touch /opt/pypyxisbbb/flag")
        global conf
        self.initAll(conf)
        batchReadInterval = 12;# conf['batchReadInterval']
        tsReport = datetime.now()
        tsGetData = tsReport
        chnlist = []

        for it in conf['mobustype']:
            if 'regTab' in it:
                regs = self.conf[it["regTab"]]
            if 'Serial' in it:
                sop = self.conf[it["Serial"]]
            if 'address' in it:
                addr = it['address']

            rep = 0 if 'report' not in it else it['report']

            if it['type']  == 'slave':
                print("slave****************")
                slv = TKSlave(sop, regs, addr, conf)
                print(sop)
                slv.mobustype=it
                slv._report = rep
                slv.start()
                slv._server.set_verbose(True)
                chnlist.append(slv)
                self.slave = slv
            elif it['type'] == 'rc':
                print("RC************")
                self.rc = RC()
                self.rc.init()
                chnlist.append(self.rc)
                self.rc.kenerl=self
            elif it['type'] == 'tcpmaster':
                print("TCP master******************")
                m = modbusTcpMaster(it['IP'],it['Port'],regs,conf)
                m.master.set_verbose(False)
                m._report = rep
                chnlist.append(m)
                self.master = m
                pass
            else:
                print("master*******************")
                m = TKMaster(sop, regs, addr, conf)
                m.master.set_verbose(True)
                m._report = rep
                m.kernel=self
                chnlist.append(m)
                self.master = m
            chnlist[-1].header= it
            chnlist[-1].kernel=self
        count=0
        readcounter = 0
        while True:

            self.check_3g()
            Helper.set_flag_timestamp("modbus")
            for i in range(3):
                if self.rc: self.rc.CheckPLCWork()
                time.sleep(1)
             
            global conf
            conf=helper.getconf() # try to update configuration without reboot
            now = datetime.now()
            strnow = now.strftime('%Y-%m-%d %H:%M:%S')
            
            for it in chnlist:
                it.readbatch()
            print("readcounter", readcounter)
            if readcounter>=5 : #15s
                readcounter=0
                self.circledata={}
                self.circledata['timestamp']= strnow
                
                info =  Helper.getsiteinfo()
                v = info["diskfreeM"]
                self.circledata['value'] = [{"sensorid":"diskfreeM", "reg_val": v}]

                for it in chnlist:
                    if not it.header.get("report", False):
                        continue
                    
                    ar=it.getdata()
                    
                    # empty the bad data
                    if isinstance(it, TKSlave) and it.received_from_plc ==False:
                        ar = [] 
                        logging.info("filter bad data {}".format(ar))

                    self.circledata['value'].extend(ar)
                self.reportdata['data']['value'].append(self.circledata)
                print(self.circledata)
                count+=1

            readcounter+=1 
            logging.info('readcount:{}, count:{} '.format(readcounter, count))
            sp = (now-tsReport).total_seconds()
            if count >= 4:#4=60s, samples count, upload data
                self.checkreport()
                tsReport = now
                self.reportdata['data']['value']=[]
                count=0

            Helper.checkguard("pyxisguard.py")
            
        pass
    def startAutoQueryThread(self):
        #t = threading.Thread(target=self._AutoQuery)
        #t.start()
        self._AutoQuery()

def hasrun():
    f=os.path.basename(__file__)
    out = os.popen("ps aux|grep python").read()
    itself=out.find(f)
    if out.find(f, itself+1)>0:
        logging.error('has run, exist')
        sys.exit(0)
#waiting for seconds for for serial resource. because the strange 2017 datetime  is seen in log file
time.sleep(1)
#hasrun()
k = Kernel()
k.startAutoQueryThread()

