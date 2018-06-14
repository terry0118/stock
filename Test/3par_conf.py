import paramiko
import sys
import re
import time

class hp3par():
    def __init__(self,ip, user, passwd, port):
        self.ip = ip
        self.user = user
        self.passwd = passwd
        self.port = port
        self.msgs = list()
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            self.ssh.connect(hostname=self.ip,username=self.user,password=self.passwd,port=self.port)
        except Exception as e:
            print("connect to " + self.ip + " failed:" + str(e))
            sys.exit(1)
    def getcmdout(self,cmd):
        self.cmd = cmd
        try:
            self.stdin,self.stdout,self.stderr = self.ssh.exec_command(self.cmd)
            return self.stdout
        except Exception as e:
            print("Excute command faild: " + str(self.e))

if __name__ == "__main__":
    s = hp3par("192.168.194.253","3paradm","3pardata",22)
    out = s.getcmdout('showhost -d')
    hosts = dict()
    for line in out.readlines():
        if(re.search(r'\d+\s+\S+\s+\S+\s+\w+\s+\S+',line)):
            (hostid,hostname,ostype,hostwwn,port,na) = line.split()
            if(hostname not in hosts.keys()):
                wwn = list()
                hosts[hostname] = dict()
            wwn.append(hostwwn)
            wwn2 = set(wwn)
            hosts[hostname]['WWN'] = wwn2
    # # print(hosts)

    for host in hosts.keys():
        out2 = s.getcmdout('showvv -showcols Name,Usr_Rsvd_MB,VSize_MB -host ' + host)
        for line in out2.readlines():
            if(re.match(r'total',line)):
                (total, usr_total, usr_vsize) = line.split()
                usr_total = int(usr_total)/1024
                usr_vsize = int(usr_vsize)/1024
                hosts[host]["usr_total"] = int(usr_total)
                hosts[host]["usr_vsize"] = int(usr_vsize)

    for host in hosts.keys():
        out3 = s.getcmdout('showhostset -host ' + host)
        for line in out3.readlines():
            # print(line)
            if(re.search(r'\d+\s+(?!total)\w+\s+\w+', line)):
                # print(line)
                (hostsetid, hostsetname, hostname) = line.split()
                hosts[host]["hostset"] = hostsetname

            if(re.match(r'No host set listed', line)):
                hosts[host]["hostset"] = 'N/A'
    # print(hosts)

    for host in hosts.keys():
        # print(host)
        for w in hosts[host]['WWN']:
            if(list(hosts[host]['WWN']).index(w) == 0):
                if(hosts[host]['hostset'] == 'N/A'):
                    print("%-20s|%-30s|%-30s|%-15s|%-15s|%-15s|%-15s" % (host, hosts[host]['hostset'], w, hosts[host]['usr_total'], hosts[host]['usr_vsize'], 0, 0))
                else:
                    print("%-20s|%-30s|%-30s|%-15s|%-15s|%-15s|%-15s" % (host, hosts[host]['hostset'], w, 0, 0, hosts[host]['usr_total'], hosts[host]['usr_vsize']))
            else:
                print("%-20s|%-30s|%-30s|%-15s|%-15s|%-15s|%-15s" % ('','', w,'','','','' ))
