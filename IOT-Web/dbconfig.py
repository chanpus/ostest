import os

#IP_ADDR = "172.30.1.6"
IP_ADDR = "192.168.219.152"
#DIR_DATAPATH = os.path.join("D:", r"D:\home\project\medical\data")

db_host = IP_ADDR
db_name = 'mysql'
db_user = 'root'
db_password = '0099'

def setHost(host):
    global db_host
    db_host = host
