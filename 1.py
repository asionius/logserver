#!/usr/bin/python -t
#coding:utf-8
#code by struggle
import rados, sys
import gzip
import re
from cStringIO import StringIO

poolname = 'test'
cluster = rados.Rados(conffile='ceph.conf')
cluster.connect()



def Get_info(obj,selstr):
    ioctx = cluster.open_ioctx(poolname)
    l, _ = ioctx.stat(obj)
    cc = ioctx.read("test",l)
    buf = StringIO(cc)
    f = gzip.GzipFile(mode = 'rb', fileobj = buf)
    for i in f.readlines():
        if re.search(selstr,i):
            print i
    ioctx.close()


def main():
    st = sys.argv[1]
    et = sys.argv[2]
    se = sys.argv[3]
    si = sys.argv[4]
    if st and et and se and si:
        pass
    else:
        print "input error"


if __name__ == '__main__':
    Get_info('test','115.34.154.148')
