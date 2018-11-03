#!/usr/bin/env  python

import sys
import os
import time
import subprocess
import logging
import rados, sys


remote_user_name = 'anjali'
cords_dir = '/home/anjali/CORDS'
workload_home = cords_dir + '/systems/filestore_remote/'
my_cluster_dir = '/home/anjali/my-cluster/'

#Filestore code home, log file names
fs_home = '/home/anjali/filestore/'
fs_logfile_name = 'filestore.out'

servers = ['0', '1', '2']

ips = {}
ips['0'] = 'ceph-p2'
ips['1'] = 'ceph-p2-node1'
ips['2'] = 'ceph-p2-node2'


def invoke_cmd(cmd):
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, 	stderr=subprocess.PIPE)
    out, err = p.communicate()
    return (out, err)

def invoke_remote_cmd(machine_ip, command):
    cmd = 'ssh {0} \'{1}\''.format(machine_ip, command)
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, 	stderr=subprocess.PIPE)
    out, err = p.communicate()
    return (out, err)

def run_remote(machine_ip, command):
    cmd = 'ssh {0} \'{1}\''.format(machine_ip, command)
    os.system(cmd)

def copy_file_remote(machine_ip, from_file_path, to_file_path):
    cmd = 'scp {0} {1}:{2}'.format(from_file_path, machine_ip, to_file_path)
    os.system(cmd)


for i in servers:
    #cfg_file = '{0}/ceph{1}.cfg'.format(workload_home, str(i))
    cmd = "sudo systemctl stop ceph-osd.target;"
    cmd += 'rm -rf {0}/debuglogs;'.format(workload_home)
    cmd += 'rm -rf {0}/trace*;'.format(workload_home)
    cmd += 'rm -rf {0}/workload_dir{1}*;'.format(workload_home, i)
    cmd += 'sleep 1;'
    #	cmd += "killall -s 9 java >/dev/null 2>&1; killall -s 9 java >/dev/null 2>&1;"
    cmd += 'mkdir -p {0}/debuglogs/{1};'.format(workload_home, i)
    cmd += 'mkdir -p {0}/workload_dir{1};'.format(workload_home, i)
    #cmd += 'rm -rf {0};'.format(cfg_file)
    cmd += '''echo {0} > {1}/workload_dir{0}/myid;'''.format(i, workload_home)
    run_remote(ips[i], cmd)

print "work fucker1"
lol = invoke_cmd('hostname')
print lol


for i in servers:
    print "work fucker 2"
    command = 'sudo systemctl start ceph-osd.target;'
    #cfg_file = '{0}/ceph{1}.cfg'.format(workload_home, str(i))
    #os.system('rm -rf {0}'.format(cfg_file))
    #with open(cfg_file, 'w') as fh:
    #	fh.write(conf_string.format(workload_home + '/workload_dir' + str(i)))
    #copy_file_remote(ips[i], cfg_file, cfg_file)

    #remote_command = 'cd {0}/debuglogs/{1};{2} {3} > ./ceph.out 2>&1 < /dev/null &'.format(workload_home, str(i), command, cfg_file)
    print "work fucker 3"
    run_remote(ips[i], command)


os.system('sleep 1')

#cluster = rados.Rados(conffile= my_cluster_dir+'ceph.conf')
#print "\nlibrados version: " + str(cluster.version())
#print "Will attempt to connect to: " + str(cluster.conf_get('mon initial members'))
#cluster.connect()

#print "\nCluster ID: " + cluster.get_fsid()
print "\n\nCluster Statistics"
print "=================="
#cluster_stats = cluster.get_cluster_stats()
print "work fucker 4"
#for key, value in cluster_stats.iteritems():
#    print key, value
#pools = cluster.list_pools()
#for pool in pools:
#cluster.delete_pool(pool)
#    print pool
#cluster.create_pool('test')
print "1"
#ioctx = cluster.open_ioctx('test')
#print "2" , type(ioctx)
#ioctx.write_full("nap", "I am sleepy")
print "3"
#print ioctx.read("nap")
#ioctx.remove_object("nap")
print "4"
#ioctx.close()
#cluster.shutdown()

os.system('sudo rados mkpool test')
os.system('echo "lol" > lolinput.txt')
os.system('sudo rados -p test put lolobject lolinput.txt')
