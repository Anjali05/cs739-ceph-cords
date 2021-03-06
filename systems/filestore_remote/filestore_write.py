import sys
import os
import time
import subprocess
import logging
import rados, sys

remote_user_name = 'anjali'
cords_dir = '/home/anjali/CORDS/'
workload_home = cords_dir + '/systems/filestore_remote/'
my_cluster_dir = '/home/anjali/my-cluster/'

#Filestore code home, log file names
fs_home = '/home/anjali/filestore/'
fs_logfile_name = 'filestore.out'


#Create Handle Examples.
cluster = rados.Rados(conffile= my_cluster_dir+'ceph.conf')
print "\nlibrados version: " + str(cluster.version())
print "Will attempt to connect to: " + str(cluster.conf_get('mon initial members'))
cluster.connect()
print "\nCluster ID: " + cluster.get_fsid()
print "\n\nCluster Statistics"
print "=================="
cluster_stats = cluster.get_cluster_stats()
for key, value in cluster_stats.iteritems():
    print key, value
pools = cluster.list_pools()
for pool in pools:
    #cluster.delete_pool(pool)
    print pool

#cluster.create_pool('test')
print "1"
ioctx = cluster.open_ioctx('test')
print "2" , type(ioctx)
ioctx.write_full("nap", "I am sleepy")
print "3"
print ioctx.read("nap")
ioctx.remove_object("nap")
print "4"

ioctx.close()
cluster.shutdown()

