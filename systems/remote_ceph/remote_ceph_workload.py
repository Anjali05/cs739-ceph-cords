#! /usr/bin/env python
# Copyright (c) 2016 Aishwarya Ganesan and Ramnatthan Alagappan.
# All Rights Reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import os
import sys
import subprocess

remote_user_name = 'anjali'
conf_dir = '/home/anjali/my-cluster/'
etc_conf_file = "/etc/ceph/ceph.conf"

def invoke_remote_cmd(machine_ip, command):
	cmd = 'ssh {0}@{1} \'{2}\''.format(remote_user_name, machine_ip, command)
	p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, 	stderr=subprocess.PIPE)
	out, err = p.communicate()
	return (out, err)

def copy_file_from_remote(machine_ip, from_file_path, to_file_path):
	cmd = 'scp {0}@{1}:{2} {3}'.format(remote_user_name, machine_ip, from_file_path, to_file_path)
	os.system(cmd)

def copy_file_to_remote(machine_ip, from_file_path, to_file_path):
	cmd = 'scp {0} {1}@{2}:{3}'.format(from_file_path, remote_user_name, machine_ip, to_file_path)
	os.system(cmd)


###############Step 1: Preprocessing#############################
print "starting step 1..."

ips = {}
servers = ['0', '1', '2']
server_dirs = {}

# The CORDS framework passes the following arguments to the workload program
# remote_ceph_workload.py trace/cords workload_dir1 workload_dir2 .. workload_dirn remote_ip1 remote_ip2 .. remote_ipn [log_dir]
# For now assume only 3 nodes
for i in range(1, 4):
	ips[str(i-1)] = sys.argv[4 + i]

# For Ceph we have three servers and hence three directories
for i in range(1, 4):
	server_dirs[str(i-1)] = sys.argv[i + 1]

uppath = lambda _path, n: os.sep.join(_path.split(os.sep)[:-n])


###############Step 2: Stop OSD#############################

#stop osd on every machine
for i in servers:
	command = "sudo systemctl stop ceph-osd@"+ i +";"
	invoke_remote_cmd(ips[i], command)



print "stopping step 2..."

###############Step 3: Change configuration#############################
# update configuration with data directory, keep ceph.conf under conf_dir
# unchanged, while make changes and propagate tmp_ceph.conf to /etc/ceph/
cfg_file = '{0}/ceph.conf'.format(conf_dir)
tmp_cfg_file = "{0}/tmp_ceph.conf".format(conf_dir)

cfg_command = "sudo rm {0}/tmp_ceph.conf;".format(conf_dir)
cfg_command += "cp -a {0}/ceph.conf {0}/tmp_ceph.conf".format(conf_dir)
os.system(cfg_command)

conf_str = "\n"
for i in servers:
	conf_str += "[osd.{0}]\n".format(str(i))
	conf_str += "osd data = {0}\n".format(server_dirs[i])
	conf_str += "osd max object name len = 256\nosd max object namespace len = 64\n\n"

with open(tmp_cfg_file, 'a') as fh:
	fh.write(conf_str)

# TODO fix hard code
os.system("ceph-deploy --ceph-conf {0} --overwrite-conf admin {1} {2} {3}"
		  .format(tmp_cfg_file, ips['0'], ips['1'], ips['2']))

print "stopping step 3...."

###############Step 4: Start OSD#############################
#start osd on every machine
for i in servers:
	command = "sudo systemctl start ceph-osd@"+str(i)+";"
	invoke_remote_cmd(ips[i], command)

print "stopping step 4....."


###############Step 5: Run Workload#############################

os.system('sleep 1')

# workload_command +=  " trace "
# for i in range(0, machine_count):
# 	workload_command += data_dir_mount_points[i] + " "
#
# for i in range(0, machine_count):
# 	workload_command += machines[i] + " "

# TODO issue reads on ALL nodes
workload_command = "sudo rados -p test get lolobject lolfile.txt"
for i in servers:
	invoke_remote_cmd(ips[i], workload_command)

print "stopping step 5"

###############Step 6: Stop OSD#############################

#stop osd on every machine
for i in servers:
	command = "sudo systemctl stop ceph-osd@"+ i +";"
	invoke_remote_cmd(ips[i], command)
	
print "stopping step 6..."

