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
import math
from collections import defaultdict
import subprocess
import argparse
remote_user_name = 'anjali'
conf_dir = '/home/anjali/my-cluster/'
etc_conf_file = "/etc/ceph/ceph.conf"
def invoke_remote_cmd(machine_ip, command):
        cmd = 'ssh {0}@{1} \'{2}\''.format(remote_user_name, machine_ip, command)
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,   stderr=subprocess.PIPE)
        out, err = p.communicate()
        return (out, err)
def copy_file_from_remote(machine_ip, from_file_path, to_file_path):
        cmd = 'scp {0}@{1}:{2} {3}'.format(remote_user_name, machine_ip, from_file_path, to_file_path)
        os.system(cmd)
def copy_file_to_remote(machine_ip, from_file_path, to_file_path):
        cmd = 'scp {0} {1}@{2}:{3}'.format(from_file_path, remote_user_name, machine_ip, to_file_path)
        os.system(cmd)
def invoke_cmd(cmd):
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        return (out, err)
###############Step 1: Start  errfs#############################
print "starting step 1..."
ERRFS_HOME = os.path.dirname(os.path.realpath(__file__))
#fuse_command_trace = 'sudo nohup ~/CORDS/errfs -f -omodules=subdir,subdir=%s,nonempty,allow_other %s trace %s &'
#fuse_command_trace = 'sudo nohup ~/CORDS/errfs -f -omodules=subdir,subdir=/var/lib/ceph/osd/ceph-0,nonempty,allow_other /var/lib/ceph/osd/ceph-0.mp trace /home/anjali/trace'
fuse_command_trace = "sudo -u ceph nohup ~/CORDS/errfs -f -ononempty,modules=subdir,subdir=%s %s trace %s > /dev/null 2>&1 &"
parser = argparse.ArgumentParser()
parser.add_argument('--trace_files', nargs='+', required = True, help = 'Trace file paths')
parser.add_argument('--machines', nargs='+', required = True, help = 'Machine ips')
parser.add_argument('--data_dirs', nargs='+', required = True, help = 'Location of data directories')
parser.add_argument('--workload_command', required = True, type = str)
parser.add_argument('--ignore_file', type = str, default = None)

args = parser.parse_args()
for i in range(0, len(args.trace_files)):
        args.trace_files[i] = os.path.abspath(args.trace_files[i])

for i in range(0, len(args.data_dirs)):
        args.data_dirs[i] = os.path.abspath(args.data_dirs[i])

trace_files = args.trace_files
data_dirs = args.data_dirs
ignore_file = args.ignore_file
machines = args.machines

assert len(trace_files) == len(data_dirs)
machine_count = len(trace_files)

workload_command = args.workload_command
uppath = lambda _path, n: os.sep.join(_path.split(os.sep)[:-n])

data_dir_snapshots = []
data_dir_mount_points = []


for i in range(0, machine_count):
        data_dir_snapshots.append(os.path.join(uppath(data_dirs[i], 1), os.path.basename(os.path.normpath(data_dirs[i]))+ ".snapshot"))
        data_dir_mount_points.append(os.path.join(uppath(data_dirs[i], 1), os.path.basename(os.path.normpath(data_dirs[i]))+ ".mp"))
        command = "sudo rm -rf " + data_dir_snapshots[i] + ";"
        command += "sudo rm -rf " + data_dir_mount_points[i] + ";"
        command += "sudo mkdir " + data_dir_mount_points[i] + ";"
        command += "sudo chown -R ceph.ceph " + data_dir_mount_points[i] + ";"
        print "creating stuff"
        invoke_remote_cmd(machines[i], command)

for i in range(0, machine_count):
    command = "sudo dd if=/dev/sdb2 of=/dev/sdb1 bs=1M;"
    command +=  "sudo cp -aR " + data_dirs[i] + " " + data_dir_snapshots[i] + ";"
    command += "sudo rm -rf " + trace_files[i]
    print "copying stuff"
    invoke_remote_cmd(machines[i], command)

for i in range(0, machine_count):
    command = fuse_command_trace%(data_dirs[i], data_dir_mount_points[i], trace_files[i])
    print "invoking fuse"
    print command
    invoke_remote_cmd(machines[i], command)
    print "done with fuse!"

print "stopping step 1...."

os.system('sleep 5')

###############Step 2: Stop OSD#############################

#stop osd on every machine
for i in range(0, machine_count):
        command = "sudo systemctl stop ceph-osd@"+str(i)+";"
        print "stopping osd"
        invoke_remote_cmd(machines[i], command)

os.system('sleep 5')

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
for i in range(0, machine_count):
        conf_str += "[osd.{0}]\n".format(str(i))
        conf_str += "osd data = {0}\n".format(data_dir_mount_points[i])
        conf_str += "osd max object name len = 256\nosd max object namespace len = 64\n\n"

with open(tmp_cfg_file, 'a') as fh:
        fh.write(conf_str)

print "starting conf"

# TODO fix hard code
os.system("ceph-deploy --ceph-conf {0} --overwrite-conf admin {1} {2} {3}"
                  .format(tmp_cfg_file, machines[0], machines[1], machines[2]))


print "stopping step 3...."

###############Step 4: Start OSD#############################
#start osd on every machine
for i in range(0, machine_count):
        command = "sudo systemctl start ceph-osd@"+str(i)+";"
        invoke_remote_cmd(machines[i], command)

print "stopping step 4....."

##############Step 5: Run Workload#############################

os.system('sleep 5')

workload_command +=  " trace "
for i in range(0, machine_count):
        workload_command += data_dir_mount_points[i] + " "

for i in range(0, machine_count):
        workload_command += machines[i] + " "

os.system(workload_command)

print "stopping step 5"

##############Step 6: Stop OSD and revert back to original conf#############################

# stop OSD
for i in range(0, machine_count):
        command = "sudo systemctl stop ceph-osd@"+str(i)+";"
        invoke_remote_cmd(machines[i], command)

print "done with stopping osd..going to sleep"

os.system('sleep 5')
#revert configuration
# TODO fix hard code
os.system("ceph-deploy --ceph-conf {0} --overwrite-conf admin {1} {2} {3}"
                  .format(cfg_file, machines[0], machines[1], machines[2]))

print "stopping step 6..."
##############Step 7: Unmount#############################
i = 0
for mp in data_dir_mount_points:
        invoke_remote_cmd(machines[i], 'sudo fusermount -u ' + mp + '; sleep 1' + '; killall errfs >/dev/null 2>&1')
        i += 1

to_ignore_files = []
if ignore_file is not None:
        with open(ignore_file, 'r') as f:
                for line in f:
                        line = line.strip().replace('\n','')
                        to_ignore_files.append(line)

def should_ignore(filename):
        for ig in to_ignore_files:
                if ig in filename:
                        return True
        return False

i = 0
for trace_file in trace_files:
        os.system('scp anjali@' + machines[i] + ':' + trace_file + ' ' + trace_file)
        i += 1

for trace_file in trace_files:
        assert os.path.exists(trace_file)
        assert os.stat(trace_file).st_size > 0
        if ignore_file is not None:
                to_write_final = ''
                with open(trace_file, 'r') as f:
                        for line in f:
                                parts = line.split('\t')
                                if parts[0] in ['rename', 'unlink', 'link', 'symlink']:
                                        to_write_final += line
                                else:
                                        assert len(parts) == 4
                                        filename = parts[0]
                                        if not should_ignore(filename):
                                                to_write_final += line

                os.remove(trace_file)
                with open(trace_file, 'w') as f:
                        f.write(to_write_final)

print 'Tracing completed...'



###############Step 8: Start OSD#############################
for i in range(0, machine_count):
        command = "sudo dd if=/dev/sdb2 of=/dev/sdb1 bs=1M;"
        command += "sudo systemctl start ceph-osd@"+str(i)+";"
        invoke_remote_cmd(machines[i], command)