#!/usr/bin/python
#
#################################################################

import argparse
import boto.ec2
from multiprocessing import Pool
import requests
import sys
import time


######################################################################
## Parse arguments from commandline                                 ##
######################################################################
parser = argparse.ArgumentParser(
   description='Snapshot one or more EBS volumes while retaining set-level data-consistency.',
   epilog='Sets are defined by tagging EBS volumes with the "Consistency Group" tag'
   )
parser.add_argument("-f", "--freeze", metavar="/MOUNT/POINT", 
                    help="Filesystem to freeze (Linux-only)",
                    action='append')
parser.add_argument("-v", "--verbose",
                    help="Enable verbose run-mode (not implemented)",
                    action="store_true")
parser.add_argument("cgroup", help="Name of the EBS consistency group")
toolargs = parser.parse_args()

cgroup = toolargs.cgroup

#                                                                    #
######################################################################


######################################################################
## Define global functions                                          ##
######################################################################

# Compute this instance's Region and ID
def instance_meta():
   url = 'http://169.254.169.254/latest/dynamic/instance-identity/document/'
   region = (requests.get(url).json()[u'region'])
   instance = (requests.get(url).json()[u'instanceId'])
   return { 'region' : region, 'instance' : instance }


# Get list of volumes for instance matching tag
def targ_vols(i, l):
   my_instance = i
   my_label = l

   my_vols = awsconn.get_all_volumes(filters={'attachment.instance-id': my_instance, 'tag:Consistency Group' : my_label})

   return my_vols


# Snapshot the passed volume-object
def snap_vols(ebs):
   ebs_vol = ebs.id

   snap_desc = instance + "-bkup-" + timestamp

   snap_id = awsconn.create_snapshot(ebs_vol, description=snap_desc)
   print snap_id.id

   return snap_id.id

# Tag successfully-created snapshots
def snap_tag(list):
   snap_list = list[0]	# This is stupidly-ugly, but works
   tags = {}
   tags['Name'] = 'AutoBack (' + instance + ') ' + timestamp
   tags['Snapshot Group'] = timestamp + ' (' + instance + ') ' + cgroup
   tags['Created by'] = 'Automated Backup'
   
   for snapshot in snap_list:
      print 'Adding tags to snapshot "' + snapshot + '"."'
      awsconn.create_tags(snapshot, tags)
   return

#                                                                    #
######################################################################


######################################################################
## Main program flow                                                ##
######################################################################
timestamp = time.strftime("%Y%m%d%H%M")

instmeta = instance_meta()
region   = instmeta['region']
instance = instmeta['instance']

# Establish connection to AWS
awsconn = boto.ec2.connect_to_region(region)

# Find volumes in 'Consistency Group'
group_vols = targ_vols(instance, cgroup)
print group_vols

# Initialize snapshot list
snap_ids = []

# Request snapshots of any found volumes
if group_vols:
   if __name__ == '__main__':
      p = Pool(5)
      snap_ids.append(p.map(snap_vols, group_vols))
# Exit if no volumes found with tag
else:
   print "No EBS volumes found with named 'Consistency Group' tag"
   sys.exit(1)

print snap_ids
snap_tag(snap_ids)
