#!/usr/bin/python
#
#################################################################

import argparse
import requests
import boto.ec2


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
parser.add_argument("-v", "--verbose", help="Enable verbose run-mode (not implemented)", action="store_true")
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

def targ_vols(i, l):
   my_instance = i
   my_label = l

   my_vols = awsconn.get_all_volumes(filters={'attachment.instance-id': my_instance, 'tag:Consistency Group' : my_label})

   return my_vols

def thread_snap(volobj):
   VolObjList = volobj

   for vol in VolObjList:
      print vol.id
   return

#                                                                    #
######################################################################


######################################################################
## Main program flow                                                ##
######################################################################
instmeta = instance_meta()
region   = instmeta['region']
instance = instmeta['instance']

print "Getting volumes for instance '%s' in region '%s'." % (instance, region)

awsconn = boto.ec2.connect_to_region(region)

thread_snap(targ_vols(instance, cgroup))

#print vollist


