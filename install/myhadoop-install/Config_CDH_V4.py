#!/usr/bin/env python

"""
    This script is to change the conf of the CDH services,

    the usage is python Config_CDH_V4.py           or
    python Config_CDH_V4.py  -f hosts_name.txt
"""


import getopt
import inspect
import logging
import sys
import textwrap

from cm_api.api_client import ApiResource

from cm_conf.confs import *
from CDH_configs_V4 import *
from utils import *

LOG = logging.getLogger(__name__)
LOG_LEVLE = logging.INFO


def get_cm_host():
    if not CM_HOST.strip():
        return socket.gethostname()
    else:
        return CM_HOST.strip()

def setup_logging(level):
    logging.basicConfig()
    logging.getLogger().setLevel(level)


def usage():
    """
    the usage is:    python Config_CDH_V4.py
                                or
                     python Config_CDH_V4.py  -f hosts_name.txt
    """
    doc = inspect.getmodule(usage).__doc__
    print >> sys.stderr, textwrap.dedent(doc)


def do_bulk_config_update(hostnames):
    """
    Given a list of hostnames, update the configs of all the
    datanodes, tasktrackers and regionservers on those hosts.
    """
    api = ApiResource(get_cm_host(), username=CM_USERNAME, password=CM_USER_PASSWORD)
    hosts = collect_hosts(api, hostnames)

    # Set config
    for h in hosts:
        configure_roles_on_host(api, h)


def collect_hosts(api, wanted_hostnames):
    """
    Return a list of ApiHost objects for the set of hosts that
    we want to change config for.
    """
    all_hosts = api.get_all_hosts(view='full')
    all_hostnames = set([h.hostname for h in all_hosts])
    wanted_hostnames = set(wanted_hostnames)

    unknown_hosts = wanted_hostnames.difference(all_hostnames)
    if len(unknown_hosts) != 0:
        msg = "The following hosts are not found in Cloudera Manager. " \
              "Please check for typos:\n%s" % ('\n'.join(unknown_hosts))
        LOG.error(msg)
        raise RuntimeError(msg)

    return [h for h in all_hosts if h.hostname in wanted_hostnames]


def configure_roles_on_host(api, host):
    """
    Go through all the roles on this host, and configure them if they
    match the role types that we care about.
    """
    for role_ref in host.roleRefs:
        # Mgmt service/role has no cluster name. Skip over those.
        if role_ref.clusterName is None:
            continue

        # Get the role and inspect the role type
        role = api.get_cluster(role_ref.clusterName) \
            .get_service(role_ref.serviceName) \
            .get_role(role_ref.roleName)
        LOG.debug("Evaluating %s (%s)" % (role.name, host.hostname))

        config = None
        if role.type == 'AGENT':
            config = AGENT_CONF
        elif role.type == 'OOZIE_SERVER':
            config = OOZIE_SERVER_CONF
        elif role.type == 'IMPALAD':
            config = IMPALAD_CONF
        elif role.type == 'STATESTORE':
            config = STATESTORE_CONF
        elif role.type == 'HUE_SERVER':
            config = HUE_SERVER_CONF
        elif role.type == 'BEESWAX_SERVER':
            config = BEESWAX_SERVER_CONF
        elif role.type == 'HIVEMETASTORE':
            config = HIVEMETASTORE_CONF
        elif role.type == 'REGIONSERVER':
            config = REGIONSERVER_CONF
        elif role.type == 'MASTER':
            config = MASTER_CONF
        elif role.type == 'SERVER':
            config = SERVER_CONF
        elif role.type == 'NODEMANAGER':
            config = NODEMANAGER_CONF
        elif role.type == 'JOBHISTORY':
            config = JOBHISTORY_CONF
        elif role.type == 'RESOURCEMANAGER':
            config = RESOURCEMANAGER_CONF
        elif role.type == 'JOURNALNODE':
            config = JOURNALNODE_CONF
        elif role.type == 'SECONDARYNAMENODE':
            config = SECONDARY_NAMENODE_CONF
        elif role.type == 'NAMENODE':
            config = NAMENODE_CONF
        elif role.type == 'DATANODE':
            config = DATANODE_CONF
        else:
            continue

        # Set the config
        LOG.info("begin update the conf for %s, the role name is: %s (%s)" % (role.type, role.name, host.hostname))
        role.update_config(config)
        LOG.info("end update the conf for %s, the role name is: %s (%s)" %(role.type, role.name, host.hostname))

def main(argv):
    setup_logging(LOG_LEVLE)

    test_connect()

    # Argument parsing
    try:
        opts, args = getopt.getopt(argv[1:], "hf:")
    except getopt.GetoptError, err:
        print >> sys.stderr, err
        usage()
        return -1

    host_file = 'hosts_name.txt'
    for option, val in opts:
        if option == '-h':
            usage()
            return -1
        elif option == '-f':
            host_file = val
        else:
            print >> sys.stderr, "Unknown flag:", option
            usage()
            return -1

    if args:
        print >> sys.stderr, "Unknown trailing argument:", args
        usage()
        return -1

    # Decide which host list to use
    hostnames = []
    if host_file is not None:
        hostnames = read_host_file(host_file)
    else:
        hostnames = read_host_file()

    LOG.info("Using host list from file '%s'. Found %d hosts." %
                 (host_file, len(hostnames)))
    # Do work
    do_bulk_config_update(hostnames)
    return 0

def test_connect():
    """ this just test connect to cloudera manager """
    import urllib2
    hosts = "http://%s:%s" %(get_cm_host(), CM_PORT,)
    try:
        urllib2.urlopen(hosts)
    except Exception, ex:
        LOG.info("Can't connect to the Cloudera manager server: %s, Please check that and retry!" % (hosts,))
        sys.exit(-1)

if __name__ == '__main__':
    sys.exit(main(sys.argv))