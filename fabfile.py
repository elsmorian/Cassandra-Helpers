# -*- coding: utf-8 -*-
"""
Fabric commands to automate some Cassandra stuff.
"""
import json

from fabric.api import run, execute, task, env, hosts, parallel
from fabric.state import output
from fabric.colors import green, white
from fabric.network import ssh

CASSANDRA_CLUSTER_DETAILS = 'cassandra_cluster_details.json'

env.user = 'admin'
ssh.util.log_to_file("paramiko.log", 10)
output.running = False
output.status = False
output.stdout = False

with open(CASSANDRA_CLUSTER_DETAILS) as data_file:
    CLUSTER_INFO = json.load(data_file)


@parallel
def _print_cassandra_version():
    """Outputs the version of Cassandra currently running on a node."""
    version = run('/usr/sbin/cassandra -v')
    node_print("Version:", version)

@parallel
def _disable_auto_compactions(keyspace):
    """Disables automatic compactions for the given keyspace."""
    result = run('nodetool disableautocompaction -- {}'.format(keyspace))
    status = red(result.failed) if result.failed else green(not result.failed)
    node_print("Compaction disabled for '{}'".format(keyspace), status)

@parallel
def _enable_auto_compactions(keyspace):
    """Enables automatic compactions for the given keyspace."""
    result = run('nodetool enableautocompaction --{}'.format(keyspace))
    status = red(result.failed) if result.failed else green(not result.failed)
    node_print("Compaction enabled for '{}'".format(keyspace), status)


def get_all_cassandra_nodes():
    """Returns a list of all the nodes in the Cassandra cluster."""
    node_list = []
    for data_center, data_center_nodes in CLUSTER_INFO.iteritems():
        node_list.extend(data_center_nodes)
    return node_list

def node_print(output_text, output):
    print "{} {}\t {} {}".format(
        white("Node:"),
        green(env.host),
        white(output_text),
        green(output),
    )


@task
def show_cluster_versions():
    """Show the version of Cassandra running on all nodes in the cluster."""
    print "\n"
    print white("Cassandra Versions:")
    execute(_print_cassandra_version, hosts=get_all_cassandra_nodes())
    print "\n"

@task
def disable_auto_compactions(keyspace=None):
    """Disables automatic compaction for the given keyspace in the cluster."""
    print "\n"
    print white("Disabling Automatic Compactions:")
    execute(
        _disable_auto_compactions,
        keyspace=keyspace,
        hosts=get_all_cassandra_nodes(),
    )
    print "\n"

@task
def enable_auto_compactions(keyspace=None):
    """Enables automatic compaction for the given keyspace in the cluster."""
    print "\n"
    print white("Enabling Automatic Compactions:")
    execute(
        _enable_auto_compactions,
        keyspace=keyspace,
        hosts=get_all_cassandra_nodes(),
    )
    print "\n"
