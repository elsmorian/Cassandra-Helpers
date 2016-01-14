# -*- coding: utf-8 -*-
"""
Fabric commands to automate some Cassandra stuff.
"""
import json
import time
import getpass

from fabric.api import run, sudo, execute, task, env, hosts, parallel
from fabric.operations import prompt
from fabric.context_managers import settings
from fabric.state import output
from fabric.utils import abort
from fabric.colors import green, white, yellow, red
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
def _print_cassandra_load():
    """Outputs the current data load of the Cassandra node."""
    result = run('nodetool info | grep Load')
    load = result.split(':')[1].strip()
    node_print("Load:", load)

@parallel
def _print_cassandra_stream_throughput():
    """Outputs the current streaming throughput limit of the Cassandra node."""
    result = run('nodetool getstreamthroughput')
    throughput = result.split(':')[1].strip()
    node_print("Stream throughput limit:", throughput)

@parallel
def _print_cassandra_compaction_throughput():
    """Outputs the current compaction throughput limit of the Cassandra node."""
    result = run('nodetool getcompactionthroughput')
    throughput = result.split(':')[1].strip()
    node_print("Compaction throughput limit:", throughput)

@parallel
def _print_cassandra_memory():
    """Outputs the current memory info of the Cassandra node."""
    result = run("nodetool info | grep 'Heap Memory'")
    heap_memory_string = result.split('\n')[0]
    off_heap_memory_string = result.split('\n')[1]
    heap_memory = heap_memory_string.split(':')[1].strip()
    off_heap_memory = off_heap_memory_string.split(':')[1].strip()
    node_print("Heap Memory (MB):", heap_memory)
    node_print("Off Heap Memory (MB):", off_heap_memory)
    print "\n"

@parallel
def _disable_auto_compactions(keyspace):
    """Disables automatic compactions for the given keyspace."""
    result = run('nodetool disableautocompaction -- {}'.format(keyspace))
    status = red(result.failed) if result.failed else green(not result.failed)
    node_print("Compaction disabled for '{}':".format(keyspace), status)

@parallel
def _enable_auto_compactions(keyspace):
    """Enables automatic compactions for the given keyspace."""
    result = run('nodetool enableautocompaction -- {}'.format(keyspace))
    status = red(result.failed) if result.failed else green(not result.failed)
    node_print("Compaction enabled for '{}':".format(keyspace), status)


@parallel
def _set_stream_throughput_limit(limit=200):
    """Enables the stream throughput limit to the given throughput in mbit/s."""
    run('nodetool setstreamthroughput -- {}'.format(limit))
    _print_cassandra_stream_throughput()

@parallel
def _set_compaction_throughput_limit(limit=64):
    """Enables the compaction throughput limit to the given throughput in MB/s."""
    run('nodetool setcompactionthroughput -- {}'.format(limit))
    _print_cassandra_compaction_throughput()

@parallel
def _flush_memtables():
    """Flushes Cassandra's memtables to disk."""
    result = run('nodetool flush')
    status = red(result.failed) if result.failed else green(not result.failed)
    node_print("Memtables flushed:", status)

def _drain_cassandra_node():
    """Drains a Cassandra node."""
    with settings(warn_only=True):
        result = run('nodetool drain')
    sucess = not result.failed
    status = green(sucess) if sucess else red(sucess)
    node_print("Drained:", status)

def _restart_cassandra_node():
    """Restarts the Cassandra process on a node."""
    result = sudo('service cassandra restart')
    node_print("Restarted:", result)

def _check_cassandra_is_running():
    """Checks Cassandra is running on a node."""
    result = run('service cassandra status')
    if result.failed or '* Cassandra is running' not in result:
        abort(yellow(result))
    node_print("Status:", result)

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
def show_cluster_load():
    """Show the data load for each Cassandra node in the cluster."""
    print "\n"
    print white("Data Load:")
    execute(_print_cassandra_load, hosts=get_all_cassandra_nodes())
    print "\n"

@task
def show_cluster_streaming_throughput_limit():
    """Show the streaming throughput limit for each Cassandra node."""
    print "\n"
    print white("Streaming throughput Limit:")
    execute(_print_cassandra_stream_throughput, hosts=get_all_cassandra_nodes())
    print "\n"

@task
def show_cluster_compaction_throughput_limit():
    """Show the compaction throughput limit for each Cassandra node."""
    print "\n"
    print white("Compaction throughput Limit:")
    execute(_print_cassandra_compaction_throughput, hosts=get_all_cassandra_nodes())
    print "\n"

@task
def show_cluster_memory():
    """Show the memory info for each Cassandra node in the cluster."""
    print "\n"
    print white("Cluster Memory:")
    execute(_print_cassandra_memory, hosts=get_all_cassandra_nodes())
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
    if keyspace is None:
        abort(yellow("Please specify a keyspace!"))

    print white("Enabling Automatic Compactions:")
    execute(
        _enable_auto_compactions,
        keyspace=keyspace,
        hosts=get_all_cassandra_nodes(),
    )
    print "\n"

@task
def set_streaming_throughput_limit(limit=200):
    """Sets the streaming throughput limit across the Cassandra cluster."""
    print "\n"
    print white("Setting streaming throughput limit to {}:".format(limit))
    execute(
        _set_stream_throughput_limit,
        limit=limit,
        hosts=get_all_cassandra_nodes(),
    )
    print "\n"

@task
def disable_streaming_throughput_limit():
    """Disable the streaming throughput limit across the Cassandra cluster."""
    print "\n"
    print white("Disabling streaming throughput limit:")
    execute(
        _set_stream_throughput_limit,
        limit=0,
        hosts=get_all_cassandra_nodes(),
    )
    print "\n"

@task
def set_compaction_throughput_limit(limit=64):
    """Sets the compaction throughput limit across the Cassandra cluster."""
    print "\n"
    print white("Setting compaction throughput limit to {}:".format(limit))
    execute(
        _set_compaction_throughput_limit,
        limit=limit,
        hosts=get_all_cassandra_nodes(),
    )
    print "\n"

@task
def disable_compaction_throughput_limit():
    """Disable the compaction throughput limit across the Cassandra cluster."""
    print "\n"
    print white("Disabling compaction throughput limit:")
    execute(
        _set_compaction_throughput_limit,
        limit=0,
        hosts=get_all_cassandra_nodes(),
    )
    print "\n"

@task
def flush_memtables():
    """Flushes Cassandra's Memtables to disk on all nodes in the cluster."""
    print "\n"
    print white("Flushing Memtables:")
    execute(_flush_memtables, hosts=get_all_cassandra_nodes())
    print "\n"

@task
def run_cluster_rolling_restart():
    """Restarts each Cassandra node in the cluster one by one."""
    print "\n"
    print white("Running rolling cluster restart:")
    print "\n"
    for data_center, data_center_nodes in CLUSTER_INFO.iteritems():
        for node in data_center_nodes:
            print white("* {}:\n".format(node))
            env.password = getpass.getpass(prompt="Enter sudo password: ")
            execute(_drain_cassandra_node, hosts=[node])
            time.sleep(10)
            execute(_restart_cassandra_node, hosts=[node])
            time.sleep(10)
            execute(_check_cassandra_is_running, hosts=[node])
            print white("Waiting 60 seconds for gossip and hinted handoffs..")
            time.sleep(60)
            print "\n"
    print white("Rolling restart complete!")
