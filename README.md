# Cassandra-Helpers
Some Fabric tools to make Cassandra admin a little easier

Rewrite the ```cassandra_cluster_details.json.example``` file to match your cassandra data centers and nodes, and then use the below to carry out common tasks / find information of the whole cluster quickly.

    $> fab --list

    Fabric commands to automate some Cassandra stuff.

    Available commands:

    disable_auto_compactions                  Disables automatic compaction for the given keyspace in the cluster.
    disable_compaction_throughput_limit       Disable the compaction throughput limit across the Cassandra cluster.
    disable_streaming_throughput_limit        Disable the streaming throughput limit across the Cassandra cluster.
    enable_auto_compactions                   Enables automatic compaction for the given keyspace in the cluster.
    flush_memtables                           Flushes Cassandra's Memtables to disk on all nodes in the cluster.
    run_cluster_rolling_restart               Restarts each Cassandra node in the cluster one by one.
    set_compaction_throughput_limit           Sets the compaction throughput limit across the Cassandra cluster.
    set_streaming_throughput_limit            Sets the streaming throughput limit across the Cassandra cluster.
    show_cluster_compaction_throughput_limit  Show the compaction throughput limit for each Cassandra node.
    show_cluster_load                         Show the data load for each Cassandra node in the cluster.
    show_cluster_memory                       Show the memory info for each Cassandra node in the cluster.
    show_cluster_streaming_throughput_limit   Show the streaming throughput limit for each Cassandra node.
    show_cluster_versions                     Show the version of Cassandra running on all nodes in the cluster.
