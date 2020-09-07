simple_gossip
=====

Simple implementation of random gossip protocol. 
Infect state information around the cluster via gossipping.

## Set data
```erlang
simple_gossip:set(<<"hello world">>).
```

## Retrieve data
```erlang
<<"hello world">> = simple_gossip:get().
```

### Get cluster state. 
```erlang
simple_gossip:status().
```

Result can be
* `mismatch`: nodes do not agree on cluster state (try again later)
* `{error, timeout}`: Cannot retrieve information from cluster nodes in time
* `{ok, 74, 'node12@cluster', ['node1@cluster', 'node9@cluster', 'node12@cluster']}`: Cluster agreed on the state. Second parameter is the gossip version, third is the claimant node, and the fifth is the list of cluster nodes.  

### Join node to cluster
```erlang
simple_gossip:join('test@cluster').
```

### Remove node from cluster
```erlang
simple_gossip:leave('test@cluster').
```


Build
-----

    $ rebar3 compile
