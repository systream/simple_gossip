simple_gossip
=====

Simple implementation of gossip protocol. 
Infect state information around the cluster via gossipping.

## Set data
```erlang
ok = simple_gossip:set({<<"hello world">>, 0}).
```
Or 
```erlang
ok = simple_gossip:set(fun({Text, Counter}) -> 
                    case Counter rem 10 of 
                      0 -> 
                        no_change; 
                      Rem -> 
                        {change, {Text, Counter+(10-Rem)}} 
                    end
                end).
```

## Retrieve data
```erlang
{<<"hello world">>, 0} = simple_gossip:get().
```

## config mode
Have a key-value "storage" over simple gossip. 
A map is being saved, and subscribed for data changes, and store key value pairs into persistent term.

```erlang
<<"default_value">> = simple_gossip:get_cfg(key, <<"default_value">>),
ok = simple_gossip:set_cfg(key, some_random_value),
some_random_value = simple_gossip:get_cfg(key, <<"default_value">>).
```

### Get cluster state. 
```erlang
simple_gossip:status().
```

Result can be
* `{ok, GossipVsn, LeaderNode, NodesInTheCluster}`
* `{error, gossip_vsn_mismatch, LeaderNode, NodesInTheCluster}`:
 Nodes do not agree on cluster state (different GossipVsn). Try again later
* `{error, {timeout, NodesDoNotResponseInTime}, LeaderNode, NodesInTheCluster}`: 
Cannot retrieve information from cluster nodes in time

### Subscribe to cluster changes
```erlang
simple_gossip:subscribe(self()).
```

It will send a message `{data_changed, {<<"hello world">>,0}}` to the given
 process's inbox when the data has been changed.


### Unsubscribe from cluster changes
```erlang
simple_gossip:unsubscribe(self()).
```

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
    
Run tests
-----

    $ rebar3 test
