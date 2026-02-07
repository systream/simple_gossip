simple_gossip
=============

Simple implementation of a gossip protocol for Erlang. It allows for infecting state information around a cluster via gossiping, maintaining eventual consistency.

Features
--------

-   **Gossip Protocol**: Efficiently propagates state updates across the cluster.
-   **Eventual Consistency**: Ensures all nodes converge to the same state.
-   **Subscriptions**: Allows processes to subscribe to data changes.
-   **Cluster Management**: Easy node joining and leaving.

Installation
------------

Add `simple_gossip` to your `rebar.config` dependencies:

```erlang
{deps, [
    {simple_gossip, "1.6.1"} %% Check hex.pm for the latest version
]}.
```

Configuration
-------------

Configure the application in your `sys.config`:

```erlang
[
  {simple_gossip, [
    %% Can be auto, sync, async.
    %% - auto: Async by default, switches to sync if events stack up (default).
    %% - sync: Synchronous event notification.
    %% - async: Asynchronous event notification.
    {event_mode, auto}
  ]}
].
```

Usage
-----

### Basic Key-Value Operations

**Set data:**

```erlang
ok = simple_gossip:set({<<"hello world">>, 0}).
```

**Retrieve data:**

```erlang
{<<"hello world">>, 0} = simple_gossip:get().
```

**Advanced Updates:**
You can update data based on the current value using a function. This is useful for counters or conditional updates.

```erlang
ok = simple_gossip:set(fun({Text, Counter}) ->
    case Counter rem 10 of
        0 ->
            no_change;
        Rem ->
            {change, {Text, Counter + (10 - Rem)}}
    end
end).
```

### Configuration Mode (Persistent Term)

Use `simple_gossip` as a distributed key-value store backed by `persistent_term` for fast reads.

```erlang
%% Get a configuration value with a default
<<"default_value">> = simple_gossip:get(key, <<"default_value">>).

%% Set a configuration value
ok = simple_gossip:set(key, some_random_value).

%% Retrieve the updated value
some_random_value = simple_gossip:get(key, <<"default_value">>).
```

### Cluster Management

**Get Cluster Status:**

```erlang
simple_gossip:status().
```

Possible results:
-   `{ok, GossipVsn, LeaderNode, NodesInTheCluster}`: Healthy state.
-   `{error, gossip_vsn_mismatch, LeaderNode, NodesInTheCluster}`: Nodes do not agree on cluster state (different GossipVsn). Transient state, should resolve itself.
-   `{error, {timeout, NodesDoNotResponseInTime}, LeaderNode, NodesInTheCluster}`: Communication issues with some nodes.

**Join a Node:**

```erlang
simple_gossip:join('test@cluster').
```

**Leave a Node:**

```erlang
simple_gossip:leave('test@cluster').
```

### Subscriptions

Receive notifications when data changes.

**Subscribe:**

```erlang
simple_gossip:subscribe(self()).
```

It will send a message `{data_changed, {Data, Version}}` to the process inbox.
Example: `{data_changed, {<<"hello world">>, 0}}`.

**Unsubscribe:**

```erlang
simple_gossip:unsubscribe(self()).
```

Build
-----

    $ rebar3 compile

Run Tests
---------

    $ rebar3 test

License
-------

Apache License 2.0
