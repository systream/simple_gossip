%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2020, Systream
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------

-record(rumor, {cluster_vclock = simple_gossip_vclock:vclock(),
                data_vclock = simple_gossip_vclock:vclock(),
                data :: any(),
                leader :: node(),
                nodes = [] :: [node()],
                max_gossip_per_period = 3 :: pos_integer(),
                gossip_period = 15000 :: pos_integer()
}).

-type rumor() :: #rumor{}.
-export_type([rumor/0]).
