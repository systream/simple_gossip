%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2020, Systream
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------

-record(rumor, {gossip_version = 1 :: pos_integer(),
                vector_clock :: simple_gossip_vclock:vclock(),
                data :: any(),
                leader :: node(),
                nodes = [] :: [node()],
                max_gossip_per_period = 2 :: pos_integer(),
                gossip_period = 15000 :: pos_integer()
}).

-record(rumor_head, {gossip_version = 1 :: pos_integer(),
                     vector_clock = simple_gossip_vclock:vclock()
}).

-type rumor() :: #rumor{}.
-type rumor_head() :: #rumor_head{}.
