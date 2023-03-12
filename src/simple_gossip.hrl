%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2020, Systream
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-author("Peter Tihanyi").

-record(rumor, {gossip_version = 1 :: pos_integer(),
                vector_clock = #{} :: #{node() => pos_integer()},
                data :: any(),
                leader :: node(),
                nodes = [] :: [node()],
                max_gossip_per_period = 3 :: pos_integer(),
                gossip_period = 15000 :: pos_integer()
}).

-type rumor() :: #rumor{}.
-export_type([rumor/0]).
