-module(distributed_ets).
%%internel operations
-export([init/0,loop/1,write/3]).
%%public system operations
-export([start_link/0,stop/0,sync/2,replicate/1,pid/0]).
%public k/v operation
-export([lookup/1,write/2]).

-define(TIMEOUT,30000).
-record(state,{tags2Id,id2Value,value2Id,last_id=0,replicas=[]}).

%%singletom 
start_link()->
	case ?MODULE:pid() of   
		undefined ->
			Pid = spawn_link(?MODULE,init,[]),
			register(?MODULE,Pid);
		_ ->
			{error,already_started}
	end.
stop()->
	call(stop).
lookup(Key)->
	call({lookup,Key}).
write(Key,Value)->
	call({write,Key,Value}).
write(Key,Value,Replicas)->
	call({write,Key,Value,Replicas}).
sync(Node,Flag) when is_boolean(Flag)->
	call({sync,Node,Flag}).
replicate(Node)->
	call({replicate,Node}).
pid()->
	whereis(?MODULE).

%%internel operations
call(Command)->
	call(?MODULE:pid(),Command).
call(Pid,Command)->
	io:format("Sending (~p) to (~p)~n",[Command,Pid]),
	Ref = make_ref(), %% unique id
	Pid ! {self(),Ref,Command},
	receive
		{reply,Ref,Value}->Value
	after ?TIMEOUT ->
		{error,timeout}
	end.

init()->
	TagsId = ets:new(tags2Id,[duplicate_bag,named_table]),
	IdId = ets:new(id2Value,[named_table]),
	ValuesId = ets:new(value2Id,[named_table]),
	loop(#state{tags2Id=TagsId,id2Value=IdId,value2Id=ValuesId}).

loop(State)->
	receive
		{Pid,Ref,stop} when is_pid(Pid),is_reference(Ref)->
			Pid ! {reply,Ref,ok};
			%%(no more loop) means stop
		{Pid,Ref,Command} when is_pid(Pid),is_reference(Ref)->
			%% NewState is state of Server,Return is function return
			{NewState,Return} = handle_call(State,Command),
			Pid ! {reply,Ref,Return},
			loop(NewState);
		{nodedown,Node}->
			Replicas = lists:delete(Node,State#state.replicas),
			io:format("Lost connect with ~p,ending sync~n",[Node]),
			loop(State#state{replicas=Replicas});
		Other ->
			io:format("Unknown message received:~p~n",[Other]),
			loop(State)
	end.

%%ets:lookup(Tab, Key) -> [Object] 
%% may exist same keys
handle_call(State,{lookup,Key})->
	Ids = [Id || {_Tag,Id}<-ets:lookup(State#state.tags2Id,Key)],
	Return = [ets:lookup(State#state.id2Value,Id)||Id <- Ids],
	{State,lists:flatten(Return)};
handle_call(State,{write,Key,Value})->
	handle_call(State,{write,Key,Value,undefined});
handle_call(State,{write,Key,Value,Replica})->
	%%first , find the value de id 
	ValueId = case ets:lookup(State#state.value2Id,Value) of
		[{Value,Id}] -> 
			Id;
		[]-> %%if don`t find id,use last_id + 1
			Id = State#state.last_id + 1,
			ets:insert(State#state.id2Value,{Id,Value}),
			ets:insert(State#state.value2Id,{Value,Id}),
			Id
		end,
	ets:insert(State#state.tags2Id,{Key,ValueId}), %% key -> value`s ids
	%% where to Replicate
	ReplicateTo = lists:filter(fun(N)->N/=Replica end,State#state.replicas),
	io:format("Replicating to ~p~n",[ReplicateTo]),

	rpc:multicall(ReplicateTo,?MODULE,write,[Key,Value,node()]),
	io:format("Tag/Value(~p) written and replicated (~p)~n",[{Key,Value},ReplicateTo]),
	{State#state{last_id=ValueId},ok};

handle_call(State,{sync,Node,false})->
	case lists:any(fun (N) -> N == Node end, State#state.replicas) of
        false ->
            {State, ok};
        true ->

			monitor_node(Node,false),
			Replicas = lists:delete(Node,State#state.replicas),
			%%show where is node  rpc:call(Node,Module,Fun,Args)
			Remote = rpc:call(Node,erlang,whereis,[?MODULE]),
			Remote ! {self(),make_ref(),{sync,node(),false}},
			%%
	%%%%%%%%%%%%%%%%%%%%%%%%%%%
	%%
	%% below I don`t take action to finish
	%%%%%%%%%%%%%%%%%%%%%%%%%%%
	 		io:format("Writes will no longer sync to (~p)~n", [Node]),

    		{State#state{replicas=Replicas}, ok}
    end;
handle_call(State, {sync, Node, true}) ->
	%%at least one Node 
    case lists:any(fun (N) -> N == Node end, State#state.replicas) of
        true ->
            {State, ok};
        false ->
            case net_adm:ping(Node) of
                pong ->
                    monitor_node(Node, true),
                    Replicas = State#state.replicas, 

                    Remote = rpc:call(Node, erlang, whereis, [?MODULE]),
                    % need async to prevent deadlock
                    Remote ! {self(), make_ref(), {sync, node(), true}},
                    io:format("Writes will sync to (~p)~n", [Node]),

                    {State#state{replicas=[Node | Replicas]}, ok};
                pang ->
                    {State, {error, connection_failed}}
            end
    end;
handle_call(State, {replicate, Node}) ->
    case net_adm:ping(Node) of
        pong ->
            replicate(State, Node);
        pang ->
            {State, {error, connection_failed}}
    end.


replicate(State, Node) ->
    % maybe setup sync
    spawn(fun () ->
            io:format("Sync started (~p)...~n", [now()]),
            %%lock tags2Id ets
            ets:safe_fixtable(State#state.tags2Id, true),

            replicate(State, Node, ets:first(State#state.tags2Id)),
            %% unlock tags2Id ets
            ets:safe_fixtable(State#state.tags2Id, false),
            io:format("...Sync finished time:(~p)~n", [now()])
          end),
    {State, started}.

replicate(_State, _Node, '$end_of_table') -> 
    io:format("End of table hit~n", []),
    ok;
replicate(State, Node, Tag) ->
    Ids = [Id || {_, Id} <- ets:lookup(State#state.tags2Id, Tag)],
    Return = [ets:lookup(State#state.id2Value, Id) || Id <- Ids],
    Records = lists:flatten(Return),
    Values = [Value || {_, Value} <- Records],
    io:format("Values ~p~n",[Values]),
    io:format("Replicating tag (~p) and its values (~p) to (~p)~n", [Tag, Values, Node]),
    lists:foreach(fun(Value) ->
        Result = rpc:call(Node, ?MODULE, write, [Tag, Value, node()]),
        io:format("Tag (~p) written to node (~p). Result: (~p)~n", [Tag, Node, Result])
    end, Values),

    replicate(State, Node, ets:next(State#state.tags2Id, Tag)).

%%%%%%%%%%%%%%%%%%%%%%
%%usage:  c(distributed_ets). erl -sname gan  
%%         erl -sname ca
%%       Node ca > distributed_ets:start_link().  每个Node都得有进程 pid
%%       Node gan > distributed_ets:start_link().
%%     先告诉kv server 要sync到某Node,然后在write
%%       Node ca > distributed_ets:write("1","a").
%%同步操作Node ca > distributed_ets:sync('gan@ailen-virtual-machine',true).
%%复制操作 Node ca > distributed_ets:replicate('gan@ailen-virtual-machine').       
%% 也可以进行 key,List的key,value形式
%%  replicate不是覆盖的方式，而是add的模式
%%%%%%%%%%%%%%%%%%%%%%%%
