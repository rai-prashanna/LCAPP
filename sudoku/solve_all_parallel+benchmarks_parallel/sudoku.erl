-module(sudoku).

-export([benchmarks/0, solve_all/0, solve/1]).

%% Your module must at least export these functions; uncomment the line below
%% once you are done implementing them.
%% Note that you might need to export other functions as well depending on your
%% implementation; in that case, you can add them into either of the export statements. 
% -export([benchmarks_parallel/0,solve_all_parallel/0,solve_all_parallel/1])

%%
%% Includes for PropEr, EUnit libraries
%%
-ifdef(PROPER).
-include_lib("proper/include/proper.hrl").
-endif.
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%%
%% Type definitions
%%
-type elem()   :: 0..9.
-type matrix() :: [[elem()]].
-type name()   :: atom().
-type puzzle() :: {name(), matrix()}.
-type musecs() :: non_neg_integer().

-type solution()   :: matrix() | 'no_solution'.
-type bm_results() :: [{name(), float()}].

%%
%% Benchmarking code
%%
-define(EXECUTIONS, 42).
-define(PROBLEMS,  "sudoku_problems.txt").
-define(SOLUTIONS, "sudoku_solutions.txt").

-spec benchmarks() -> {musecs(), bm_results()}.
benchmarks() ->
  {ok, Problems} = file:consult(?PROBLEMS),
  timer:tc(fun () -> benchmarks_parallel(Problems,[]) end).

benchmarks_parallel([],Accumaltor)-> Accumaltor;
benchmarks_parallel([{Name,M}],Accumaltor)-> 
  [{Name,bm(fun() -> solve(M) end)}|Accumaltor];
benchmarks_parallel([FirstPuzzle,SecondPuzzle|TailPuzzles],Accumaltor)-> 
  {Name1, M1}=FirstPuzzle,
  {Name2, M2}=SecondPuzzle,
  Parent=self(),
  Tag = make_ref(),
  spawn_link(fun()->
  Parent!{Tag,benchmarks_parallel(TailPuzzles,[{Name1,bm(fun() -> solve(M1) end)}|Accumaltor])} end),
  Gs={Name2,bm(fun() -> solve(M2) end)},
  receive {Tag,Ys}->  
  [Gs|Ys]
 end.



bm(F) ->
  {T, _} = timer:tc(fun () -> repeat(?EXECUTIONS, F) end),
  T / ?EXECUTIONS / 1000.

-spec repeat(non_neg_integer(), fun(() -> term())) -> 'ok'.
repeat(0, _) -> ok;
repeat(N, F) when N > 0 ->
  _ = F(), repeat(N-1, F).


%%
%% solve all puzzles in the (hardcoded) input file
%%
-spec solve_all() -> [{name(), solution()}].
solve_all() ->
  {ok, Puzzles} = file:consult(?PROBLEMS),
  solve_all_parallel(Puzzles,[]).

solve_all_parallel([],Accumaltor) -> Accumaltor;
solve_all_parallel([FirstPuzzle,SecondPuzzle|TailPuzzles],Accumaltor)-> 
  {Name1, M1}=FirstPuzzle,
  {Name2, M2}=SecondPuzzle,
  Parent=self(),
  Tag = make_ref(),
  spawn_link(fun()->
  Parent!{Tag,solve_all_parallel(TailPuzzles,[{Name1,solve(M1)}|Accumaltor])} end),
  Gs={Name2,solve(M2)},
  receive {Tag,Ys}->  
  [Gs|Ys]
 end.



%%
%% solve a Sudoku puzzle
%%
-spec solve(matrix()) -> solution().
solve(M) ->
  Solution = solve_refined(refine(fill(M))),
  case valid_solution(Solution) of
    true -> Solution;
    false -> % in correct puzzles should never happen
      exit({invalid_solution, Solution})
  end.

solve_refined(M) ->
  case solved(M) of
    true -> M;
    false -> solve_one(guesses(M))
  end.

solve_one([]) -> no_solution;
solve_one([M]) -> solve_refined(M);
solve_one([M|Ms]) ->
  case solve_refined(M) of
    no_solution -> solve_one(Ms);
    Solution -> Solution
  end.


%% is a puzzle solved?
solved(no_solution) -> true;
solved(M) -> lists:all(fun solved_row/1, M).

solved_row(Row) -> lists:all(fun is_decided/1, Row).

is_decided(no_solution) -> true;
is_decided(X) -> 1 =< X andalso X =< 9.


%% check solutions for validity
valid_solution(no_solution) -> true;
valid_solution(M) -> valid_rows(M) andalso valid_rows(transpose(M)) andalso valid_rows(blocks(M)).

valid_rows(M) -> lists:all(fun valid_row/1, M).

-define(NINE, [1, 2, 3, 4, 5, 6, 7, 8, 9]).

valid_row(Row) -> lists:usort(Row) =:= ?NINE.


%% fill blank entries with a list of all possible values 1..9
fill(M) ->
  Nine = ?NINE,
  [[case is_decided(X) of 
    true -> X; 
    false -> Nine 
    end 
  || X <- Row] || Row <- M].


%% refine entries which are lists by removing numbers they are known
%% not to be
refine(M) ->
  NewM =
    refine_rows(
      transpose(
	refine_rows(
	  transpose(
	    unblocks(
	      refine_rows(
		blocks(M))))))),
  if 
    M =:= NewM -> M;
    true -> refine(NewM)
  end.

refine_rows(no_solution) ->
  no_solution;
refine_rows(M) ->
  Refined = [refine_row(R) || R <- M],
  case lists:member(no_solution, Refined) of
    true -> no_solution;
    false -> Refined
  end.

refine_row(Row) ->
  Entries = entries(Row),
  NewRow =
    [if is_list(X) ->
    	 case X -- Entries of
    	   [] -> no_solution;
    	   [Y] -> Y;
    	   NewX -> NewX
    	 end;
    	true -> X
     end
    || X <- Row],

  %% check we didn't create a duplicate entry, or any entry that has
  %% no solution anymore; cheat by adding 'no_solution' to new entries
  NewEntries = [no_solution|entries(NewRow)],
  case length(lists:usort(NewEntries)) =:= length(NewEntries) of
    true -> NewRow;
    false -> no_solution
  end.

entries(Row) -> [X || X <- Row, is_decided(X)].

is_wrong(no_solution) -> true;
is_wrong(_) -> false.


%% how hard is the puzzle?
hard(M) -> lists:sum([lists:sum([hardness(X) || X <- Row]) || Row <- M]).

hardness(X) when is_list(X) -> length(X);
hardness(_) -> 0.


%% choose a position {I, J, Guesses} to guess an element, with the
%% fewest possible choices
guess(M) ->
  Nine = ?NINE,
  {_, I, J, X} =
    lists:min([{length(X), I, J, X}
	       || {I, Row} <- lists:zip(Nine, M),
		  {J, X} <- lists:zip(Nine, Row),
		  is_list(X)]),
  {I, J, X}.

%% given a matrix, guess an element to form a list of possible
%% extended matrices, easiest problem first.
guesses(M0) ->
  {I, J, Guesses} = guess(M0),
  Ms = [refine(update_element(M0, I, J, G)) || G <- Guesses],
  SortedGuesses = lists:sort([{hard(M), M} || M <- Ms, not is_wrong(M)]),
  [G || {_, G} <- SortedGuesses].

%% -------------------------------------------------------------------
%% Matrix operations (with some of their testing code)

transpose(no_solution) -> no_solution;
transpose([Row]) -> [[X] || X <- Row];
transpose([Row|M]) -> [[X|Xs] || {X, Xs} <- lists:zip(Row, transpose(M))].

-ifdef(PROPER).
prop_transpose() ->
  ?FORALL({M, N}, {nat(), nat()},
	  ?FORALL(Mat, vector(M+1, vector(N+1, elem())),
		  transpose(transpose(Mat)) =:= Mat)).
-endif.

update_element(M, I, J, G) -> update_nth(I, update_nth(J, G, lists:nth(I, M)), M).

update_nth(I, X, Xs) ->
  {Pre, [_|Post]} = lists:split(I-1, Xs),
  Pre ++ [X|Post].

-ifdef(PROPER).
prop_update() ->
  ?FORALL(L, list(integer()),
	  ?IMPLIES(L =/= [],
		   ?FORALL(I, choose(1, length(L)),
			   update_nth(I, lists:nth(I, L), L) =:= L))).
-endif.

%% map a matrix to a list of 3x3 blocks, each represented by the list
%% of elements in row order

triples([A,B,C|D]) -> [[A,B,C]|triples(D)];
triples([]) -> [].

blocks(no_solution) -> no_solution;
blocks(M) ->
  Blocks = [triples(X) || X <- transpose([triples(Row) || Row <- M])],
  lists:append([[lists:append(X) || X <- B] || B <- Blocks]).

unblocks(no_solution) -> no_solution;
unblocks(M) ->
  case lists:member(no_solution, M) of
    true -> no_solution;
    false ->    
      [lists:append(X)
       || X <- transpose([lists:append(Y)
                          || Y <- [[triples(T) || T <- Ts] || Ts <- triples(M)]])]
  end.

-ifdef(PROPER).
prop_blocks() ->
  ?FORALL(M, vector(9, vector(9, elem())), unblocks(blocks(M)) =:= M).
-endif.

%% -------------------------------------------------------------------
%% EUnit tests below

-ifdef(TEST).
sanity_test_() -> [test_benchmarks()].

test_benchmarks() ->
  {ok, Problems} = file:consult(?PROBLEMS),
  {ok, Solutions} = file:consult(?SOLUTIONS),
  ZipF = fun ({Name, P}, {Name, S}) -> {P, S} end,
  Pairs = lists:zipwith(ZipF, Problems, Solutions), % assumes order is the same
  [?_assertEqual(Sol, solve(Problem)) || {Problem, Sol} <- Pairs].
-endif.
