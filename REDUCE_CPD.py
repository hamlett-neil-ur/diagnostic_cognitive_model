#
#
# |∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕
# ⓖ REDUCE CONDITIONAL PROBABILITY DISTRIBUTION. Start with a 'quiescent' conditional-probability distribution. Marginalize out 
#    a specified conditioning variable with a specified distribution.  Return the reduced probability distribution.
#
#    Our inputs are:
#    ⧐ digraph, A Networkx digraph object containing the vertex and its predecessors;
#    ⧐ var_states, the variable state;
#    ⧐ vertex, The target vertex for which the reduced conditional probability is desired; and 
#    ⧐ cond_dist, A dictionary object containing baseline conditional-probability distributions for the target
#                 vertex and its conditioning variables.
#
#    Our logic performs the following steps.
#    Ⓐ Create a list of predecessors to vertex.
#    Ⓑ Build the conditional-probability table for vertex and for its predecessors for which probability distributions
#       are specified.
#    Ⓒ Merge the CPTs to perform factor multiplication.
#    Ⓓ Perform the factor multiplication. 
#    Ⓔ Marginalize out the specified vertices.  Use groupby-sum.
#    Ⓕ Return the ID-root-reduced probability-distribution as a list.
#
#    Ⓐ Create a list of predecessors to vertex.
#
def reduce_cpd(digraph, var_states, vertex, cond_dist):
	parent_verts = digraph.predecessors(vertex)
#
#    Ⓑ Build the conditional-probability table for vertex. Use internally-defined function state_df
#       to build the variable states for each variable. We then use a constant-unit-valued join key
#       to make our merge perform like a cartesian product.  We drop unneded attributes from the colum.
	base_cpt = fct.reduce(lambda x, y: pd.merge(left = x, right = y),
									[state_df(state_var = vert,
											var_states = var_states.drop('UNMEASURED',axis = 0)['CAT_LEVEL_IDX'])
									for vert in digraph.predecessors(vertex) + [vertex]])\
					.assign(MEAS = cond_dist.get(vertex))\
					.drop(labels = 'join_key',
						axis = 1)\
					.rename(columns = {'MEAS' : 'P_' + vertex})
#
#    Ⓒ Merge the CPTs to perform factor multiplication. We simultaneously build the CPTs for the ID-root vertices.
	factor_prod = fct.reduce(lambda x, y: pd.merge(left = x, right = y),
							[base_cpt] +\
							[state_df(state_var = vert,
										var_states = var_states.drop('UNMEASURED',axis = 0)['CAT_LEVEL_IDX'])\
									.assign(MEAS = cond_dist.get(vert))\
									.drop(labels = 'join_key',
											axis = 1)\
									.rename(columns = {'MEAS' : 'P_' + vert})
							for vert in list(cond_dist.keys())
							if vert != vertex] )
#
#    Ⓓ Perform the factor multiplication. 
	factor_prod = factor_prod.assign(factor_prod = factor_prod[[fact_col for fact_col in factor_prod.columns.tolist()
																if 'P_' in fact_col]].product(axis = 1).tolist())\
							.drop(labels = [fact_col for fact_col in factor_prod.columns.tolist()
											if 'P_' in fact_col],
								axis = 1)\
							.rename(columns = {'factor_prod' : 'P_' + vertex})
#
#    Ⓔ Marginalize out the ID-root vertices.
	red_cpt = factor_prod.groupby(by = list(set(parent_verts) - set(list(cond_dist.keys()))) + [vertex],
								axis = 0,
								as_index = False)['P_' + vertex].sum()\
							[list(set(parent_verts) - set(list(cond_dist.keys()))) + [vertex] + ['P_'+ vertex]]\
							.sort_values(by = list(set(parent_verts) - set(list(cond_dist.keys()))) + [vertex],
										axis = 0)
#
#    Ⓕ Return the ID-root-reduced probability-distribution as a list.
	return red_cpt['P_' + vertex].tolist()
#