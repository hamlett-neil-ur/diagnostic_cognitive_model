# |∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕
# ⓡ TRANSFORM EVIDENCE OF LEARNING INTO MASTERY DIAGNOSES.  Orchestrate the application of evidence of learning from
#    a cohort of learners enrolled in an academci course to a Bayesian network.  Efficiency through exploitation of
#    graph-local redundancy in evidentiary states of cohort members. By this we mean that if groups of students exist
#    who share the same evidentiary states for a set of learning standards within the span of a graph, we should only
#    query the Bayesian network once for all of those students.
#
#    We search for this redundancy through the following logic.
#    🄰 Find an find the within-evidentiary-range induced subgraph of the course graphical neighborhood. This
#       induced subgraph consists of the learning standards within ±2 graphical steps of any measured
#       learning standard for any subject (aka student).
#    🄱 Break the within-evidentiary-range induced subgraph into 'bite-sized' estimation subgraphs using the
#       decompose_digraph logic.
#    🄲 Group subjects into evidentiary profiles for each digraph. An evidentiary profile distinguishes
#       groups of subjects who share identical sets of measured vertices.
#    🄳 Identify the evidentiary state — the measured-variable state for each measured variable — for
#       each subject in a given evidentiary profile.
#    🄴 Query the Bayesian network for each evidentiary state.
#    🄵 Assemble the Bayesian-network query responses and return the result.
#
#    The inputs to this function include:
#    ⧐ wide_evid_dataframe, a wide-format dataframe for which columns corresponding to learning standards,
#      rows correspond to learner identities, and values represent individual=learner evidentiary states
#      for distinct learning standards;
#    ⧐ digraph_edge_list contains a list of edges on which the Bayesian network is based; and
#    ⧐ var_states, a variable-state-label indexed set of integer indices for the variable states.
#    The evid_prof_dict is produced by the internally defined subroutine groupby_evid_profileile.
#
#    The subroutine returns an update evid_prof_dict to two dataframes are added:
#    ⧐ KNOWLEDGE_STATE  captures marginal, conditional probabilities that each learner — distinguished by a distinct
#      identity, is in a knowledge state with respecti to a specific learning standard; and
#    ⧐ CLUST_EXECUTION_TIME contains execution-time statistics for each Bayesian-network query
#      required to produce KNOWLEDGE_STATE.
#
#    The subroutine logic passes its one evididentiary-proficiency object at a time to another
#    subroutine est_know_state_for_evid_prof.  It returns dictionary objects that are added as
#    updates to evid_prof_dict.  The subroutine returns the updated evid_prof_dict.
#
#
	vert_list = vertex_list
	digraph_edge_list = course_map_edge
	var_states = mastery_color
	analysis_case_parameters = pd.DataFrame(data = {'ATTRIBUTE' : list(session_attributes.keys()),
													'VALUE' : list(session_attributes.values())})\
									.set_index(keys = 'ATTRIBUTE',
												drop = False)



	wide_evid_dataframe = build_wide_evid_state_table(
		evid_state_long= vert_list.rename(columns= {'SIH_PERSONPK_ID_ST': 'STUDENT_ID',
			  										'EVIDENCE_OF_LEARNING_SID': 'WORK_PRODUCT_TITLE'})\
                                            .assign(LEARNING_STANDARD_ID = list(map(str, vert_list['LEARNING_STANDARD_ID']))),
		digraph_edge_list=digraph_edge_list,
		enrollees=vert_list[['SIH_PERSONPK_ID_ST']] \
					  .drop_duplicates().rename(columns={'SIH_PERSONPK_ID_ST': 'STUDENT_ID'}),
		admiss_score_partitions=var_states)

	group_evid_state = wide_evid_dataframe
	digraph_edge_list = digraph_edge_list
	var_states = var_states
# def evaluate_group_know_state(group_evid_state, digraph_edge_list, var_states):
	#    🄰 Find an find the within-evidentiary-range induced subgraph of the course graphical neighborhood.
	#       ⑴ First find from wide_evid_dataframe all of the measured learning standards.
	graph_measured_verts = group_evid_state.columns[(group_evid_state != 'UNMEASURED').any(axis=0)].tolist()
	#
	#       ⑵ Identify the induced digraph constrained by the evidentiary range.
	course_nhbd_graph = nx.DiGraph()
	course_nhbd_graph.add_edges_from(digraph_edge_list[['CONSTITUENT_LEARNING_STD_ID',
														'LEARNING_STANDARD_ID']].to_records(index=False) \
									 .tolist())
	in_range_digraph = build_induced_inrange_graph(meas_list=graph_measured_verts,
												   evid_radius=2,
												   course_nhbd_graph=course_nhbd_graph)
	#
	#    🄱 Break the within-evidentiary-range induced subgraph into 'bite-sized' estimation subgraphs.
	#       Internally defined decompose_digraph accomplishes this.  We then invoke internally-defined
	#       classify_subgraph to classify graphs according to their separability.
	course_subgraphs = decompose_digraph(composite_digraph=in_range_digraph,
										 measured_verts=graph_measured_verts)
	course_subgraphs = classify_subgraph(
								subgraph_components = {subgraph_idx : {'SUBGRAPH' : subgraph_obj.get('SPANNING_SUBGRAPH')}
										for (subgraph_idx, subgraph_obj) in  course_subgraphs.items()})
	#
	#    🄲 Associate subjects (𝘢𝘬𝘢 students) in our group evidentiary-state table with evidentiary profiles, states with respect to each subgraph.  
	#       ① First group subjects by evidentiary profile for all subgraphs.  We embed within our dictionary object a taxonomical
	#          branch 'PROFILES'. This 
	course_subgraphs.get('SUBGRAPHS').update(
				{ subgraph_key :
						{ key : val
							for dict_comp in [subgraph_val,
											  {'PROFILES' : {key : val 
											  					for (key, val) in groupby_evid_profile(
											  										wide_evid_dataframe = group_evid_state[list(subgraph_val.get('SUBGRAPH').nodes()) ]).items()
											  					if len(val.get('LEARNING_STANDARD_ID')) > 0}}]
						for (key, val) in dict_comp.items()}
					for (subgraph_key, subgraph_val) in course_subgraphs.get('SUBGRAPHS').items()})
	#
	#       ② Truncate each subgraph according to its evidentiary profile.  Only retain verticers within a specified evidentiary range
	#          of any vertex measured by the profile.  Insert the resulting IN_RANGE_DIGRAPH object.  Only 
	course_subgraphs.get('SUBGRAPHS').update({subgraph_key : {key : val
																	for dict_comp in 
																		[{'PROFILES' : {prof_key : {key : val 
																			for dict_comp in [{'IN_RANGE_DIGRAPH' : 
																									build_induced_inrange_graph(
																										meas_list = prof_val.get('LEARNING_STANDARD_ID'),
																										evid_radius = 2,
																										course_nhbd_graph = subgraph_val.get('SUBGRAPH') )},
																								prof_val ]
																				for (key, val) in dict_comp.items() }
																			for (prof_key, prof_val) in subgraph_val.get('PROFILES').items()}},
																		{key : val
																			for (key, val) in subgraph_val.items()
																			if key != 'PROFILES'}]
																for (key, val) in dict_comp.items()}
																	if subgraph_key not in course_subgraphs.get('SUBGRAPH_SEPARABILITY').get('ID_ROOT_SEPARABLE')
															else subgraph_val
							for (subgraph_key, subgraph_val) in course_subgraphs.get('SUBGRAPHS').items()})
	#
	#
	#    🄳 Query the Bayesian network for each separability scenario.  Our Bayesian-network query algorithms return
	#       dictionaries of two values:  BAYESNET_QUERY_RESP contains the marginalized conditional probabilties for each
	#       vertex in the in-range DiGraph, and CLUSTER_EXEC_TIME the statistics associated with the Bayesian-network
	#       queries. These span all evidentiary states associated with each evidentiary profile.  We insert the returned dictionary
	#       as a new value for each profile index
	#       ① Small nonseparable subgraphs.         ## subgraph_idx = course_subgraphs.get('SUBGRAPH_SEPARABILITY').get('NON_SEPARABLE_SMALL')[0]
	course_subgraphs.get('SUBGRAPHS').update({subgraph_key :
			{key : val
					for dict_comp in [{subgraph_key : subgraph_val
								for (subgraph_key, subgraph_val) in subgraph_val.items()
								if subgraph_key != 'PROFILES'},
							{'PROFILES' : {prof_key : {key : val
													for dict_comp in [exact_infer_group_know_state(
																				bayesnet_digraph = prof_val.get('IN_RANGE_DIGRAPH'),
																				wide_evid_dataframe = group_evid_state.loc[prof_val.get('STUDENT_ID'),
																														 list(prof_val.get('IN_RANGE_DIGRAPH').nodes())],
																				evid_prof_conformees = prof_val.get('STUDENT_ID'),
																				var_states = var_states,
																				clust_idx = subgraph_key),
																	prof_val]
												for (key, val) in dict_comp.items()}
											for (prof_key, prof_val) in subgraph_val.get('PROFILES').items()}}]
			for (key, val) in dict_comp.items()}
					if subgraph_key in course_subgraphs.get('SUBGRAPH_SEPARABILITY').get('NON_SEPARABLE_SMALL')
			else {key : val
					for dict_comp in [{subgraph_key : subgraph_val
								for (subgraph_key, subgraph_val) in subgraph_val.items()
								if subgraph_key != 'PROFILES'},
							{'PROFILES' : {prof_key : {key : val
													for dict_comp in [approx_infer_group_know_state(
																				bayesnet_digraph = prof_val.get('IN_RANGE_DIGRAPH'),
																				wide_evid_dataframe = group_evid_state.loc[prof_val.get('STUDENT_ID'),
																														 list(prof_val.get('IN_RANGE_DIGRAPH').nodes())],
																				evid_prof_conformees = prof_val.get('STUDENT_ID'),
																				var_states = var_states,
																				clust_idx = subgraph_key),
																	prof_val]
												for (key, val) in dict_comp.items()}
											for (prof_key, prof_val) in subgraph_val.get('PROFILES').items()}}]
			for (key, val) in dict_comp.items()}
					if subgraph_key in course_subgraphs.get('SUBGRAPH_SEPARABILITY').get('NON_SEPARABLE_LARGE')
			else subgraph_val
		for (subgraph_key, subgraph_val) in course_subgraphs.get('SUBGRAPHS').items()})







#≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈
#‼️‼️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️‼️#
#≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈
wide_evid_dataframe = subgraph_group_evid_state[list(set().union(*[set(id_root_subgraph.get('SEPSET_CDF_EDGES').get(sep_vert).get('id_roots')) 
																for sep_vert in list(id_root_subgraph.get('SEPSET_CDF_EDGES').keys())]))]

# |∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕
# ⓛ GROUP SUBJECTS BY EVIDENTIARY PROFILE.  Evidentiary profile refers to varibales for which evidentiary
#    measurements are available.  Our input variable is a wide-table-format dataframe
#    whose indices are subject ids and whose columns are evidentairy variables. When no evidence is
#    available for a subject regarding a given variable, the corresponding data-frame cell is
#    assigned 'UNMEASURED'.  We return an index whose keys are evidentiary-profile hash-signature
#    variables. Each dictionary item is itself a dictionary for which the keys are
#    a list of measured evidentiary variables and a list of subject id's for for which
#    the evidentiary-varaible list exactly coincides with the set for which the subject has measurements.
def groupby_evid_profile(wide_evid_dataframe):
    #    We create an "indicator-function" table containing values of False for cells containing "UNMEASURED"
    #    and True otherwise.
    indicator_evid_table = wide_evid_dataframe != 'UNMEASURED'
    #
    #    Next assign an evidentiary-profile hash signature by calculating a hash function across the
    #    rows of indicator_evid_table, our evidentiary indicator table.  Also assign values to column STUDENT_ID
    #    based on row indices of indicator_evid_table.
    indicator_evid_table['EVID_PROF_SIG'] = list(indicator_evid_table.apply(lambda x: hash(tuple(x)), axis=1))
    indicator_evid_table['STUDENT_ID'] = indicator_evid_table.index.values.tolist()
    #
    #    Group STUDENT_ID by EVID_PROF_SIG.  Organize the result as a dictionary.  Use the orient = 'index' specification
    #    in the to_dict function in order to produce dictionary items comprised of lists of STUDENT_IDs.
    evid_prof = indicator_evid_table[['EVID_PROF_SIG', 'STUDENT_ID']].groupby(by='EVID_PROF_SIG',
                                                                              as_index=True)\
                                                                    .agg(lambda x: list(x)).to_dict(orient='index')
    #
    #    We now nee to extract from indicator_evid_table a list of the evidentiary profiles for each evidentiary-profile
    #    signature.  This requires us to reindex indicator_evid_table by EVID_PROF_SIG and to drop the STUDENT_ID column.  We then
    #    drop duplicate rows.
    indicator_evid_table = indicator_evid_table.drop(labels='STUDENT_ID',
                                                     axis=1,
                                                     inplace=False).drop_duplicates().set_index(keys='EVID_PROF_SIG')
    #
    #     Finally, loop through each row of indicator_evid_table, updating its corresponding dictionary
    #     item in evid_prof with a list of measured variables.
    for dict_idx in list(evid_prof.keys()):  ## dict_idx = list(evid_prof.keys())[0]
        evid_prof.get(dict_idx).update({'LEARNING_STANDARD_ID':
                                            list(indicator_evid_table.columns.values[
                                                     np.where(indicator_evid_table.loc[dict_idx])])})
        #
        #      Delete the dict_idxᵗʰ if the corresponding evidentiary profile contains measured variables.
        #if len(evid_prof.get(dict_idx).get('LEARNING_STANDARD_ID')) == 0: del evid_prof[dict_idx]

    #
    #     Our dictionary evid_prof is now our returned value.
    return evid_prof





#≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈
#‼️‼️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️‼️#
#≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈


