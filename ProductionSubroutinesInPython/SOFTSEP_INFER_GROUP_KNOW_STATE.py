#
# |∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕
# ⓖ USE SOFT SEPARATION TO INFER GROUP KNOWLEDGE STATE. We find under certain scenarios the computational
#    complexity of Bayesian-network queries leads to unacceptable processing times if data are naïvely 
#    applied to a Bayesian-neteork query. This particularly occurs for  The occurrence of
#    identically-distributed (ID) root vertices provides an opportunity avoid unnecessary computational work.
#
#    ID-root vertices are root vertices sharing identical sets of immediate successors. This is a special case of the 
#    fundamental Bayesian-network conditional-probability semantics for which a variable 𝑋 is independent of its
#    non-descendants given its parents. In the case of Bayesian networks, root vertices are independent of
#    each other if no evidence is applied to "downstream" vertices. In the case in which a pair of vertices
#    with identical sets of immediate descendants, they are identically and independently distributed.
#
#    Our approach here is to "soft-separate" ID-root vertices from the remainder of the subgraph.  We apply here
#    principles from variable elimination approach.  We separate directed graph 𝒢 on which our Bayesian network is
#    based into two complementary subgraphs.  
#
#    ℕ𝕆𝕋𝔸𝕋𝕀𝕆ℕ:  Let ℛ denote our ID-root vertices and 𝒞 their shared immediate 
#    descendants.  We one induce subgraph 𝒢ʹ = 𝒢\ℛ, and a second 𝒢ʺ = ℛ. Moreover, projecting our measured 
#    vertices ℳ onto these subgraphs produces ℳʹ = ℳ∩𝒢ʹ and ℳʺ = ℳ∩ℛ.  Furthermore, let 𝒫 be the predecessors
#    of 𝒞 not contained in ℛ:  𝒫 = parents{𝒞}]\ℛ. Assign finally let 𝒮ʹ be an evidentiary state on ℳ′ and
#    𝒮ʺ an evidentiary state on ℳʺ.  𝒴 are our subjects for which we have measurements, calculate estimates
#    on 𝒢.
#
#    Note that if an evidentiary profile exists for which all variables in 𝒞 are measured — ℳ∩𝒞 = 𝒞 — then the
#    𝒢 is hard-separated. A simpler logic allows to break 𝒢 into induced subgraphs that are d-separated
#    by 𝒞, and handle each separately.
#
#    Our soft-separation logic works as follows. 
#    Ⓐ Reduce the conditional-probabilities for 𝒞. We want to get P(𝒞|𝒫) = ∑P(𝒞|𝒫, ℛ) P(ℛ). This results from
#       straightforward factor multiplication. We characterize P(𝒞|𝒫, ℛ) as our "quiescent" conditional-probability
#       distributions (CPDs) for each variable in 𝒞. We call P(𝒞|𝒫) our "perturbed" CPD.  We must construct a distinct 
#       P(𝒞|𝒫) for every evidentiary state on ℳʺ.  
#    Ⓑ Construct for each evidentiary state ℳʺ a joint distribution function P(ℛ, 𝒞|ℳʺ).  We obtain this again
#       by straightforward factor multiplication. Then perform a Bayesian inversion of the result to get
#       P(ℛ|𝒞, ℳʺ).
#    Ⓒ Construct a Bayesian network on 𝒢ʹ using P(𝒞|𝒫) as the CPD for the 𝒞 vertices instead of the quiescent CPD.
#       Query the Bayesian network for each evidentiary state ℳʹ. We pay particular attention to P(𝒞|ℳʹ), which we
#       use in the next step.
#    Ⓓ Finally, get P(ℛ|ℳʹ, ℳʺ) by factor-multiplication P(ℛ|ℳʹ, ℳʺ) = ∑P(ℛ|𝒞, ℳʺ) P(𝒞|ℳʹ).  This gives us 
#       
#    Observe that tracking evidentiary states in this approach is somewhat more complicated. We previously associated
#    subjects (aka "students") with evidentiary states over the entire 𝒢. Each subject "had" a distinct evidentiary 
#    state. We now associate each subject with two evidentiary states, (𝒮ʹ, 𝒮ʺ). We expect this to often reduce to 
#    total number of Bayesnet queries required.  In generall, #{𝒮} ≥ #{(𝒮ʹ, 𝒮ʺ)}. 
	subgraph_idx = 'SUBGRAPH_221637'
	id_root_subgraph = course_subgraphs.get('SUBGRAPHS').get(subgraph_idx)
	subgraph_group_evid_state = group_evid_state[list(id_root_subgraph.get('SUBGRAPH').nodes())]
	var_states = var_states
	subj_state_prof_bridge = course_subgraphs.get('SUBGRAPHS').get(subgraph_idx).get('subj_state_prof_bridge')
#
#    ⓪ First group learners by evidentiary profile over ℛ∪𝒞 and over 𝒢\ℛ.  We accomplish all of this via
#       dictionary-comprehension logic. 
#       ① We particularly seek instances in which ℳ ⊃ 𝒞. That is our ID-root descendant separating-set variables
#          are measured. These scenarios allow us to avoid the complexity of the soft-separation logic that follows.
#          The separating-set vertices 𝒞 are the keys to the SEPSET_CDF_EDGES object.  We add to each element
#          of SEPSET_CDF_EDGES an additional object id_root_meas_sep, to which we assign logical True if
#          𝒞ᵢ ⊂ ℳ for the iᵗʰ ID-root/successor subspace.
	id_root_subgraph.update({key : val
		for dict_comp in [{'PROFILES' : {prof_key :  {key : val
											for dict_comp in [{'id_root_meas_sep' : {sep_var_key : True
																						if sep_var_key in prof_val.get('LEARNING_STANDARD_ID')
																						else False
																						for (sep_var_key, sep_var_val) in id_root_subgraph.get('SEPSET_CDF_EDGES').items()}},
																prof_val]
																for (key, val) in dict_comp.items()}
										for (prof_key, prof_val) in id_root_subgraph.get('PROFILES').items() }},
						{key : val
							for (key, val) in id_root_subgraph.items()
							if key != 'PROFILES'}]
		for (key, val) in dict_comp.items()})
#
#       ② Associate each subject with an evidentiary profiles with respect to subspaces ℛ and 𝒢\ℛ.  Accomplish this using
#          dictionary-comprehension logic. Introduce to id_root_subghraph dictionary evidentiary-profile items for
#          each subspace.  We invoke internally-defined function groupby_evid_profile to construct our profile groupings.
#          Our arguments are the columns of wide_evid_dataframe — evidentiary-states by subject (aka student) for 
#          the entire space 𝒢 — associated with our subspaces of interest, ℛ∪𝒞, 𝒢\ℛ, respectively.
	id_root_subgraph.update({
		'id_root_prof' :
			{'profiles' : 	groupby_evid_profile(
								wide_evid_dataframe = subgraph_group_evid_state[list(set().union(*[set(id_root_subgraph.get('SEPSET_CDF_EDGES').get(sep_vert).get('id_roots')) 
																					for sep_vert in list(id_root_subgraph.get('SEPSET_CDF_EDGES').keys())]))   ])},
		'id_root_comp_prof' : 
			{'profiles' : groupby_evid_profile(
							wide_evid_dataframe = subgraph_group_evid_state[list(remove_digraph_verts(digraph_object = id_root_subgraph.get('SUBGRAPH'),
																							removed_nodes = set().union(*[set(id_root_subgraph.get('SEPSET_CDF_EDGES').get(sep_vert).get('id_roots')) 
																					for sep_vert in list(id_root_subgraph.get('SEPSET_CDF_EDGES').keys())])).nodes()      )])},
		'sep_prof' :
			{'profiles' : 	groupby_evid_profile(
								wide_evid_dataframe = subgraph_group_evid_state[list(id_root_subgraph.get('SEPSET_CDF_EDGES').keys())   ])},
		'id_root_sep_prof' :
			{'profiles' : 	groupby_evid_profile(
								wide_evid_dataframe = subgraph_group_evid_state[list(id_root_subgraph.get('SEPSET_CDF_EDGES').keys()) +\
																				  list(set().union(*[set(id_root_subgraph.get('SEPSET_CDF_EDGES').get(sep_vert).get('id_roots')) 
																					for sep_vert in list(id_root_subgraph.get('SEPSET_CDF_EDGES').keys())]))])},
			})
#
#       ③ Now, group each subject (aka student) in each evidentiary profile with an evidentiary state. We again use dictionary-comprehension
#          logic.  Under our dictionary items for id_root_desc_prof, ℛ, and id_root_comp_prof, 𝒢\ℛ. We insert new dictionary items
#          representing evidentiary-state groupings of each evidentiary-profile conformee by evidentiary state for each evidentiary profile.
	id_root_subgraph.update(
		{key : val
		for dict_comp in [{id_root_key : {'profiles' : {prof_key : {key : val
																	for dict_comp in [prof_val,
																			 {'states' : groupby_evid_state(
																			 				evid_states_to_be_grouped = subgraph_group_evid_state.loc[prof_val.get('STUDENT_ID'),
																			 																		prof_val.get('LEARNING_STANDARD_ID')], 
																			 				evid_prof_conformees = prof_val.get('STUDENT_ID'), 
																			 				evid_state_cat_map = var_states.drop('UNMEASURED')['CAT_LEVEL_IDX'])
																			 			if len(prof_val.get('LEARNING_STANDARD_ID')) > 0
																			 			else {'EVID_STATE_CONFORMEES' :
																			 								pd.DataFrame(data = {'STUDENT_ID' : prof_val.get('STUDENT_ID')})\
																			 											.assign(EVID_STATE_SIG = 'unmeasured'),
																			 					'EVID_STATE_SIG' : ['unmeasured'],
																			 					'unmeasured' : dict()    }   }] 
														for (key, val) in dict_comp.items()}
																	for (prof_key, prof_val) in id_root_val.get('profiles').items()} }
												for (id_root_key, id_root_val) in id_root_subgraph.items()
												if id_root_key in {'id_root_prof','id_root_comp_prof', 'sep_prof', 'id_root_sep_prof'}},
												{id_root_key : id_root_val
												for (id_root_key, id_root_val) in id_root_subgraph.items()
												if id_root_key not in {'id_root_prof','id_root_comp_prof', 'sep_prof', 'id_root_sep_prof'}}]
		for (key, val) in dict_comp.items()})
#   
#       ④ Develop bridge table associating each subject with evidentiary state, profile.  In terms of our notation, we build a map
#          (𝓎, 𝓂′, 𝓂″, 𝓈′, 𝓈″) explicitliy associating each subject 𝓎 with its evidentairy profiles on 𝒢′ and 𝒢″.  Let 𝔈 represent
#          our subject-evidentiary map.  That is 𝔈 = 𝒴 × ℳ′ × ℳ″ × 𝒮′ × 𝒮″. Our evidentiary map 𝔈 captures each point in this space.
#          Our dictionary of dictionaries structure contains  items for 𝒢′ = 𝒢\ℛ 
#          and for 𝒢″ = ℛ∪𝒞. Each of these contains evidentiary profiles and states. For each profile we have 
#          a dictionary item containing constituent evidentiary states. The evidentiary-states item contains a dataframe 
#          EVID_STATE_CONFORMEES. We want to join these together on STUDENT_ID. In the process, we rename tcolumns
#          so as to differentiate between tables corresponding to 𝒢′ and 𝒢″.
	id_root_subgraph.update(
		{key : val
		for dict_comp in [{id_root_key : 			
						{key : val
						for dict_comp in [{'subj_state_prof_bridge' :
											pd.concat([prof_val.get('states').get('EVID_STATE_CONFORMEES')\
																				.assign(prof_sig = prof_key)\
																				.rename(columns = {'prof_sig' : id_root_key,
																									'EVID_STATE_SIG' : str(id_root_key).replace('_prof', '_state')})
											for (prof_key, prof_val) in id_root_val.get('profiles').items()])},
						id_root_val]
						for (key, val) in dict_comp.items()}
					for (id_root_key, id_root_val) in id_root_subgraph.items()
					if id_root_key in {'id_root_prof','id_root_comp_prof', 'id_root_sep_prof','sep_prof'}},
					{id_root_key : id_root_val
					for (id_root_key, id_root_val) in id_root_subgraph.items()
					if id_root_key not in {'id_root_prof','id_root_comp_prof', 'id_root_sep_prof','sep_prof'}}]
		for (key, val) in dict_comp.items()})
#
	subgraph_obj_idx = 'id_root_comp_prof'
	(id_root_key, id_root_val_val) = (subgraph_obj_idx, id_root_subgraph.get(subgraph_obj_idx))

	id_root_subgraph.update({'subj_state_prof_bridge' : fct.reduce(lambda x, y: pd.merge(left = x, right = y),
																		[id_root_subgraph.get(id_root_key).get('subj_state_prof_bridge')
																		for id_root_key in {'id_root_prof','id_root_comp_prof', 'id_root_sep_prof','sep_prof'}])})
#
#    Ⓐ Reduce the conditional-probabilities for 𝒞. We want to get P(𝒞|𝒫) = ∑P(𝒞|𝒫, ℛ)P(ℛ). We must handle cases
#       in which ℛ∩ℳ = ∅ and ℛ∩ℳ ≠ ∅. This varies by evidentiary profile on 𝒞∪ℛ. 𝙏𝙝𝙞𝙨
#       ① Identify unmeasured ID-root vertices ℛ ⊅ ℳ and 𝒞 ⊅ ℳ. We need to construct P(𝒞 | 𝒫) We need to marginalize out all 
#          ℛ veretices. If the ℛ elements are unmeasured we apply the sum-product factor combination logic using
#          the quiescent P(ℛ).  Otherwise we must apply evidentiary-state-specific measured ℛ for evidentiary 
#          profiles on ℛ∪𝒞. 
#
#          Our SEPSET_CDF_EDGES dictionary item is keyed with respect to 𝒞 and contains an id_roots value listing the 
#          ID root vertices that are predecessors for each vertex in 𝒞. We cycle through our id_root_desc_prof dictionary
#          item and incorporate a meas_id_root_vert item for each.  This is the intersection of the LEARNING_STANDARD_ID
#          object for each evidentiary profile in id_root_desc_prof and the id-root descendants.  Accomplish this using
#          dictionary-comprehension logic. We insert a id_root_desc_meas object into each evidentiary-profile dictionary item.
#          Then, for convenience, we extract the id_root_desc_meas for the evidentiary profiles in ℛ∪𝒞
	id_root_subgraph.update(
		{subgraph_obj_key : 
			{key : val 
						for dict_comp in [{'profiles' : {prof_key : {key : val
														for dict_comp in [{'id_root_desc_meas' : {'meas_sepset' : set(prof_val.get('LEARNING_STANDARD_ID'))\
																													.intersection(id_root_subgraph.get('SEPSET_CDF_EDGES').keys()),
																									'unmeas_sepset' : set(id_root_subgraph.get('SEPSET_CDF_EDGES').keys())\
																														- set(prof_val.get('LEARNING_STANDARD_ID')),
																									'meas_id_root' : set(it.chain(*[val.get('id_roots') for (key, val) in id_root_subgraph.get('SEPSET_CDF_EDGES').items()]))\
																															.intersection(prof_val.get('LEARNING_STANDARD_ID')),
																									'unmeas_id_root' : set(it.chain(*[val.get('id_roots') for (key, val) in id_root_subgraph.get('SEPSET_CDF_EDGES').items()]))\
																															- set(prof_val.get('LEARNING_STANDARD_ID')),
																									'meas_separated' :  set(id_root_subgraph.get('SEPSET_CDF_EDGES').keys())\
																																.issubset(set(prof_val.get('LEARNING_STANDARD_ID'))) } },
																									prof_val]
																	for (key, val) in dict_comp.items()}
												for (prof_key, prof_val) in subgraph_obj_val.get('profiles').items()}},
								{key : val
									for (key, val) in subgraph_obj_val.items()
									if key != 'profiles'}]
					for (key, val) in dict_comp.items()}
						if subgraph_obj_key in {'id_root_prof', 'id_root_sep_prof'}
				else {key : val 
						for dict_comp in [{'profiles' : {prof_key : {key : val
														for dict_comp in [{'id_root_desc_meas' : {'meas_sepset' : set(prof_val.get('LEARNING_STANDARD_ID'))\
																													.intersection(id_root_subgraph.get('SEPSET_CDF_EDGES').keys()),
																									'unmeas_sepset' : set(id_root_subgraph.get('SEPSET_CDF_EDGES').keys())\
																														- set(prof_val.get('LEARNING_STANDARD_ID')),
																									'meas_id_root' : set(it.chain(*[val.get('id_roots') for (key, val) in id_root_subgraph.get('SEPSET_CDF_EDGES').items()]))\
																															.intersection(prof_val.get('LEARNING_STANDARD_ID')),
																									'unmeas_id_root' : set(it.chain(*[val.get('id_roots') for (key, val) in id_root_subgraph.get('SEPSET_CDF_EDGES').items()]))\
																															- set(prof_val.get('LEARNING_STANDARD_ID')),
																									'meas_separated' :  set(id_root_subgraph.get('SEPSET_CDF_EDGES').keys())\
																																.issubset(set(prof_val.get('LEARNING_STANDARD_ID'))) } },
																									prof_val]
																	for (key, val) in dict_comp.items()}
												for (prof_key, prof_val) in subgraph_obj_val.get('profiles').items()}},
								{key : val
									for (key, val) in subgraph_obj_val.items()
									if key != 'profiles'}]
					for (key, val) in dict_comp.items()}
						if subgraph_obj_key in {'id_root_comp_prof'}
			else subgraph_obj_val
		for (subgraph_obj_key, subgraph_obj_val) in id_root_subgraph.items() } )
#
	meas_sep_id_root_desc = {subgraph_obj_key : {prof_key : prof_val.get('id_root_desc_meas')
												for (prof_key, prof_val) in subgraph_obj_val.get('profiles').items()
												if (len(prof_val.get('STUDENT_ID')) > 0 ) and prof_val.get('id_root_desc_meas').get('meas_separated')}
							for (subgraph_obj_key, subgraph_obj_val) in id_root_subgraph.items()
							if subgraph_obj_key in {'id_root_prof', 'id_root_comp_prof', 'id_root_sep_prof'}}
	non_sep_id_root_desc = {subgraph_obj_key : {prof_key : prof_val.get('id_root_desc_meas')
												for (prof_key, prof_val) in subgraph_obj_val.get('profiles').items()
												if (len(prof_val.get('STUDENT_ID')) > 0 ) and not prof_val.get('id_root_desc_meas').get('meas_separated')}
							for (subgraph_obj_key, subgraph_obj_val) in id_root_subgraph.items()
							if subgraph_obj_key in {'id_root_prof', 'id_root_comp_prof', 'id_root_sep_prof'}}
#
#       ② Explicitly construct the subgraphs 𝒞∪ℛ and 𝒢\ℛ.  Assign as dictionary objects under id_root_desc_prof, 
#          id_root_comp_prof, respectively.  Our 𝒞∪ℛ simply involves using internally-defined function remove_digraph_verts
#          to remove the ID-root vertices. For 𝒢\ℛ we induce a subgraph on the vertices of 𝒞 and their predecessors.
	subgraph_obj_idx = 'id_root_comp_prof'
	(subgraph_obj_key, subgraph_obj_val) = (subgraph_obj_idx, id_root_subgraph.get(subgraph_obj_idx))
	prof_idx  = list(subgraph_obj_val.get('profiles').keys())[0]
	(prof_key, prof_val) = (prof_idx, subgraph_obj_val.get('profiles').get(prof_idx))

	id_root_subgraph.update(
		{key : val
		for dict_comp in [{'id_root_comp_prof' : {key : val
									for dict_comp in [id_root_subgraph.get('id_root_comp_prof'),
														{'subgraph' : remove_digraph_verts(digraph_object = id_root_subgraph.get('SUBGRAPH'), 
																							removed_nodes = it.chain(*[val.get('id_roots') for 
																													(key, val) in id_root_subgraph.get('SEPSET_CDF_EDGES').items()])) }]
								for (key, val) in dict_comp.items() }},
						{'id_root_sep_prof' : {key : val
													for dict_comp in [id_root_subgraph.get('id_root_sep_prof'),
																		{'subgraph' : id_root_subgraph.get('SUBGRAPH').subgraph(set(it.chain(*[id_root_subgraph.get('SUBGRAPH').predecessors(sepset_vert)
																																		for sepset_vert in list(id_root_subgraph.get('SEPSET_CDF_EDGES').keys())]))\
																																.union(id_root_subgraph.get('SEPSET_CDF_EDGES').keys())) }]
												for (key, val) in dict_comp.items() }},
						{key : val
							for (key, val) in id_root_subgraph.items()
							if key not in {'id_root_sep_prof', 'id_root_comp_prof'}}]
		for (key, val) in dict_comp.items()})
#
#       ③ When evidentiary profiles measurement-separate the subgraph along our vertices in 𝒞, we can
#          use existing machinery to construct and query the Bayesian network.  We decompose our graph 
#          along the axis of the separating vertices, producing 𝒞∪ℛ and 𝒢\ℛ. We add the results
#          under the profiles for each.
	subgraph_obj_idx = 'id_root_sep_prof'
	(subgraph_obj_key, subgraph_obj_val) = (subgraph_obj_idx, id_root_subgraph.get(subgraph_obj_idx))
	prof_idx = list(subgraph_obj_val.get('profiles').keys())[0]
	(prof_key, prof_val) = (prof_idx, subgraph_obj_val.get('profiles').get(prof_idx))


	id_root_subgraph.update(
		{subgraph_obj_key : 
			{key : val
			for dict_comp in [{'profiles' : {prof_key : 
										{key : val
											for dict_comp in [prof_val,
																exact_infer_group_know_state(
																	bayesnet_digraph = subgraph_obj_val.get('subgraph'),
																	wide_evid_dataframe = group_evid_state.loc[prof_val.get('STUDENT_ID'),
																											list(subgraph_obj_val.get('subgraph'))],
																	evid_prof_conformees = prof_val.get('STUDENT_ID'),
																	var_states = var_states,
																	clust_idx = subgraph_idx)]
										for (key, val) in dict_comp.items()}
											if prof_key in {key for (key, val) in meas_sep_id_root_desc.get(subgraph_obj_key).items() if val.get('meas_separated')}
										else prof_val
										for (prof_key, prof_val) in subgraph_obj_val.get('profiles').items() }},
								{subgraph_obj_key : subgraph_obj_val
									for (subgraph_obj_key, subgraph_obj_val) in subgraph_obj_val.items()
									if subgraph_obj_key != 'profiles'}]
			for (key, val) in dict_comp.items()} 
				if subgraph_obj_key in {'id_root_sep_prof', 'id_root_comp_prof'}
			else subgraph_obj_val
			for (subgraph_obj_key, subgraph_obj_val) in id_root_subgraph.items()  } )
#
#       ④ ℍ𝕖𝕣𝕖'𝕤 𝕥𝕙𝕖 𝕋𝕣𝕚𝕔𝕜𝕪 ℙ𝕒𝕣𝕥!  We apply here our soft-separation logic. We do this for evidentiary profiles such that
#          for which our separating set 𝒞 contains unmeasured verticesm, that is ℳ ⊅ 𝒞. We specifically must
#          reduce the CPDs for the vertices 𝑥 ∈ 𝒞\ℳ, the unmeasured vertices in our separating-set 𝒞. 
#        
#          We constructed an index dictionary id_root_desc_meas. It is a map into id_root_subgraph, our primary information-bearing
#          dictionary item. We pay particular interest to items meas_separated and unmeas_sepset for each evidentiary profile
#          in id_root_desc_meas.  The first tells us where or not the corresponding evidentiary profile does not measure-separate
#          along 𝒞. If meas_separated = 'False', we then need to reduce the CPDs for 𝑥 ∈ 𝒞\ℳ. Our reduction factorizes out
#          the effects of 𝒫|ℳ for each evidentiary state in ℳ∩ℛ. 
#
#          We call another locally-defined subroutine soft_sep_variable_eliminate to produce our reduced CPDs. This subroiutine
#          requires the following information:
#          ⧐ The vertices 𝓍 ∈ 𝒞\ℳ. This information resides in the id_root_desc_meas item for each evidentiary profile.
#             The id_root_desc_meas contains an object unmeas_sepset identifying the 𝓍 ∈ 𝒞\ℳ variables.
#          ⧐ The predecessors to each 𝓍 in 𝒢ʺ. We get this 
#          ⧐ The variable-state space var_states; and
#          ⧐ All of the evidentiary states on ℛ∩ℳ.
#          We need back CPD on 𝒞|𝒫, (ℛ|ℳ). The latter variables are suppressed, in that our reduced CPD C|𝒫 is used to construct
#          a Bayesian network on 𝒫\ℛ.  
#
#          So the inputs 







#≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈
#‼️‼️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️‼️#
#≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈



	set().union(*[set(id_root_subgraph.get('SEPSET_CDF_EDGES').get(sep_vert).get('id_roots')) 
				for sep_vert in list(id_root_subgraph.get('SEPSET_CDF_EDGES').keys())])
	groupby_evid_profile(wide_evid_dataframe = subgraph_group_evid_state[list(set().union(*[set(id_root_subgraph.get('SEPSET_CDF_EDGES').get(sep_vert).get('id_roots')) 
																							for sep_vert in list(id_root_subgraph.get('SEPSET_CDF_EDGES').keys())]))])




# ⓜ GROUP BY EVIDENTIARY STATE.  A set of distinct states for a specific evidentiary profile constitutes an
#    evidentiary stae. We want during "batch-processing" to query the Bayesian network only once for
#    each evidentiary state and reuse that result for al subjects conforming to that state.  This subroutine —
#    very similar in logical flow to groupby_evid_profileile prodices a dictionary of evidentiary states
#    and of subject ID's for conformees to that state.
#def groupby_evid_state(evid_states_to_be_grouped, evid_prof_conformees, evid_state_cat_map):
    #    Begin by creating a hash-function state signature derived from the values of each row
    #    in wide_evid_dataframe.  Also, assign a STUDENT_ID column based on the indices of
    #    our group_by_evid_state dataframe.
    evid_states_to_be_grouped = evid_states_to_be_grouped.loc[evid_prof_conformees]
    evid_state_sig_hash = evid_states_to_be_grouped.apply(lambda x: hash(tuple(x)), axis=1)
    evid_states_to_be_grouped = evid_states_to_be_grouped.assign(EVID_STATE_SIG=list(evid_state_sig_hash))
    evid_states_to_be_grouped = evid_states_to_be_grouped.assign(STUDENT_ID=evid_states_to_be_grouped.index.tolist())
    #
    #    Group STUDENT_ID by EVID_STATE_SIG.  Organize the result as a dictionary.  Use the orient = 'index' specification
    #    in the to_dict function in order to produce dictionary items comprised of lists of STUDENT_IDs.
    evid_state = evid_states_to_be_grouped[['EVID_STATE_SIG', 'STUDENT_ID']].groupby(by='EVID_STATE_SIG',
                                                                                     as_index=True).agg(
        lambda x: list(x)).to_dict(orient='index')
    evid_state_conformees = evid_states_to_be_grouped[['EVID_STATE_SIG', 'STUDENT_ID']]
    #    We now nee to extract from wide_evid_dataframe a list of the evidentiary states for each evidentiary-state
    #    signature.  This requires us to reindex wide_evid_dataframe by EVID_STATE_SIG and to drop the STUDENT_ID column.  We then
    #    drop duplicate rows.
    evid_states_to_be_grouped = evid_states_to_be_grouped.drop(labels='STUDENT_ID',
                                                               axis=1,
                                                               inplace=False)\
                                                            .drop_duplicates()\
                                                            .set_index(keys='EVID_STATE_SIG')
    #
    #    Now work through the wide_evid_dataframe one row at a time.  For each row, create a dictionary of
    #    all measured variables, excluding those for which the state is "UNMEASURED". Update the corresponding entry in evid_state
    #    with the dictionary of LEARNING_STANDARD_IDs and their observed state from wide_evid_dataframe.
    for dict_idx in list(evid_state.keys()):  ## dict_idx = list(evid_state.keys())[0]
        #    The corresponding evidentiary state is the dict_idxʰ row of wide_evid_dataframe.  We exclude the columns
        #    for which the value is "UNMEASURED".
        evid_state_dict_idx = evid_states_to_be_grouped.loc[
            dict_idx, list(evid_states_to_be_grouped.loc[dict_idx,] != 'UNMEASURED')].map(arg=evid_state_cat_map)
        evid_state.get(dict_idx).update({'EVID_STATE': evid_state_dict_idx.to_dict()})
        #
        #    If the evidentiary-state contains no evidentiary measurements, delete it.
        if len(evid_state.get(dict_idx).get('EVID_STATE')) == 0: del evid_state[dict_idx]
    #
    evid_state.update({'EVID_STATE_SIG': list(evid_state.keys())})
    evid_state.update({'EVID_STATE_CONFORMEES': evid_state_conformees})
    #
    #    The dictionary evid_state finally constitutes our returned variable
    return evid_state







#≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈
#‼️‼️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️‼️#
#≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈

	non_sep_id_root_desc = {subgraph_obj_key : {prof_key : prof_val.get('id_root_desc_meas')
												for (prof_key, prof_val) in subgraph_obj_val.get('profiles').items()
												if (len(prof_val.get('STUDENT_ID')) > 0 ) and not prof_val.get('id_root_desc_meas').get('meas_separated')}
							for (subgraph_obj_key, subgraph_obj_val) in id_root_subgraph.items()
							if subgraph_obj_key in {'id_root_desc_prof', 'id_root_comp_prof'}}

	prof_idx = list(non_sep_id_root_desc.get('id_root_desc_prof').keys())[0]
	(prof_key, prof_val) = (prof_idx, subgraph_obj_val.get('profiles').get(prof_idx))

	







#≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈
#‼️‼️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️‼️#
#≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈

ad_hoc_plot_dir = '/Users/nahamlet@us.ibm.com/Desktop' 
for prof_idx in list(id_root_subgraph.get('PROFILES').keys()):			## prof_idx = list(id_root_subgraph.get('PROFILES').keys())[0]
	nx_digraph_for_plot = nx.relabel_nodes(G = id_root_subgraph.get('SUBGRAPH'),
											mapping = dict(zip(id_root_subgraph.get('SUBGRAPH').nodes(),
																[node[3:] for node in id_root_subgraph.get('SUBGRAPH').nodes()]))  )
	nx_digraph_for_plot.add_nodes_from(set([node[3:] for node in id_root_subgraph.get('SUBGRAPH').nodes()])\
						.intersection(set([node[3:] for node in id_root_subgraph.get('PROFILES').get(prof_idx).get('LEARNING_STANDARD_ID')])),
					style = 'filled',
					fillcolor = '#ffc82d')
	agraph_for_plot = nx.nx_agraph.to_agraph(N = nx_digraph_for_plot)
	agraph_for_plot.layout('dot')
	agraph_for_plot.draw(os.path.abspath(os.path.join(ad_hoc_plot_dir,'_'.join(list(session_attributes.values()) + [subgraph_idx, str(prof_idx)]) + '.png')))



ad_hoc_plot_dir = '/Users/nahamlet@us.ibm.com/Desktop' 
for prof_idx in list(id_root_subgraph.get('PROFILES').keys()):			## prof_idx = list(id_root_subgraph.get('PROFILES').keys())[0]
	nx_digraph_for_plot = nx.relabel_nodes(G = id_root_subgraph.get('SUBGRAPH'),
				mapping = lrn_std_id_cd.loc[id_root_subgraph.get('SUBGRAPH'),
											'LEARNING_STANDARD_CD'].to_dict())
	nx_digraph_for_plot.add_nodes_from(set(lrn_std_id_cd.loc[id_root_subgraph.get('SUBGRAPH'),
															'LEARNING_STANDARD_CD'])\
						.intersection(set(lrn_std_id_cd.loc[id_root_subgraph.get('PROFILES').get(prof_idx).get('LEARNING_STANDARD_ID'),
															'LEARNING_STANDARD_CD'])),
					style = 'filled',
					fillcolor = '#ffc82d')
	agraph_for_plot = nx.nx_agraph.to_agraph(N = nx_digraph_for_plot)
	agraph_for_plot.layout('dot')
	agraph_for_plot.draw(os.path.abspath(os.path.join(ad_hoc_plot_dir,'_'.join(list(session_attributes.values()) + [subgraph_idx, str(prof_idx)]) + '.png')))


nx.relabel_nodes(G = id_root_subgraph.get('SUBGRAPH'),
				mapping = lrn_std_id_cd.loc[id_root_subgraph.get('SUBGRAPH'),
											'LEARNING_STANDARD_CD'].to_dict())



{prof_idx : prof_val.get('LEARNING_STANDARD_ID')
	for (prof_idx, prof_val) in id_root_subgraph.get('PROFILES').items()}

