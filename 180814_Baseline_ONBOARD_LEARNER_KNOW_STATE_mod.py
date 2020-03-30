# coding: utf-8
## PURPOSE:  APPLY A BAYESIAN NETWORK TO ESTIMATE LEARNER KNOWLEGE STATE GIVEN EVIDENCE OF LEARNING
## MEASUREMENT.  The scope of the estimation is bounded by the learning map associated with a course unit and the
## course section.  An external file SESSION_ATTRIBUTES specifies the scope of the query.
##
## MAJOR STEPS IN THE ALGORITHM LOGIC.
## ① Set workspace parameters and read in working files.  We specifically require the following:
##      ⪧ SESSION_ATTRIBUTES guides the case study on which we focus.
##      ⪧ COURSE_ENROLL contains the enrollment and responsible educator.
##      ⪧ EoL_MEAS contains the learners' evidence of learning (EoL) measurements.
##      ⪧ MASTERY_LEVEL_CAT contains relationships between learners' measured learning evidence and their implied knowledge states.
##      ⪧ GRAPH_CLUST_N_UNIT_MAP_JDF contains the joint distrubition functions (JDF) for Nᵗʰ cluster of connected vertices
##         within COURSE_MAP_EDGE_LIST. We employ this to get the in-scope vertices.
## ② Window the EoL_MEAS learning-measurement table. Retain only records corresponding to subjects (students) for whom
##      SIH_PERSONPK_ID_ST exists in EoL_MEAS. Also, limit the LEARNING_STANDARD_ID to the variables specfied within the columns of
##      GRAPH_CLUST_N_UNIT_MAP_JDF.  Also, sort the EoL_MEAS by ASSESSMENT_DATE and retain only the most-recent in cases
##      of multiple measurements of LEARNING_STANDARD_IDs for distinct subjects.
## ③ Apply MASTERY_LEVEL_CAT to impute hard-decision knowledge-state estimates for each EoL_MEAS.
## ④ Identify the evidence states in EoL_MEAS.  We introduce here three aspects of our framework.
##      ⓐ KNOWLEDGE STATE represents the estimated extent of mastery for an individual learner with respect to all LEARNING_STANDARD_ID
##           attributes from the proficiency model.
##      ⓑ EVIDENTIARY PROFILE contains all of the observed variables from which that estimate is derived.
##      ⓒ EVIDENTIARY STATE specifies the actual state for each evidentiary-profile variable for a specific learner.
##      ⓓ PROFICIENCY_PROFILE spans all proficiency-model targets (aka "learning standards") of interest, including those for which
##           observed evidence is available and those for which previsions are sought.
##      We extract during this stage the evidentiary profile and evidentiary state for each subject (learner, student) from EoL_MEAS.
##       Categorize learners according to evidentiary profile and evidentiary state. Also identify by cluster for each unit-submap cluster
##       of connected vertices:
##       ⓐ Observed variables from the evidentiary profile on which we condition the submap-cluster's JDF; and
##       ⓑ The target variables for which we obtain marginal CDFs conditioned on evidentiary states in the evidentiary profile.
##      on each observed evidentiary state.  Marginalize the resulting conditional distribution with respect to each target variable to obtain
##      a distribution of knowledge-state probabilities for each observed evidentiary state.
## ⑥ Associate the LEARNING_STANDARD_ID-marginalized CDFs for each learner with the measured knowledge state to get a complete
##      probability distribution for each variable.  Append to LEARNER_KNOW_STATE.  Reshape to wide-table format so that LEARNER_KNOW_STATE
##      contains for each SIH_PERSONPK_ID_ST × LEARNING_STANDARD_ID pair a row of conditional probability distributions regarding the LEARNER's state.
#
# ⓪ Initialize the environment. Import the needed packages. and functions.
import time
import csv
import timeit as tit
from datetime import datetime
import pandas as pd
from pandas.api.types import CategoricalDtype
import numpy as np
import networkx as nx
from pomegranate import *
import re
import ast
import json
from sqlalchemy import create_engine
import ibm_db
import ibm_db_dbi
import logging
import os
from pgmpy.models import BayesianModel
from pgmpy.factors.discrete.CPD import TabularCPD
from pgmpy.inference import VariableElimination
from collections import Counter
from logging.handlers import TimedRotatingFileHandler
import sqlalchemy.exc as exc
import warnings

#################################################################################################################################
# |∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—|∕—\|#
##################⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇ LOCAL FUNCTIONS ⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇#############
# We use several local-utility functions in order to make our code compact.  We define these here.
# ⓐ CONDITIONAL-PROBABILITY DATA-FRAME STATE DEFINITION.  The ConditionalProbabilityTable function by which Pomegranate
#    affixes conditional probabilities to graph vertices requires explicit articulation of all of the variable states
#    associated with each conditional-probability measure. Our CPT_LONG input table handles this implicitly.  We infer the
#    conditioning-, target-variable states by knowing the assumed structure employed in construction of CPT_LING.
#
#    Constructing explicit variable-state tables occurs through recursive cartesin products of the variable states.
#    We accomplish this in two stages.  First, state_df consntructs a dataframe containing the variable states for a specified
#    variable, as well as a constant-value join key.
def state_df(state_var, var_states):
    return pd.DataFrame(data={state_var: var_states,
                              'join_key': list(np.repeat(a=1,
                                                         repeats=len(var_states)))})


#
# |∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕
# ⓑ CONDITIONAL-DEPENDENCY STRUCTURE OF GRAPH.  We repeatedly require the conditional-dependency structure
#    associated with an edge list for a directed graph on which our Bayesian network is based. We seek a dictionary
#    object with entries for each target variable. Our dictionary entries consist of the observation variables
#    and the in-degree valency, or constituent count.
def cond_prob_struc(edge_list, vert_list):
    #	The observation-variables result from a groupby operation to the edge-list dataframe.  We then count the
    #   conditioning variables rsulting from the groupby in order to get CONSTITUENT_COUNT.
    struc_cond_prob = edge_list.groupby(by='LEARNING_STANDARD_ID',
                                        as_index=True)['CONSTITUENT_LEARNING_STD_ID'].apply(list).to_frame()
    struc_cond_prob['CONSTITUENT_COUNT'] = struc_cond_prob['CONSTITUENT_LEARNING_STD_ID'].agg(lambda x: len(x)).tolist()
    struc_cond_prob['LEARNING_STANDARD_ID'] = struc_cond_prob.index.values.tolist()
    #
    #   Now, left-outer-join struc_cond_prob onto single-column dataframe containing a unique occurrence of all of the
    #   vertices in the graph.
    struc_cond_prob = pd.merge(right=struc_cond_prob,
                               left=pd.DataFrame(data=vert_list,
                                                 columns=['LEARNING_STANDARD_ID']),
                               how='left')
    struc_cond_prob = struc_cond_prob.set_index(keys='LEARNING_STANDARD_ID')
    #
    #   Assign zero to non-finite CONSTITUENT_COUNT records in struc_cond_prob. Return the result as a dictionary.  The returned
    #   result contains an entry for each vertex in the graph, a list of observation variables on which each is conditioned, and
    #   CONSTITUENT_COUNT, the number of observation
    struc_cond_prob.loc[pd.isnull(struc_cond_prob['CONSTITUENT_COUNT']), 'CONSTITUENT_COUNT'] = 0
    return struc_cond_prob.to_dict(orient='index')


#
# |∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕
# ⓒ BUILD POMEGRANATE CONDITIONAL/DISCRETE-PROBABILITY INPUTS.  Construct a dictionary of the conditional/discrete
#    probabilities that comprise the input variables to the discrete/conditional probability tables. Our input variables are
#    ⧐ CPT_LIST, the long-table conditional probability measures without explicit variable states;
#    ⧐ A dictionary capturing the conditional-independence structure produce by internal function cond_prob_struc; and
#    ⧐ A list of variable states.
#    We produce a dictionary with an entry for each graph vertex.  Each dictionary entry contains:
#    ⧐ The conditional/discrete-probability function argument for the pomegranate funciton;
#    ⧐ A unit-length string indicating with which case it is associated; and
#    ⧐ A list of conditioning variables used for the conditional-probability case.
def pom_cond_probs(cond_dependence, var_cat_indices):
    #    ⅰ. Begin by identifying the graph that are root-vertices, characterized by zero-valued CONSTITUENT_COUNT or
    #       non-root vertices, characterized by finite constituent count.
    cond_probs = dict()
    root_verts = list(
        dict_key for dict_key in list(cond_dependence.keys()) if cond_dependence[dict_key]['CONSTITUENT_COUNT'] == 0.)
    nonroot_verts = list(
        dict_key for dict_key in list(cond_dependence.keys()) if cond_dependence[dict_key]['CONSTITUENT_COUNT'] != 0.)
    #
    #    ⅱ. Handle the root vertices, first. These discrete-probability terms are simplest.  They are simply dictionaries
    #       whose indices are observed-variable states and whose values are the observed-variable probabilities.
    for vert_idx in root_verts:  ## vert_idx = root_verts[0]
        cond_prob_vert_idx = dict()
        cond_prob_vert_idx.update({'COND_PROB': dict(zip(var_cat_indices,
                                                         CPT_LIST.loc[CPT_LIST['IS_ROOT'] == '1'][
                                                             'MEAS'].values.tolist()))})
        cond_prob_vert_idx.update({'NODE_TYPE': 'discrete'})
        cond_probs[vert_idx] = cond_prob_vert_idx
    #
    #   ⅲ. Next handle the non-root vertices. These nodes are specified by conditional-probability tables. We must build an exhaustive
    #      list variable states.  This results from a cartesian-product recursion.  Once finished with the recursion, enforce column
    #      sequence and drop the join_key attribute.  Also, add the MEAS value from cpt_list.
    for vert_idx in nonroot_verts:  ## vert_idx = nonroot_verts[0]
        cond_prob_vert_idx = dict()
        var_state_list_vert_idx = state_df(vert_idx, var_cat_indices)
        for const_idx in cond_dependence.get(vert_idx).get('CONSTITUENT_LEARNING_STD_ID'):
            var_state_list_vert_idx = pd.merge(left=var_state_list_vert_idx,
                                               right=state_df(const_idx, var_cat_indices))
        var_state_list_vert_idx = var_state_list_vert_idx[
            [vert_idx] + cond_dependence.get(vert_idx).get('CONSTITUENT_LEARNING_STD_ID')]
        var_state_list_vert_idx['MEAS'] = \
            CPT_LIST.loc[
                CPT_LIST['CONSTITUENT_COUNT'] == str(int(cond_dependence.get(vert_idx).get('CONSTITUENT_COUNT')))][
                'MEAS'].values.tolist()
        cond_prob_vert_idx.update({'COND_PROB': var_state_list_vert_idx.values.tolist()})
        cond_prob_vert_idx.update({'NODE_TYPE': 'conditional'})
        cond_prob_vert_idx.update(
            {'CONSTITUENT_LIST': cond_dependence.get(vert_idx).get('CONSTITUENT_LEARNING_STD_ID')})
        cond_probs[vert_idx] = cond_prob_vert_idx
    #
    #   ⅳ. Return the cond_probs.
    return cond_probs


#
# |∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕
# ⓓ CONSTRUCT GRAPH-VERTEX CLASSIFICATION TABLE. Contruct a dictionary in which the vertices are decomposed into
#    two lists, one for root vertices and one for non-root vertices.
def graph_vert_class(edge_list, vert_list):
    vert_class = dict()
    #
    #    First, find singleton vertices.  These are vertices not appearing in the edge list. We will add these to the ROOT_VERTS list.
    graph_conn_verts = set(edge_list['CONSTITUENT_LEARNING_STD_ID']).union(set(edge_list['LEARNING_STANDARD_ID']))
    singleton_verts = set(vert_list) - graph_conn_verts
    #
    #    The root vertices appear in the CONSTITUENT_LEARNING_STD_ID attribute but not in the LEARNING_STANDARD_ID.  Leaf
    #    vertices similarly appear in LEARNING_STANDARD_ID but not in CONSTITUENT_LEARNING_STANDARD_ID.
    vert_class.update(
        {'ROOT_VERTS': list(set(edge_list['CONSTITUENT_LEARNING_STD_ID']) - set(edge_list['LEARNING_STANDARD_ID']))})
    vert_class.update({'ROOT_VERTS': list(set(vert_class.get('ROOT_VERTS')).union(singleton_verts))})
    vert_class.update(
        {'LEAF_VERTS': list(set(edge_list['LEARNING_STANDARD_ID']) - set(edge_list['CONSTITUENT_LEARNING_STD_ID']))})
    #
    #    The remaining vertices are non-root vertices.
    vert_class.update({'NONROOT_VERTS': list(graph_conn_verts - set(vert_class.get('ROOT_VERTS')))})
    #
    return vert_class


#
# |∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕
# ⓔ TRANSFER GRAPH-VERTEX LABEL FROM NON_INTEG_VERT TO INTEG_VERT LIST IN VERT_INTEG_CHECKLIST.  We must track our
#    progression — vertex-by-vertex — through the graph as we build up conditional-probability tables. This function
#    moves graph-vertex labels from the non-integrated vertex list to the integrated vertex list as the conditonal
#    -probability tables are built up.
def vert_merg_checkoff(graph_vert, vert_checklist):
    vert_checklist.update({'INTEG_VERTS': vert_checklist.get('INTEG_VERTS') + [graph_vert]})
    vert_checklist.update({'NON_INTEG_VERTS': list(set(vert_checklist.get('NON_INTEG_VERTS')) - set([graph_vert]))})
    return vert_checklist


#
# |∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕
# ⓕ PROCEDURE:  BUILD A POMEGRANATE CONDITIONAL-PROBABILITY INPUT AND ADD IT TO POM_COND_PROB_TABLES.  This is a procedure, and doesn't
#    return a value. Our input is a graph vertex. For that vertex we extract the conditional conditional-probability object from COND_PROB_TABLES,
#    the prerequisite learning standards if NODE_TUPE is 'conditional', and all of the immediate-processor POM_COND_PROB_TABLE objects from
#    corresponding to the immediate predecessors.  We then construct the objects and add the result to POM_COND_PROB_TABLE.
def build_pom_cond_tab(graph_vert):
    #   Ascertain the node-type — whether root vertex for which we use DiscreteDistribution or non-root for which we use ConditionalProbabilityTable.
    #   The NODE_TYPE entry in the corresponding dictionary entry of COND_PROB_TABLES contains this information.
    NODE_TYPE_graph_vert = COND_PROB_TABLES.get(graph_vert).get('NODE_TYPE')
    #
    #   If NODE_TYPE_graph_vert is 'discrete' we simply apply COND_PROB object to DiscreteDistribution and store as update to
    #   POM_COND_PROB_TABLE.
    if NODE_TYPE_graph_vert == 'discrete':
        POM_COND_PROB_TABLES.update(
            {graph_vert: DiscreteDistribution(COND_PROB_TABLES.get(graph_vert).get('COND_PROB'))})
    #
    #   If NODE_TYPE is not 'discrete', we need to use the ConditionalProbabilityTable. This requires additional arguments.
    else:
        #   For convenience and compactness, first get the list of observation vertices from COND_PROB_TABLES.
        OBS_VERTS_graph_vert = COND_PROB_TABLES.get(graph_vert).get('CONSTITUENT_LIST')
        #
        #   Now update POM_COND_PROB_TABLES by calling ConditionalProbabilityTable.  It builds the corresponding conditional-probability table
        #   using as arguments the COND_PROB value in COND_PROB_TABLES corresonding to graph_vert and the PROM_COND_PROB_TABLES values
        #   identified in OBS_VERTS_graph_vert.
        POM_COND_PROB_TABLES.update(
            {graph_vert: ConditionalProbabilityTable(COND_PROB_TABLES.get(graph_vert).get('COND_PROB'),
                                                     list(POM_COND_PROB_TABLES.get(dict_idx)
                                                          for dict_idx in OBS_VERTS_graph_vert))})


#
# |∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕
# ⓖ FIND MERGE-READY NON-ROOT VERTICES.  We proceed from root to leaf in buidling up our conditional-probability
#    tabes.  All immediate-predecessor conditional-probability tables must be built up before we can build up
#    a Pomegranate conditional-probability object.  This subroutine identifies all non-root vertices for which this condition
#    is satisfied. It takes as its arguments merged_vert, a list of merged vertices from the vertex-integration checklist managed
#    by vert_merg_checkoff above, and constituent_vert_dictionary, listing the constituent vertices for all merged vertices.
#    Merge-ready vertices are those for which the the set-difference between constit_vert_dict and merged_vert is null.
def merge_ready(merged_vert, constit_vert_dict):
    merge_ready = list()
    for key_idx in list(constit_vert_dict.keys()):  ## key_idx = list(constit_vert_dict.keys())[0]
        if len(set(constit_vert_dict[key_idx]) - set(merged_vert)) == 0: merge_ready = merge_ready + [key_idx]
    return merge_ready


#
# |∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕
# ⓖ QUERY STAR-GRAPH BAYESIAN NETWORK.  Instances occur when the Pomegranate Bayesian network produces slow convergence behavior.
#    Empirical examination points to a diagnosis of slow numerical convergence of the Loopy Belief-Propagation (LPB) algorithm
#    when conditional indendepencies exist in the subgraph for which calculations are performed. The LBP logic appears tio
#    execute substantial numbers of iterative calculations searching for non-existent dependencies.
#
#    This slow convergence occurs particularly when:
#    ⧐ All evidence coincides with root vertices; and
#    ⧐ At least one unit-valency unmeasured vertex exists in the graph.
#    We find for such circumstances that exact-inference calculations return quiescent — "prior", "base-case", zero-valued
#    constituent-count — marginalized conditional probability distributions for these scenarios.  This coincides with
#    theoretical expectations. Given the logic of conditional independence, zero-valency root vertices in a graph are
#    indepent of each other if no evidence is applied to common successors.
#
#    We deal here with three cases.
#    CASE ①: The measured vertices ℳ d-separate the graph 𝓖. We can under these scenarios break the graph into
#             smaller subgraphs, each of which can be handled separately.
#    CASE ②:  All measured vertices ℳ are contained in set of root vertices root(𝒳), where 𝒳 is all veriables
#             in the network. That is, ℳ ⊆ root(𝒳). In this case, non-measurerd root bvertices root(𝒳)\ℳ are identically and
#             independently distributed. The marginalized CPDs for root(𝒳)/ℳ  furthermore exactly coincide with
#             the quescent CPDs for root vertices.
#    CASE ③: Measured vetices ℳ are not conatined in root(𝒳) — ℳ ⊈ root(𝒳) — and elements of root(𝒳) share common
#             descendants. That is, desc(𝒳ᵢ) = desc(𝒳ⱼ) = ..., where {𝒳ᵢ, 𝒳ⱼ, ...} ⊆ root(𝒳). Although where {𝒳ᵢ, 𝒳ⱼ, ...}
#             are no longer strictly independent, they are identically distriuted. We can threfore calculate the CPDs for
#             desc(𝒳ᵢ) | ℳ and then calculate once the CPDs occuring for {𝒳ᵢ, 𝒳ⱼ, ...} ⊆ root(𝒳)\ℳ.
#    CASE ④:  None of CASES ⓵ – ⓷ appliy.
#
#    Obvously CASE ① and CASE ② are mutually exclusive. Measured vertices ℳ do not d-separate a graph if ℳ ⊆ root(𝒳).
#    CASE ① and CASE ③ are similarly mutually exclusive. Occurrences of CASE ② and CASE ③ obviously can coincide.
#
#
#    CASE ①:  Our logic with this deals with these scenarios as follows.
#    🄰 Create a clean copy of our input digraph.
#    🄱 Remove the measured vertices ℳ from the graph and caulculate the number of connected components in 𝒳\ℳ.
#    🄲 If the number of connected components exceeds unity, construct each of the connected components
#       as a digraph — including the measured vertices 𝒳.
#    🄳 Perform an independendent Bayesian-network query for each connected componenty.
#
#    CASE ②:  Our logic with this deals with these scenarios as follows.
#    🄰 If conditions above exist, remove the unmeasured, unit-valency root vertices from
#       the subgraph on which the Bayesian-network query is based. Construct a response
#       dataframe for these variables using quiescent CPDs from CPT_LIST, our global conditional-probability
#       table.
#    🄱 Construct and query the Bayesian network for each conditionally-independent subgraph.
#    🄲 Concatenate the Bayesian-network query responses with the quiescent CPDs.
#
#    CASE ⓷: We apply variable-elimination logic to isolate the effects of unmeasured
#             root vertices when evidence is applied to descendants.
#    🄰 Identify identically-distributed, unmeasured root vertices. These are root vertices sharing common sets
#       of descendants.
#    🄱 Marginalize out from the common descendants the affects
#
#    Our subroutine inputs are:
#    ⧐ bayesnet_digraph is a networkx DiGraph object representing the subgraph under consideration;
#    ⧐ wide_evid_dataframe is a dataframe of evidentiary states for all subjects (aka "students");
#    ⧐ evid_prof_conformees contains a list of all STUDENT_IDs for subjects conforming to the evidentiary
#      profile;
#    ⧐ var_states contains the specifications for mastery-level category assignbments; and
#    ⧐ clust_idx is a scalar-valued label indicating the subgraph subject to analysis.
#    This subroutine serves as an API to the Bayesian-network query subroutines  exact_infer_group_know_state
#    and approx_infer_group_know_state, which construct the Bayesian networks and handle queries thereof
#    conditioned on each evidentiary state.
#
#    The subroutine returns a dictionary object comprised of two data frames:
#    ⧐ BAYESNET_QUERY_RESP contains the query-response results, marginalized CPDs for each (STUDENT_ID,
#      LEARNING_STANDARD_ID) couple; and
#    ⧐ CLUSTER_EXEC_TIME contains the execution-time statistics for each Bayesian-network query.
#
#
def query_star_graph_Bayesnet(bayesnet_digraph, wide_evid_dataframe, evid_prof_conformees, var_states, clust_idx):
    # ⓿ GENERAL PREPARATION.
    #    ⑴ Create a copy of our Bayesian-network DAG.  Extract an edge list as a dataframe. We subsequently require this
    #       to classify vertices as roots, leafs using internally-defined graph_vert_class. Function.
    red_star_graph = nx.DiGraph()
    red_star_graph.add_edges_from(list(bayesnet_digraph.edges()))
    red_star_graph_edges = pd.DataFrame(data=list(bayesnet_digraph.edges()),
                                        columns=['CONSTITUENT_LEARNING_STD_ID',
                                                 'LEARNING_STANDARD_ID'])
    #
    #    ⑵ Classify vertices as either root or non-root through invoking graph_vert_class.
    red_graph_vert_classes = graph_vert_class(edge_list=red_star_graph_edges,
                                              vert_list=list(bayesnet_digraph.nodes()))
    #
    #    ⑶ Identify vertices as either measured or unmeasured.
    meas_group_evid_prof = (wide_evid_dataframe.loc[evid_prof_conformees,
                                                    list(bayesnet_digraph.nodes())] != 'UNMEASURED').all(axis=0)
    clust_prof_measured_verts = (set([evid_prof_idx for evid_prof_idx in list(meas_group_evid_prof.index)
                                      if meas_group_evid_prof[evid_prof_idx]])).intersection(
        set(bayesnet_digraph.nodes()))
    unmeasured_verts = (set([evid_prof_idx for evid_prof_idx in list(meas_group_evid_prof.index)
                             if not meas_group_evid_prof[evid_prof_idx]])).intersection(set(bayesnet_digraph.nodes()))
    #
    #    ⑷ Identify identically-distributed root vertices. These are root vertices with common immediate successors. If not measured,
    #       they are identically distributed. We get at this by constructing a dictionary of root-vertex immediate successors. We then
    #       pairwise compare them. We keep those for which descendant sets are equal.
    #        ⅰ. Construct dictionary of root-vertex descendants. This is a unit-radius, directed ego-graph centered at each
    #           root vertex and ommitting the central vertex.
    root_vert_descendants = dict([(vert, set(nx.ego_graph(G=red_star_graph,
                                                          n=vert,
                                                          radius=1,
                                                          center=False,
                                                          undirected=False).nodes()))
                                  for vert in red_graph_vert_classes.get('ROOT_VERTS')])
    #
    #        ⅱ. Generate a list of pairwise combinations of root vertices.
    root_vert_pairs = list(it.combinations(red_graph_vert_classes.get('ROOT_VERTS'), 2))
    #
    #        ⅲ. Now pairwise-compare the root-vertex descendants. Retain pairs for which the descendants are identical.
    ident_dist_root_pairs = [pair
                             for pair in root_vert_pairs
                             if root_vert_descendants.get(pair[0]) == root_vert_descendants.get(pair[1])]
    #
    #        ⅳ. Finally, conbine our list of pairs into an aggregate set if they are part of a common set.  Use
    #           graph algebra. Interpret our pairs as edges in an undirected graph. Then find the connected components.
    #           Use the result to "filter" root_vert_descendants to only include the identically-distributed root vertices
    #           and union their descendats into a set of shared descendants.
    if len(ident_dist_root_pairs) > 0:
        root_pair_graph = nx.Graph()
        root_pair_graph.add_edges_from(ident_dist_root_pairs)
        nx.connected_components(G=root_pair_graph)
        ident_dist_roots = list(nx.connected_components(G=root_pair_graph))[0]
        ident_dist_root_descendants = set().union(*[root_vert_descendants.get(root) for root in ident_dist_roots])
    else:
        ident_dist_roots = set()
        ident_dist_root_descendants = set()
    #
    #        ⅴ. Identify measured and unmeasured root vertices.  Also, identify which belong to groups of identically-distributed
    #           root vertices when unmeasured.
    unmeas_root_verts = list(unmeasured_verts.intersection(set(red_graph_vert_classes.get('ROOT_VERTS'))))
    meas_root_verts = list(clust_prof_measured_verts.intersection(set(red_graph_vert_classes.get('ROOT_VERTS'))))
    unmeas_ident_dist_roots = ident_dist_roots.intersection(unmeas_root_verts)
    meas_ident_dist_roots = ident_dist_roots.intersection(meas_root_verts)
    #
    # CASE ①:  ℳ D-separates our digraph 𝓖.
    # 🄰 Create a clean copy of our reduced graph 𝓖.  Remove the measured vertices ℳ.
    separable_digraph = nx.DiGraph()
    separable_digraph.add_edges_from(red_star_graph.edges())
    separable_digraph.remove_nodes_from(list(clust_prof_measured_verts))
    #
    # 🄱 Calculate the number of connected components in sep_check_graph. Define a logical variable
    #    indicating whether the essential condition — greater-than-unity connected-component count —
    #    applies. If it does, handle as CASE ①. Otherwise handle as CASE ②.  In anticipation of subsequently
    #    incorporating logic for CASE ③, define here the logical veriable (ℳ ⊆ root(𝒳)).
    d_separated_graph = (nx.number_connected_components(G=separable_digraph.to_undirected()) > 1)
    meas_verts_all_root = clust_prof_measured_verts.issubset(set(red_graph_vert_classes.get('ROOT_VERTS'))) \
                          and (len(unmeas_root_verts) > 0)
    #
    # 🄲 If the number of connected components exceeds unity, construct each of the connected components
    #    as a digraph — including the measured vertices 𝒳.
    if d_separated_graph:
        #    ⑴  Begin by using connected_components to get vertex lists of connected components.
        connected_component_vert_sets = list(nx.connected_components(G=separable_digraph.to_undirected()))
        #
        #    ⑵ Measure-extend each of our connected components. Our connected components are leftovers from when all of the
        #       measured vertices are removed. If we compose all unit-radius ego graphs centered on the vertices of which
        #       each connected-component is comprised, we should have all immediately-adjacent — and measurement-separating —
        #       vertices.  Each of these represents an independtly queryable digraph on which A Bayesian Network can be based.
        #       Identify any measured vertices not contained in these subgraph components.
        meas_sep_comp_nodes = [set().union(*[set(nx.ego_graph(G=red_star_graph,
                                                              n=vert,
                                                              radius=1,
                                                              undirected=True).nodes())
                                             for vert in list(vert_set)])
                               for vert_set in connected_component_vert_sets]
        meas_sep_comp_digraphs = [{'COMPONENT_DIGRAPH': red_star_graph.subgraph(verts),
                                   'COMPONENT_VERTS': verts} for verts in meas_sep_comp_nodes]
        meas_verts_not_in_components = clust_prof_measured_verts \
                                       - set().union(*[graph_dict.get('COMPONENT_VERTS')
                                                       for graph_dict in meas_sep_comp_digraphs])
        #
        # 🄳 Perform an independendent Bayesian-network query for each connected component.
        #    ⑴ Update each dictionary object in component_digraphs with Bayesian-network query-response objects.
        for comp_digraph in meas_sep_comp_digraphs:     ## comp_digraph = meas_sep_comp_digraphs[0]
            comp_digraph.update({'BAYESNET_RESP':
                                     exact_infer_group_know_state(
                                         bayesnet_digraph=comp_digraph.get('COMPONENT_DIGRAPH'),
                                         wide_evid_dataframe=wide_evid_dataframe.loc[evid_prof_conformees,
                                                                                     comp_digraph.get(
                                                                                         'COMPONENT_VERTS')],
                                         evid_prof_conformees=evid_prof_conformees,
                                         var_states=var_states,
                                         clust_idx=clust_idx)}
                                     )
        #
        #
        #    ⑵ Rearrange the results. Our return variable is a dictionary of two dataframes — BAYESNET_RESP and CLUSTER_EXEC_TIME —
        #       obtained from concatenation the respective dictionary-object results contained in component_digraphs.
        cluster_bayesnet_query = dict(
            {'BAYESNET_QUERY_RESP': pd.concat([comp_digraph.get('BAYESNET_RESP').get('BAYESNET_QUERY_RESP')
                                               for comp_digraph in meas_sep_comp_digraphs]).drop_duplicates(),
             'CLUSTER_EXEC_TIME': pd.concat([comp_digraph.get('BAYESNET_RESP').get('CLUSTER_EXEC_TIME')
                                             for comp_digraph in meas_sep_comp_digraphs])})
        #
        # 🄴  Render measured vertices not in the components in CPD format. Concatenate onto the BAYESNET_QUERY_RESP dataframe in our cluster_bayenet_query
        #     dataframe.
        meas_states_not_in_components = wide_evid_dataframe.loc[evid_prof_conformees,
                                                                meas_verts_not_in_components].to_dict(orient='dict')
        for verts in list(
                meas_states_not_in_components.keys()):  ## verts = list(meas_states_not_in_components.keys())[0]
            meas_states_not_in_components.get(verts).update({'BAYESNET_QUERY_RESP':
                                                                 pd.get_dummies(data=pd.DataFrame(
                                                                     data={'STUDENT_ID': list(
                                                                         meas_states_not_in_components.get(
                                                                             verts).keys()),
                                                                           'category': pd.Categorical(
                                                                               values=var_states.loc[list(
                                                                                   meas_states_not_in_components.get(
                                                                                       verts).values()),
                                                                                                     'CAT_LEVEL_IDX'],
                                                                               categories=var_states['CAT_LEVEL_IDX'][
                                                                                          :-1])}),
                                                                     columns=['category'],
                                                                     prefix='',
                                                                     prefix_sep='') \
                                                            .assign(KNOWLEDGE_LVL_TYPE='MEASURED') \
                                                            .assign(LEARNING_STANDARD_ID=verts) \
                                                            .rename(columns=dict(
                                                                     zip(var_states['CAT_LEVEL_IDX'][:-1].astype(str),
                                                                         var_states['CAT_LEVEL_IDX'][:-1])))})
        cluster_bayesnet_query.update(
            {'BAYESNET_QUERY_RESP': pd.concat([meas_states_not_in_components.get(verts).get('BAYESNET_QUERY_RESP')
                                               for verts in list(meas_states_not_in_components.keys())] + \
                                              [cluster_bayesnet_query.get('BAYESNET_QUERY_RESP')])})
    #
    # CASE ②:  Measured vertices are all root vertices in 𝓖, ℳ ⊆ root(𝒳).
    elif meas_verts_all_root:
        # 🄰 Develop lists of measured, unmeasured root veretices.
        #    ⑴ Remove unmeasured root vertices from our subgraph red_star_graph.
        red_star_graph.remove_nodes_from(list(unmeas_root_verts))
        #
        #    ⑶ Create a dataframe of marginalized CPDs for unmeasured root vertices.
        #         ⅰ. First, create a dataframe comprised one identical row for each quiescent CPD. Index the rows by
        #            the LEARNING_STANDARD_IDs. Also, rename the columns. The default indexing will index columns
        #            as integers, whereas subsequent logic requires them as strings.  Derive from the data-frame
        #            indices a LEARNING_STANDARD_ID attribute for subsequent use as a join key.
        quiescent_cpds = pd.DataFrame.from_dict(
            data=dict([(idx, CPT_LIST.loc[CPT_LIST['CONSTITUENT_COUNT'] == '0', 'MEAS'])
                       for idx in unmeas_root_verts]),
            orient='index')
        #
        quiescent_cpds = quiescent_cpds.assign(LEARNING_STANDARD_ID=quiescent_cpds.index.tolist())
        #
        #         ⅱ. Now, create a cartesian product STUDENT_IDs in evid_prof_conformees and LEARNING_STANDARD_IDs in
        #            unmeas_root_verts.  Join quiescent_cpds into this cartesian product.
        quiescent_cpds = pd.merge(left=quiescent_cpds,
                                  right=pd.DataFrame(data=list(it.product(unmeas_root_verts, evid_prof_conformees)),
                                                     columns=['LEARNING_STANDARD_ID', 'STUDENT_ID']))
        #
        #         ⅲ. Assign a KNOWLEDGE_LVL_TYPE attribute as "ESTIMATED". Wait until later to assign EVID_STATE_SIG, which is derived from
        #            Bayesian-network query results.
        quiescent_cpds = quiescent_cpds.assign(KNOWLEDGE_LVL_TYPE='ESTIMATED')
        #
        #
        # 🄱 Construct and query the Bayesian network for each conditionally-independent subgraph.
        cluster_bayesnet_query = exact_infer_group_know_state(bayesnet_digraph=red_star_graph,
                                                              wide_evid_dataframe=wide_evid_dataframe.loc[
                                                                  evid_prof_conformees],
                                                              evid_prof_conformees=evid_prof_conformees,
                                                              var_states=var_states,
                                                              clust_idx=clust_idx)
        quiescent_cpds = pd.merge(left=quiescent_cpds,
                                  right=cluster_bayesnet_query.get('BAYESNET_QUERY_RESP')[['EVID_STATE_SIG',
                                                                                           'STUDENT_ID']].drop_duplicates())
        #
        #    🄲 Concatenate the Bayesian-network query responses with the quiescent CPDs.
        cluster_bayesnet_query.update({'BAYESNET_QUERY_RESP': pd.concat([quiescent_cpds,
                                                                         cluster_bayesnet_query.get(
                                                                             'BAYESNET_QUERY_RESP')])})
    #
    # CASE ③: Measured vetices ℳ are not conatined in root(𝒳) — ℳ ⊈ root(𝒳) — and elements of root(𝒳) share common
    #          descendants. Detect this case by finite-length list of unmeasured, identically-distributed root vertices in unmeas_ident_dist_roots.
    elif len(unmeas_ident_dist_roots) > 1:
        # 🄰 Marginalize the conditional-probablity tables (CPTs) for the shared descendants of identically-distributed
        #    root vertices. We previously identified our identically distributed root vertices and stored in variable ident_dist_roots.
        #    We similarly identified the shared descendants and stored in ident_dist_root_descendants. We construct here the
        #    conditional probabilities for ident_dist_root_descendants and for each of the unmeasured id-root vertices. We
        #    sum-product combine the factors in order to get a joint distribution. We then marginalize out the unmeasured
        #    id-root vertex variables.  What remains is a reduced CPT for the id-root descendants.
        #    ⑴ Begin by constructing unit-radius, directed ego graphs centered on the id-root shared descendants in
        #       ident_dist_root_descendants. Then union the independent ego graphs to get a single graph.  The union
        #       accounts for the scenario in which the shared id-root descendants are adjacent. Also, remove edges
        #       that are not incident on our id-root descendants.
        bayesnet_build_start = tit.default_timer()
        id_root_desc_ego_graphs = dict([(vert, nx.ego_graph(G=red_star_graph.reverse(),
                                                            n=vert,
                                                            center=True,
                                                            undirected=False,
                                                            radius=1).reverse())
                                        for vert in ident_dist_root_descendants])
        id_root_desc_nbhd = id_root_desc_ego_graphs.get(list(id_root_desc_ego_graphs.keys())[0])
        if len(id_root_desc_ego_graphs) > 1:
            for vert in list(id_root_desc_ego_graphs.keys())[1:]:
                id_root_desc_nbhd = nx.compose(G=id_root_desc_nbhd,
                                               H=id_root_desc_ego_graphs.get(vert))
        id_root_desc_nbhd.remove_edges_from([non_inc_edge for non_inc_edge in list(id_root_desc_nbhd.edges())
                                             if len(set(non_inc_edge).intersection(ident_dist_root_descendants)) == 0])
        #
        #     ⑵ Employ internally-defined functions to get the conditional-probability structure for this
        id_root_cond_prob_struc = cond_prob_struc(edge_list=pd.DataFrame(data=list(id_root_desc_nbhd.edges()),
                                                                         columns=['CONSTITUENT_LEARNING_STD_ID',
                                                                                  'LEARNING_STANDARD_ID']),
                                                  vert_list=list(id_root_desc_nbhd.nodes()))
        id_root_vert_class = graph_vert_class(edge_list=pd.DataFrame(data=list(id_root_desc_nbhd.edges()),
                                                                     columns=['CONSTITUENT_LEARNING_STD_ID',
                                                                              'LEARNING_STANDARD_ID']),
                                              vert_list=list(id_root_desc_nbhd.nodes()))
        #
        # 🄱 We next want to marginalize out the conditional probabilities associated with the unmeasured root vertex. We
        #    Employ the Sum-Product algorithm described in Kohler. We derive conditional "quiescent" conditional probabilities
        #    for the descedants of our identically-distributed root vertices. These are in turn employed in a Bayesian network.
        #    ⑴ First, ascertain the conditional-probability structure from the identically-distributed root-vertex
        #       graph.
        id_root_cond_prob_struc = cond_prob_struc(edge_list=pd.DataFrame(data=list(id_root_desc_nbhd.edges()),
                                                                         columns=['CONSTITUENT_LEARNING_STD_ID',
                                                                                  'LEARNING_STANDARD_ID']),
                                                  vert_list=list(id_root_desc_nbhd.nodes()))
        id_root_vert_class = graph_vert_class(edge_list=pd.DataFrame(data=list(id_root_desc_nbhd.edges()),
                                                                     columns=['CONSTITUENT_LEARNING_STD_ID',
                                                                              'LEARNING_STANDARD_ID']),
                                              vert_list=list(id_root_desc_nbhd.nodes()))
        #
        #    ⑵ Update our conditional-probability structure with conditional-probablity tables.  We use our previously developed
        #       logic to build conditional probability tables for Pomegranate Bayesian Networks.  We modify the results to get the dataframe
        #       cpt structure we seek.
        cond_prob_tabs = pom_cond_probs(cond_dependence=id_root_cond_prob_struc,
                                        var_cat_indices=var_states['CAT_LEVEL_IDX'][:-1].tolist())
        for vert in id_root_vert_class.get('NONROOT_VERTS'):  ## vert = id_root_vert_class.get('NONROOT_VERTS')[1]
            cond_prob_table_vert = pd.DataFrame(data=cond_prob_tabs.get(vert).get('COND_PROB'),
                                                columns=cond_prob_tabs.get(vert).get('CONSTITUENT_LIST') \
                                                        + [vert] + ['P_' + str(vert)])
            cond_prob_table_vert[cond_prob_tabs.get(vert).get('CONSTITUENT_LIST') + [vert]] = \
                cond_prob_table_vert[cond_prob_tabs.get(vert).get('CONSTITUENT_LIST') + [vert]].astype(int)
            cond_prob_tabs.get(vert).update({'COND_PROB_TABLE': cond_prob_table_vert,
                                             'COND_PROB': None})
        for vert in id_root_vert_class.get('ROOT_VERTS'):  ## vert = id_root_vert_class.get('ROOT_VERTS')[0]
            cond_prob_tabs.get(vert).update({'COND_PROB_TABLE':
                                                 pd.DataFrame.from_dict(data=cond_prob_tabs.get(vert).get('COND_PROB'),
                                                                        orient='index') \
                                            .assign(state_var=var_states['CAT_LEVEL_IDX'][:-1].tolist()) \
                                            .rename(columns={'state_var': vert,
                                                             0: 'P_' + str(vert),
                                                             'COND_PROB': None})})
        #
        #    ⑶ Merge all of the conditional-probability distributions to set up the "product" step of the sum-product procedure. Then
        #       Build the joint distribution by merging all of the conditional-probability tables.  First get a topological sort of
        #       or id-root descendent neighborhood graph.  We need to merge in reverse topologocial order to make sure that common
        #       variables are available for each join.
        id_root_desc_nbhd_top_sort = list(nx.topological_sort(G=id_root_desc_nbhd))
        id_root_desc_nbhd_top_sort.reverse()
        joint_dist = cond_prob_tabs.get(id_root_desc_nbhd_top_sort[0]).get('COND_PROB_TABLE')
        for vert in id_root_desc_nbhd_top_sort[1:]:
            joint_dist = pd.merge(left=joint_dist,
                                  right=cond_prob_tabs.get(vert).get('COND_PROB_TABLE'))
        joint_dist = joint_dist.assign(P_joint=joint_dist[['P_' + vert for vert in id_root_desc_nbhd.nodes()]] \
                                       .prod(axis=1)) \
            .drop(labels=['P_' + vert for vert in id_root_desc_nbhd.nodes()],
                  axis=1)
        #
        #    ⑷ We seek a conditional probability for each of our soft-separating vertices in our id-root-descendant
        #       neighborhood graph.  We get at this through the following steps.
        #         ⅰ. Marginalize out the non-immediate descendants. These are irrelevant for our purpose.
        #         ⅱ. Marginalize out the measured id-root immediate predecessors.
        #         ⅲ. Condition on the remaining immediate predessors.
        for sep_vert in list(ident_dist_root_descendants):  ## sep_vert = list(ident_dist_root_descendants)[0]
            #         ⅰ. Marginalize out the non-immediate descendants. We need a bit of graph-algebra for this.  We
            #            find the vertices in the inbound-only unit-radius ego graph including the central vertex and
            #            discard what is not included therein.
            non_predecessors_sep_vert = set(id_root_desc_nbhd.nodes()) - \
                                        set(nx.ego_graph(G=id_root_desc_nbhd.reverse(),
                                                         n=sep_vert,
                                                         radius=1,
                                                         center=True,
                                                         undirected=False).nodes())
            #
            #         ⅱ. Marginalize out the unmeasured id-root immediate predecessors.  We get at this by looking at the
            #            intersection of the previously-identified identically-distributed roots with the imeediate predecessors,
            #            and remove measured vertices from this set. We marginalize out these two — the non-predecessors
            #            and the unmeasured id_roots — vertex sets in a single operation.
            #            📛📛 CAUTION:  ONLY CONDITION if Parent vertices are present on which to Conditon‼️ 📛📛
            unmeasured_id_root = set(nx.ego_graph(G=id_root_desc_nbhd.reverse(),
                                                  n=sep_vert,
                                                  radius=1,
                                                  center=False,
                                                  undirected=False).nodes()) \
                                     .intersection(ident_dist_roots) - clust_prof_measured_verts
            joint_dist_sep_vert = joint_dist.groupby(by=list(set(id_root_desc_nbhd.nodes()) - \
                                                             non_predecessors_sep_vert.union(unmeasured_id_root)),
                                                     axis=0,
                                                     as_index=False)['P_joint'].sum()
            #
            #    ⑹ We now condition on the remaining predecessors, those that are not unmeasured id-root vertices.  We identify
            #       the variables on which we want to condition.  We marginalize on them. Them multiply the reciprocal of the
            #       marginalized CPD by the joint.
            non_unmeas_idroot_predecessors = set(nx.ego_graph(G=id_root_desc_nbhd.reverse(),
                                                              n=sep_vert,
                                                              radius=1,
                                                              center=False,
                                                              undirected=False).nodes()) - unmeasured_id_root
            if len(non_unmeas_idroot_predecessors) > 0:
                marg_cpd_sep_vert = joint_dist_sep_vert.groupby(by=list(non_unmeas_idroot_predecessors),
                                                                axis=0,
                                                                as_index=False)['P_joint'].sum()
                marg_cpd_sep_vert = marg_cpd_sep_vert.assign(invP_joint=np.reciprocal(marg_cpd_sep_vert['P_joint'])) \
                    .drop(labels='P_joint',
                          axis=1)
                cond_prob_sep_vert = pd.merge(left=joint_dist_sep_vert,
                                              right=marg_cpd_sep_vert)
                cond_prob_sep_vert = cond_prob_sep_vert.assign(
                    P_joint=cond_prob_sep_vert[['P_joint', 'invP_joint']].prod(axis=1)) \
                    .drop(labels='invP_joint',
                          axis=1)
            else:
                cond_prob_sep_vert = joint_dist_sep_vert
            # cond_prob_sep_vert.groupby(by = list(non_unmeas_idroot_predecessors),
            #                               axis = 0,
            #                               as_index = False)['P_joint'].sum()
            #
            #
            #    ⑸ Update the conditional-probability table COND_PROB_TABLE with for the sep_vertᵗʰ item in our conditional-probability
            #       dictionary cond_prob_tabs with our reduced, marginalized conditional probability table marg_dist_sep_vert. Also update
            #       for the same dictionary item with CONSTITUENT_LIST with its parent vertices.
            cond_prob_tabs.get(sep_vert).update({'COND_PROB_TABLE': cond_prob_sep_vert,
                                                 'CONSTITUENT_LIST': list(non_unmeas_idroot_predecessors)})
        #
        # 🄲 Construct and query the Bayesian network for the graph excluding the identically-distributed, unmeasured
        #    root vertices. Use the modified, reduced conditional-probablities in cond_prob_tabs for the purpose.  We build our
        #    Bayesian network by specifying CPD tables other than the defaults from CPT_LIST.  We use build_pgmpy_bayesnet
        #    for a graph excluding our unmeaured id-root vertices. We construct it's conditional-probability structure and
        #    conditional-probability tables. We then substitute the id-root vertex parents' CPTs from cond_prob_tabs in place
        #    of those based on the default based on CPT_LIST.
        #    ⑴ Remove unmeasured it-root vertices from red_star_graph.
        idroot_red_star_graph = nx.DiGraph()
        idroot_red_star_graph.add_edges_from(list(red_star_graph.edges()))
        idroot_red_star_graph.remove_nodes_from(unmeas_ident_dist_roots)
        #
        #    ⑵ Get the conditional-probability structure, vertex classification, and default conditional-probability
        #       tables for our complementary graph.
        comp_graph_prob_struc = cond_prob_struc(edge_list=pd.DataFrame(data=list(idroot_red_star_graph.edges()),
                                                                       columns=['CONSTITUENT_LEARNING_STD_ID',
                                                                                'LEARNING_STANDARD_ID']),
                                                vert_list=list(idroot_red_star_graph.nodes()))
        comp_graph_vert_class = graph_vert_class(edge_list=pd.DataFrame(data=list(idroot_red_star_graph.edges()),
                                                                        columns=['CONSTITUENT_LEARNING_STD_ID',
                                                                                 'LEARNING_STANDARD_ID']),
                                                 vert_list=list(idroot_red_star_graph.nodes()))
        comp_cond_prob_tabs = pom_cond_probs(cond_dependence=comp_graph_prob_struc,
                                             var_cat_indices=var_states['CAT_LEVEL_IDX'][:-1].tolist())
        for vert in comp_graph_vert_class.get('NONROOT_VERTS'):  ## vert = id_root_vert_class.get('NONROOT_VERTS')[1]
            cond_prob_table_vert = pd.DataFrame(data=comp_cond_prob_tabs.get(vert).get('COND_PROB'),
                                                columns=comp_cond_prob_tabs.get(vert).get('CONSTITUENT_LIST') \
                                                        + [vert] + ['P_cond'])
            cond_prob_table_vert[comp_cond_prob_tabs.get(vert).get('CONSTITUENT_LIST') + [vert]] = \
                cond_prob_table_vert[comp_cond_prob_tabs.get(vert).get('CONSTITUENT_LIST') + [vert]].astype(int)
            comp_cond_prob_tabs.get(vert).update({'COND_PROB_TABLE': cond_prob_table_vert,
                                                  'COND_PROB': None})
        for vert in comp_graph_vert_class.get('ROOT_VERTS'):  ## vert = id_root_vert_class.get('ROOT_VERTS')[0]
            comp_cond_prob_tabs.get(vert).update({'COND_PROB_TABLE':
                                                      pd.DataFrame.from_dict(
                                                          data=comp_cond_prob_tabs.get(vert).get('COND_PROB'),
                                                          orient='index') \
                                                 .assign(state_var=var_states['CAT_LEVEL_IDX'][:-1].tolist()) \
                                                 .rename(columns={'state_var': vert,
                                                                  0: 'P_cond',
                                                                  'COND_PROB': None})})
        #
        #    ⑶ Substitute our "reduced" conditional probability tables from cond_prob_tabs into comp_cond_prob_tabs for the
        #       id-root descendant vertices.
        for vert in list(ident_dist_root_descendants):  ## vert = list(ident_dist_root_descendants)[0]
            comp_cond_prob_tabs.get(vert).update({'COND_PROB_TABLE': cond_prob_tabs.get(vert).get('COND_PROB_TABLE') \
                                                 .rename(columns={'P_joint': 'P_cond'})})
        #
        #    ⑷ Build a pgmpy Bayesian network based on the id-root-reduced graph.
        id_reduced_bayesnet = build_pgmpy_bayesnet(directed_graph=idroot_red_star_graph,
                                                   var_states=var_states,
                                                   bayesnet_label=clust_idx,
                                                   cond_probs=comp_cond_prob_tabs)
        idroot_red_baysenet_build_time = tit.default_timer() - bayesnet_build_start
        #
        #    ⑸ Query the Bayesian network id-reduced Bayesian network.
        #        ⅰ. First group evidntiary-profile conformees by evidentiary state.
        evid_state_by_subj = groupby_evid_state(
            evid_states_to_be_grouped=wide_evid_dataframe[list(idroot_red_star_graph.nodes())],
            evid_prof_conformees=evid_prof_conformees,
            evid_state_cat_map=var_states['CAT_LEVEL_IDX'][:-1])
        #
        #        ⅱ. Configure a dataframe within which to collect execution-time statistics.
        cluster_knowledge_state = pd.DataFrame(
            columns=['EVID_STATE_SIG', 'LEARNING_STANDARD_ID', 'KNOWLEDGE_LVL_TYPE'] + \
                    list(var_states['CAT_LEVEL_IDX'])[:-1])
        clust_exec_time = pd.DataFrame(columns=['EVID_STATE_SIG',
                                                'GRAPH_ORDER',
                                                'EDGE_COUNT',
                                                'MEAS_VERT_COUNT',
                                                'EST_VERT_COUNT',
                                                'TIME_NOW',
                                                'ELAPSED_TIME'],
                                       index=evid_state_by_subj.get('EVID_STATE_SIG'))
        clust_exec_time = clust_exec_time.assign(EDGE_COUNT=len(idroot_red_star_graph.edges()))
        clust_exec_time = clust_exec_time.assign(GRAPH_ORDER=len(idroot_red_star_graph.nodes()))
        clust_exec_time = clust_exec_time.assign(CLUSTER=clust_idx)
        clust_exec_time = clust_exec_time.assign(INFERENCE_APPROACH='EXACT')
        #
        #        ⅲ. Cycle through the evidentiary states, querying the Bayesian network for each.  This logic follows exactly
        #            that in exact_query_group_know_state subroutine.
        #
        #             📛📛 CAUTION:  We only query the Bayesian network one or more vertices in unmeasured. Otherwise
        #                             We translate the evidentiary states to a dataframe.
        if set(idroot_red_star_graph.nodes()).issubset(clust_prof_measured_verts):
            know_state_state_idx = pd.concat([pd.get_dummies(data=pd.DataFrame(
                data={'LEARNING_STANDARD_ID': list(evid_state_by_subj.get(state_idx).get('EVID_STATE').keys()),
                      'category': pd.Categorical(values=evid_state_by_subj.get(state_idx).get('EVID_STATE').values(),
                                                 categories=var_states['CAT_LEVEL_IDX'][:-1])}),
                columns=['category'],
                prefix='',
                prefix_sep='') \
                                             .assign(EVID_STATE_SIG=state_idx) \
                                              for state_idx in evid_state_by_subj.get('EVID_STATE_SIG')]) \
                .rename(columns=dict(zip(var_states['CAT_LEVEL_IDX'][:-1].astype(str),
                                         var_states['CAT_LEVEL_IDX'][:-1])))
            know_state_clust_idx = pd.merge(left=know_state_state_idx,
                                            right=evid_state_by_subj.get('EVID_STATE_CONFORMEES')) \
                .assign(KNOWLEDGE_LVL_TYPE='MEASURED')

        else:
            for state_idx in evid_state_by_subj.get(
                    'EVID_STATE_SIG'):  # state_idx = evid_state_by_subj.get('EVID_STATE_SIG')[0]
                print('Evidentiary state ' + str(state_idx) + ', ' + str(
                    evid_state_by_subj.get('EVID_STATE_SIG').index(state_idx) + 1) + \
                      ' of ' + str(len(
                    evid_state_by_subj.get('EVID_STATE_SIG'))) + ' evidentiary states for ' + clust_idx + ' at time ' + \
                      str(datetime.utcnow().time()) + 'Z')
                start_time_state_idx = tit.default_timer()
                bayesnet_query_resp = id_reduced_bayesnet.query(variables=list(set(idroot_red_star_graph.nodes()) - \
                                                                               set(evid_state_by_subj.get(
                                                                                   state_idx).get(
                                                                                   'EVID_STATE').keys())),
                                                                evidence=evid_state_by_subj.get(state_idx).get(
                                                                    'EVID_STATE'))
                know_state_state_idx = pd.DataFrame.from_dict(data=dict([(lrn_std, marg_cpd.values.tolist())
                                                                         for (lrn_std, marg_cpd) in
                                                                         bayesnet_query_resp.items()]),
                                                              orient='index')
                for lrn_std in list(evid_state_by_subj.get(state_idx).get('EVID_STATE').keys()):
                    know_state_state_idx.loc[
                        lrn_std, evid_state_by_subj.get(state_idx).get('EVID_STATE').get(lrn_std)] = 1
                know_state_state_idx.fillna(value=0, inplace=True)
                know_state_state_idx = know_state_state_idx.assign(EVID_STATE_SIG=state_idx)
                know_state_state_idx = know_state_state_idx.assign(
                    LEARNING_STANDARD_ID=know_state_state_idx.index.tolist())
                know_state_state_idx = know_state_state_idx.assign(KNOWLEDGE_LVL_TYPE='ESTIMATED')
                know_state_state_idx.loc[
                    list(evid_state_by_subj.get(state_idx).get('EVID_STATE').keys()), 'KNOWLEDGE_LVL_TYPE'] = 'MEASURED'
                evid_state_by_subj.get(state_idx).update({'BAYESNET_QUERY_RESP': know_state_state_idx})
                clust_exec_time.loc[state_idx,
                                    ['EVID_STATE_SIG',
                                     'TIME_NOW',
                                     'ELAPSED_TIME',
                                     'EST_VERT_COUNT',
                                     'MEAS_VERT_COUNT']] = [state_idx,
                                                            str(datetime.utcnow().time()),
                                                            tit.default_timer() - start_time_state_idx,
                                                            len(list(set(bayesnet_digraph.nodes()) - \
                                                                     set(evid_state_by_subj.get(state_idx).get(
                                                                         'EVID_STATE').keys()))),
                                                            len(evid_state_by_subj.get(state_idx).get(
                                                                'EVID_STATE').keys())]
            print(clust_exec_time.loc[state_idx].T.squeeze())
            know_state_clust_idx = pd.merge(left=pd.concat([evid_state_by_subj.get(state_idx).get('BAYESNET_QUERY_RESP')
                                                            for state_idx in evid_state_by_subj.get('EVID_STATE_SIG')]),
                                            right=evid_state_by_subj.get('EVID_STATE_CONFORMEES'))
        #
        # 🄳 Construct our estimates for the id-root vertices. We get ths by factor manipulation.  We have in know_state_clust_idx for
        #    each evidentiary state of estimates marginalized conditional probability distributions for each of the non id-root
        #    vertices in the id-root graphical vicinity. We also have this graphical neighborhood's joint probability distribution.
        #    We now condition our joint distribution on the variables for which we have estimates in know_state_clust_idx.
        #    We then apply via factor multiplication and marginalize out the results to get the our estimated conditional
        #    probabilities for the id-root vertices.
        #    ⑴ Marginalize our joint distribution with respect to the non-idroot vertices in our id-root graphical vicinity.
        #      We want to marginalize out all of the variables except for the id-roots vertices and their immediate-descendants
        #      by which they are soft-separated from the rest of the graph.
        marg_idroot_desc_dist = joint_dist.groupby(by=list(unmeas_ident_dist_roots.union(ident_dist_root_descendants)),
                                                   axis=0,
                                                   as_index=False)['P_joint'].sum()
        #
        #    ⑵ Now, condition our marginalized distribution on the id-root descendants by which the id-root vertices themselves
        #       are soft-separatged from the rest of the graph.
        desc_marg_dist = marg_idroot_desc_dist.groupby(by=list(ident_dist_root_descendants),
                                                       axis=0,
                                                       as_index=False)['P_joint'].sum()
        desc_marg_dist = desc_marg_dist.assign(inv_P_joint=np.reciprocal(desc_marg_dist['P_joint'])) \
            .drop(labels='P_joint',
                  axis=1)
        marg_idroot_desc_cond_dist = pd.merge(left=marg_idroot_desc_dist,
                                              right=desc_marg_dist)
        marg_idroot_desc_cond_dist = marg_idroot_desc_cond_dist.assign(
            P_marg_cond=marg_idroot_desc_cond_dist[['P_joint',
                                                    'inv_P_joint']] \
            .prod(axis=1)) \
            .drop(labels=['P_joint',
                          'inv_P_joint'],
                  axis=1)
        #
        #    ⑶ Extract from know_state_clust_idx a unique instance of the the CPD for each learning standard for each evidentiary
        #       state. Reshape each into factors that can be join and multiplied onto our conditional probabilities marg_idroot_desc_cond_dist.
        #       collect these factors in nested dictionaries.
        conditioning_est_states = know_state_clust_idx.loc[
            know_state_clust_idx['LEARNING_STANDARD_ID'].isin(ident_dist_root_descendants)] \
            .drop_duplicates(subset=['EVID_STATE_SIG',
                                     'LEARNING_STANDARD_ID'],
                             keep='first') \
            .drop(labels=['KNOWLEDGE_LVL_TYPE',
                          'STUDENT_ID'],
                  axis=1)
        root_desc_cpds = dict(
            [(state_idx, dict([(lrn_std, pd.DataFrame({'var_state': var_states['CAT_LEVEL_IDX'][:-1].tolist(),
                                                       'cond_prob': pd.merge(left=conditioning_est_states,
                                                                             right=pd.DataFrame(
                                                                                 {'LEARNING_STANDARD_ID': [lrn_std],
                                                                                  'EVID_STATE_SIG': [state_idx]})) \
                                                      .drop(labels=['EVID_STATE_SIG',
                                                                    'LEARNING_STANDARD_ID'],
                                                            axis=1) \
                                                      .transpose()[0].tolist()}) \
                                .rename(columns={'var_state': lrn_std,
                                                 'cond_prob': 'P_' + str(lrn_std)}))
                               for lrn_std in list(ident_dist_root_descendants)]))
             for state_idx in evid_state_by_subj.get('EVID_STATE_SIG')])
        #
        #   ⑷ Factor-multiply the id-root descendants' CPDs for each evidentiary state onto our id-root-descendant-conditioned
        #      conditional-probabilty distribution marg_idroot_desc_cond_dist. The marginalize out all variables except for a single
        #      id-root variable.  Replicate that marginlized cpd for each unmeasured id-root variable. Build the repeated
        #      cpds up into a dataframe with LEARNING_STANDARD_ID and EVID_STATE_SIG attributes. Store the result as an object in
        #      our root_desc_cpds dictionary.
        for state_idx in evid_state_by_subj.get(
                'EVID_STATE_SIG'):  ## state_idx = evid_state_by_subj.get('EVID_STATE_SIG')[0]
            marg_id_root_cpd = marg_idroot_desc_cond_dist
            for lrn_std in list(ident_dist_root_descendants):  ##      lrn_std = list(ident_dist_root_descendants)[0]
                marg_id_root_cpd = pd.merge(left=marg_id_root_cpd,
                                            right=root_desc_cpds.get(state_idx).get(lrn_std))
            marg_id_root_cpd = marg_id_root_cpd.assign(P_marg_cond=marg_id_root_cpd[['P_marg_cond'] + \
                                                                                    ['P_' + str(lrn_std)
                                                                                     for lrn_std in
                                                                                     list(ident_dist_root_descendants)]] \
                                                       .prod(axis=1)) \
                .drop(labels=['P_' + str(lrn_std) for lrn_std in list(ident_dist_root_descendants)],
                      axis=1) \
                .groupby(by=list(unmeas_ident_dist_roots)[0],
                         axis=0,
                         as_index=True)['P_marg_cond'].sum().tolist()
            root_desc_cpds.get(state_idx).update(
                {'BAYESNET_QUERY_RESP': pd.DataFrame(data=[marg_id_root_cpd] * len(unmeas_ident_dist_roots),
                                                     columns=var_states['CAT_LEVEL_IDX'][:-1].tolist(),
                                                     index=list(unmeas_ident_dist_roots)) \
                .assign(EVID_STATE_SIG=state_idx) \
                .assign(LEARNING_STANDARD_ID=list(unmeas_ident_dist_roots))})
        #
        #    ⑸ Extract from our root_desc_cpds dictionary all of the Bayesian-network query responses and concatenate them into
        #       a single dataframe. Merge this with the EVID_STATE_CONFORMEES index from our evid_state_by_subj dictionary.
        #       Concatenate this result onto know_state_clust_idx and package to be returned as an argument function.
        cluster_bayesnet_query = dict({'BAYESNET_QUERY_RESP': pd.concat(
            [pd.merge(left=pd.concat([root_desc_cpds.get(state_idx).get('BAYESNET_QUERY_RESP')
                                      for state_idx in evid_state_by_subj.get('EVID_STATE_SIG')]),
                      right=evid_state_by_subj.get('EVID_STATE_CONFORMEES')) \
            .assign(KNOWLEDGE_LVL_TYPE='ESTIMATED'),
             know_state_clust_idx]).reset_index() \
                                      .sort_values(by=['STUDENT_ID', 'LEARNING_STANDARD_ID']),
                                       'CLUSTER_EXEC_TIME': clust_exec_time.assign(
                                           BAYESNET_BUILD_TIME=idroot_red_baysenet_build_time)})
    #
    #
    # CASE ④:  None of CASES ⓵ – ⓷ apply. Just query the Bayesian network using exact or approximate inference based on
    #           the graph order.
    else:
        if red_star_graph.order() <= 11:
            cluster_bayesnet_query = exact_infer_group_know_state(bayesnet_digraph=red_star_graph,
                                                                  wide_evid_dataframe=wide_evid_dataframe.loc[
                                                                      evid_prof_conformees],
                                                                  evid_prof_conformees=evid_prof_conformees,
                                                                  var_states=var_states,
                                                                  clust_idx=clust_idx)
        else:
            cluster_bayesnet_query = approx_infer_group_know_state(bayesnet_digraph=red_star_graph,
                                                                   wide_evid_dataframe=wide_evid_dataframe.loc[
                                                                       evid_prof_conformees],
                                                                   evid_prof_conformees=evid_prof_conformees,
                                                                   var_states=var_states,
                                                                   clust_idx=clust_idx)
            #
            # 🄴 Concatenate the Bayesian-network query responses from each subgraph above.  Return the result.  We do this conditionally,
            #    only if our conditional-independence conditions — measured_vertices ⊆ root vertices ⋀ valency{unmeasured root vertices} ⊆ {1} —
            #    are satisfied.
            #    ⑴ First, extract the EVID_STATE_SIG attribute from the BAYESNET_QUERY_RESP dataframe item in cluster_bayesnet_query.
            #      Join onto quiescent_cpds in order to introduce to the latter.
            #
            #    ⑵ Concatenate quiescent_cpds onto the BAYESNET_QUERY_RESP dataframe in cluster_bayesnet_query.
            #
    return cluster_bayesnet_query


#
# |∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕
# ⓚ BUILD POMEGRANATE BAYESET.  Use pgmpy's exact-inference calculation to estimate group knowledge state.
#    We use Pomegranate's loopy belief-propagation (LBP) except by exception.  Exact inference is limited to cases of relatively
#    small graphs. We use it in degernate cases for which Pomegranate's convergence is slow.  Star graphs represent a noteworkty exception.
#
#    Our logic here follows that of the Pomegranate. We perform the following steps.
#    ⑴ Construct a pgmpy Bayesian-network object.
#    ⑵ Prepare the evidence for application to the Bayesian Network.
#    ⑶ Apply evidence to query the Baysesian network.
#    ⑷ Assemble and return the results.
def build_pgmpy_bayesnet(directed_graph, var_states, bayesnet_label, cond_probs):
    #    ⑴ Construct a pgmpy Bayesian-network object.
    #       ⒜ First declare the Bayesian model itself.  This directly follows from the edges of our Bayesian-network
    #          DiGraph.
    start_time_state_idx = tit.default_timer()
    pgmpy_bayesnet = BayesianModel(directed_graph.edges())
    #
    #       ⒝ We now specify conditional-probabilities for each vertex in the graph.
    #           ⅰ. We first need to identify  the immediate successors for each vertex. We identify the root and non-root vertices. We identify
    #              the immediate predecessors for the non-root vertices.
    vertex_in_degree = dict(pgmpy_bayesnet.in_degree())
    root_verts = [vert for (vert, degree) in vertex_in_degree.items() if degree == 0]
    non_root_verts = list(set(pgmpy_bayesnet.nodes()) - set(root_verts))
    non_root_predecessors = dict([(vert, list(nx.ego_graph(G=directed_graph.reverse(copy=True),
                                                           n=vert,
                                                           radius=1,
                                                           center=False,
                                                           undirected=False).nodes()))
                                  for vert in non_root_verts])
    #
    #           ⅱ. Now add tabular CPDs for the root vertices.
    var_state_cardinality = len(var_states) - 1
    for vert_idx in root_verts:  # vert = root_verts[0]
        pgmpy_bayesnet.add_cpds(TabularCPD(variable=vert_idx,
                                           variable_card=var_state_cardinality,
                                           values=np.reshape(
                                               a=CPT_LIST.loc[CPT_LIST['CONSTITUENT_COUNT'] == str(0)]['MEAS'].tolist(),
                                               newshape=(var_state_cardinality, 1))))
    for vert_idx in non_root_verts:  ## vert_idx = non_root_verts[0]
        pgmpy_bayesnet.add_cpds(TabularCPD(variable=vert_idx,
                                           variable_card=var_state_cardinality,
                                           evidence_card=list(np.repeat(a=var_state_cardinality,
                                                                        repeats=vertex_in_degree.get(vert_idx))),
                                           values=np.reshape(
                                               a=cond_probs.get(vert_idx).get('COND_PROB_TABLE')['P_cond'].tolist(),
                                               newshape=(var_state_cardinality,
                                                         var_state_cardinality ** vertex_in_degree.get(vert_idx)),
                                               order='F'),
                                           evidence=non_root_predecessors.get(vert_idx)))
    pgmpy_bayesnet_compiled = VariableElimination(pgmpy_bayesnet)
    return pgmpy_bayesnet_compiled


#
#
# |∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕
# ⓗ Build an induced-inrange graphical neighborhood for vertices for which evidence is available.  We limit our analysis
#    to measurements within a specified radius, evid_radius.  Our meas_list contains a list of vertices for which
#    evidence is available. The course_nhbd graph is the overall graphical neighborhood to which we are limited.
#    We build our overal graph by composing ego grpahs for each vertex specified in meas_list.
def build_induced_inrange_graph(meas_list, evid_radius, course_nhbd_graph):
    induced_inrange_graph = nx.ego_graph(G=course_nhbd_graph,
                                         n=meas_list[0],
                                         radius=evid_radius,
                                         undirected=True)
    for vert_idx in meas_list[1:]:
        induced_inrange_graph = nx.compose(G=induced_inrange_graph,
                                           H=nx.ego_graph(G=course_nhbd_graph,
                                                          n=vert_idx,
                                                          radius=evid_radius,
                                                          undirected=True))
    return induced_inrange_graph


#
# |∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕
# ⓗ EXTRACT BAYESIAN-NETWORK QUERY RESULTS.  The pomegranate bayesian-network query returns results on a json-like format. We want
#    them in an array.
def extract_bayesnet_query(pred_proba_result, graph_node_order, evid_state, var_states):
    #    First, zip the query-response result pred_proba_result into a dictionary.  Initialize a blank dataframe within which
    #    to store in a structured format.
    pred_proba_result_dict = dict(zip(graph_node_order,
                                      pred_proba_result.tolist()))
    #
    #    Loop through the elements of pred_proba_result_dict, extracting the 'parameters' object from the json object.
    #    Extract each object into a dataframe row.  We must handle measured and estiamted vertices differently.
    #    The Pomegranate proba returns a json object containing conditional probabilities for estimated vertices.
    #    An integer corresponding to the observed state is all that is returned for the measured vertices.  We
    #    must transform the measured vertices into a cpd-like representation in identical format to the estimates.
    for dict_key in list(set(pred_proba_result_dict.keys()) - set(
            evid_state.keys())):  # dict_key = '462881' list(pred_proba_result_dict.keys())[0]
        pred_proba_result_dict.update({dict_key:
                                           json.loads(pred_proba_result_dict.get(dict_key).to_json())['parameters'][0]})
        pred_proba_result_dict.update({dict_key:
                                           dict((int(float(str(key))), value) for key, value in
                                                pred_proba_result_dict.get(dict_key).items())})
    #
    #    We construct a dataframe of dummy variables for the measured-vertex observed categories.  We transform
    #    the resulting dataframe into a dictionary and update the items corresponding to the measured vertices
    #    in the evidentiary state with the dummy lists.
    meas_verts_states = pd.get_dummies(data=pd.DataFrame(data={'LEARNING_STANDARD_ID': list(evid_state.keys()),
                                                               'category': pd.Categorical(
                                                                   values=[int(val) for val in evid_state.values()],
                                                                   categories=var_states['CAT_LEVEL_IDX'][:-1])}),
                                       columns=['category'],
                                       prefix='',
                                       prefix_sep='') \
        .set_index(keys='LEARNING_STANDARD_ID',
                   drop=True) \
        .rename(columns=dict(zip([str(state) for state in var_states['CAT_LEVEL_IDX'][:-1]],
                                 var_states['CAT_LEVEL_IDX'][:-1]))) \
        .to_dict(orient='index')
    for dict_key in list(meas_verts_states):
        pred_proba_result_dict.update({dict_key: meas_verts_states.get(dict_key)})
    #
    #   Return the result as a list of lists.
    return pd.DataFrame.from_dict(data=pred_proba_result_dict,
                                  orient='index')


#
# |∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕
# ⓘ UPDATE CONDITIONAL PROBABILITY TABLE.  Add to POM_COND_PROB_TABLES dictionary the diexrete-/conditional-probability
#    object for a specified graph vertex. Take as an inputs:
#    ⧐ graph_vert, the graph vertex for which the discrete-/conditional-probablity structure is constructed;
#    ⧐ disc_cond_prob a dictionary object containing the conditional-probability structure for the correspoding vertex; and
#    ⧐ pom_cond_prob_tab, the dictionary of conditional-probability objects, such as it is.
def update_pom_cond_tab(graph_vert, disc_cond_prob, pom_cond_prob_tab):
    #   Ascertain the node-type — whether root vertex for which we use DiscreteDistribution or non-root for which we use ConditionalProbabilityTable.
    #   The NODE_TYPE entry in the corresponding dictionary entry of COND_PROB_TABLES contains this information.
    NODE_TYPE_graph_vert = disc_cond_prob.get('NODE_TYPE')
    #
    #   If NODE_TYPE_graph_vert is 'discrete' we simply apply COND_PROB object to DiscreteDistribution and store as update to
    #   POM_COND_PROB_TABLE.
    if NODE_TYPE_graph_vert == 'discrete':
        pom_cond_prob_tab.update({graph_vert: DiscreteDistribution(disc_cond_prob.get('COND_PROB'))})
    #
    #   If NODE_TYPE is not 'discrete', we need to use the ConditionalProbabilityTable. This requires additional arguments.
    else:
        #   Now update POM_COND_PROB_TABLES by calling ConditionalProbabilityTable.  It builds the corresponding conditional-probability table
        #   using as arguments the COND_PROB value in COND_PROB_TABLES corresonding to graph_vert and the PROM_COND_PROB_TABLES values
        #   identified in OBS_VERTS_graph_vert.
        pom_cond_prob_tab.update({graph_vert: ConditionalProbabilityTable(disc_cond_prob.get('COND_PROB'),
                                                                          list(pom_cond_prob_tab.get(dict_idx)
                                                                               for dict_idx in disc_cond_prob.get(
                                                                              'CONSTITUENT_LIST')))})
    #
    #   Return pom_cond_prob_tab as the returned argument.
    return pom_cond_prob_tab


#
# |∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕
# ⓙ CONSTRUCT COMPILED, POMEGRANATE BAYESIAN NETWORK.  Our inputs are
#    ⧐ directed_graph, a networkx DiGraph object on which the graph is based.
#    Another variable — CPT_LIST — containing the conditional-probability relationships must be defined within the environment.
#
#    This function/procedure hybrid executes the following procedure.
#    ⓐ Derive "utility" variables about the graphical, conditional-probability structures by which the Bayesian network
#       is defined.
#    ⓑ Assemble a dictionary of Pomegranate conditional-/discrete-probability, node objects.  Since these objects
#       are recursively, we define vertex conditional-probablity objects following a root-to-leaf sequence.
#    ⓒ Translate the conditional-/discrete-probability objects into Pomegranate node objects.
#    ⓓ Declare the Pomegranate Bayesian network.  Add nodes and edges.
#    ⓔ Compile and return the result.
#    The returned result is a dictionary comprised of:
#    ⧐ The compiled ("baked") Bayesnet object itself; and
#    ⧐ A list of the vertex labels in the sequence corresponding to their indices in the graph model.
def build_pom_bayesnet(directed_graph, var_states, bayesnet_label):
    #    ⓐ Derive "utility" variables.
    #        ⅰ. Get the graph edge list.
    IN_RANGE_GRAPH_EDGES = pd.DataFrame(data=list(directed_graph.edges()),
                                        columns=['CONSTITUENT_LEARNING_STD_ID',
                                                 'LEARNING_STANDARD_ID'])
    #
    #        ⅱ. Construct a dictionary representing the conditional-dependence structure of the graph.
    #           This straightforwardly results from a groupby of the observation vertices with the targets.
    CONDITIONAL_DEPENDENCE = cond_prob_struc(edge_list=IN_RANGE_GRAPH_EDGES,
                                             vert_list=list(directed_graph.nodes()))
    #
    # 	     ⅲ. Classify graph vertices as root or non-root, using LEARNING_PROGRESSION.  Also, filter the CONDITIONAL_DEPENDENCE
    #           dictionary to have only the observation variables for each non-root vertex.  Also define an OBS_VERTS variable,
    #           an excerpt from the GRAPH_VERT_CLASS listing the observation vertices for each non-root vertex.
    GRAPH_VERT_CLASS = graph_vert_class(edge_list=IN_RANGE_GRAPH_EDGES,
                                        vert_list=list(directed_graph.nodes()))
    OBS_VERTS = dict((dict_key, CONDITIONAL_DEPENDENCE.get(dict_key).get('CONSTITUENT_LEARNING_STD_ID'))
                     for dict_key in GRAPH_VERT_CLASS.get('NONROOT_VERTS'))
    #
    #        ⅳ. Define a vertex-integration check-off list. We must follow a root-to-leaf sequence in building
    #           up the Pomegranate discrete-/conditional-probability distribution objects. We cannot define
    #           a "downstream" conditional probability for a vertex until the discrete-/conditional-probability
    #           objects for all of its observation-variable vertices have first been constructed.
    VERT_INTEG_CHECKLIST = {'NON_INTEG_VERTS': list(CONDITIONAL_DEPENDENCE.keys()),
                            'INTEG_VERTS': []}
    #
    #    ⓑ Assemble a dictionary of Pomegranate conditional-/discrete-probability, node objects.  The first part
    #       of this — the discrete-/conditional-probabilities — requires dilligent adherence to the root-to-leaf
    #       sequence entailed in the directed directed graph.
    #        ⅰ. Construct the conditional-probability objects and assemble into a dictionary. These objects are
    #           the quantitative conditional probabilities associted with each vertex.  They are extracted from
    #           an in-environment variable CPT_LONG.
    COND_PROB_TABLES = pom_cond_probs(cond_dependence=CONDITIONAL_DEPENDENCE,
                                      var_cat_indices=list(var_states['CAT_LEVEL_IDX'])[:-1])  ### ⬅︎‼⛔‼⛔‼⛔
    #
    #        ⅱ. Now, apply the conditional-probability elements from COND_PROB_TABLES as arguments to the
    #           Pomegrante DiscreteDistribution, ConditionalProbabilityTable functions. The root-to-leaf
    #           sequence of recursion must be followed here. We first declare the dictionary
    #           POM_COND_PROB_TABLES within which the Pomegranate objects are collected.
    POM_COND_PROB_TABLES = dict()
    #
    #           Begin with the root vertices. These are straightforward.  No dependencies by definition exist.
    for vert_idx in GRAPH_VERT_CLASS.get('ROOT_VERTS'):  ## vert_idx = GRAPH_VERT_CLASS.get('ROOT_VERTS')[0]
        #          Call local routine build_pom_cond_tab to build the object and store it in POM_COND_PROB_TABLES.
        POM_COND_PROB_TABLES = update_pom_cond_tab(graph_vert=vert_idx,
                                                   disc_cond_prob=COND_PROB_TABLES.get(vert_idx),
                                                   pom_cond_prob_tab=POM_COND_PROB_TABLES)
        #
        #           Now check off the integration of this vertex by calling vert_merg_checkoff.
        VERT_INTEG_CHECKLIST = vert_merg_checkoff(graph_vert=vert_idx,
                                                  vert_checklist=VERT_INTEG_CHECKLIST)
    #
    #        ⅲ. Now build out the non-root vertices. We first must check identify "merge-ready" vertices,
    #           those for which all of the immediate predecessor conditional-probability tables are already
    #           defined in POM_COND_PROB_TABLES. A local-funciton merge_ready compares the INTEG_VERTS list
    #           in VERT_INTEG_CHECKLIST to the observation-vertices in OBS_VERTS to identify vertices
    #           for which predecessor conditional-probabilities already exist in POM_COND_PROB_TABLES.
    #           We control our recursion with a while loop.  As we integrate each vertex, we move its
    #           label from the NON_INTEG_VERTS in VERT_INTEG_CHECKLIST to the INTEG_VERTS.  We stop
    #           when NON_INTEG_VERTS has zero-length.
    #
    #           Begin by identifying the first merge-ready vertex.
    vert_idx = merge_ready(merged_vert=VERT_INTEG_CHECKLIST.get('INTEG_VERTS'),
                           constit_vert_dict=OBS_VERTS)[0]
    #
    #           Now for some housekeeping.  Update VERT_INTEG_CHECKLINST, moving vert_idx from NON_INTEG_VERTS to INTEG_VERTS.  Then drop
    #           from OBS_VERTS the corresponding entry from OBS_VERTS.
    VERT_INTEG_CHECKLIST = vert_merg_checkoff(graph_vert=vert_idx,
                                              vert_checklist=VERT_INTEG_CHECKLIST)
    del OBS_VERTS[vert_idx]
    #
    #            Build up the conditional-probability table for graph vertex corresponding to vert_idx.  Call local routine build_pom_cond_tab.
    #            Repeat the procedure above as long as non-integrated vertices remain.
    POM_COND_PROB_TABLES = update_pom_cond_tab(graph_vert=vert_idx,
                                               disc_cond_prob=COND_PROB_TABLES.get(vert_idx),
                                               pom_cond_prob_tab=POM_COND_PROB_TABLES)
    while (len(VERT_INTEG_CHECKLIST.get('NON_INTEG_VERTS')) > 0):
        vert_idx = merge_ready(merged_vert=VERT_INTEG_CHECKLIST.get('INTEG_VERTS'),
                               constit_vert_dict=OBS_VERTS)[0]
        VERT_INTEG_CHECKLIST = vert_merg_checkoff(graph_vert=vert_idx,
                                                  vert_checklist=VERT_INTEG_CHECKLIST)
        del OBS_VERTS[vert_idx]
        POM_COND_PROB_TABLES = update_pom_cond_tab(graph_vert=vert_idx,
                                                   disc_cond_prob=COND_PROB_TABLES.get(vert_idx),
                                                   pom_cond_prob_tab=POM_COND_PROB_TABLES)
    #
    #    ⓒ Translate the conditional-/discrete-probability objects into Pomegranate node objects. We simply construct a dictionary
    #       within each entry is a pomegranate node object resulting from applying the Node function to the
    #       corresponding entry in POM_COND_PROB_TABLES.
    POM_NODES = dict()
    for vert_idx in list(POM_COND_PROB_TABLES.keys()): POM_NODES.update(
        {vert_idx: Node(POM_COND_PROB_TABLES.get(vert_idx),
                        name=str(vert_idx))})
    #
    #    ⓓ Declare the Pomegranate Bayesian network.  Add nodes and edges.
    MASTERY_DIAGNOSTIC_MODEL = BayesianNetwork(bayesnet_label)
    for vert_idx in list(POM_NODES.keys()):
        MASTERY_DIAGNOSTIC_MODEL.add_node(POM_NODES.get(vert_idx))
    for edge_idx in range(len(IN_RANGE_GRAPH_EDGES)):
        MASTERY_DIAGNOSTIC_MODEL.add_edge(POM_NODES.get(IN_RANGE_GRAPH_EDGES.iloc[edge_idx][0]),
                                          POM_NODES.get(IN_RANGE_GRAPH_EDGES.iloc[edge_idx][1]))
    #
    #    ⓔ Compile and return the result.
    MASTERY_DIAGNOSTIC_MODEL.bake()
    return {'Pomegranate_Bayesnet': MASTERY_DIAGNOSTIC_MODEL,
            'DiGraph_Vert_Order': list(POM_NODES.keys())}


#
# |∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕
# ⓚ INFER GROUP KNOWLEDGE STATE BY EXACT INFERENCE.  Use pgmpy's exact-inference calculation to estimate group knowledge state.
#    We use Pomegranate's loopy belief-propagation (LBP) except by exception.  Exact inference is limited to cases of relatively
#    small graphs. We use it in degernate cases for which Pomegranate's convergence is slow.  Star graphs represent a noteworkty exception.
#
#    Our logic here follows that of the Pomegranate. We perform the following steps.
#    ⑴ Construct a pgmpy Bayesian-network object.
#    ⑵ Prepare the evidence for application to the Bayesian Network.
#    ⑶ Apply evidence to query the Baysesian network.
#    ⑷ Assemble and return the results.
def exact_infer_group_know_state(bayesnet_digraph, wide_evid_dataframe, evid_prof_conformees, var_states, clust_idx):
    #    ⑴ Construct a pgmpy Bayesian-network object.
    #       ⒜ First declare the Bayesian model itself.  This directly follows from the edges of our Bayesian-network
    #          DiGraph.
    start_time_state_idx = tit.default_timer()
    pgmpy_bayesnet = BayesianModel(bayesnet_digraph.edges())
    #
    #       ⒝ We now specify conditional-probabilities for each vertex in the graph.
    #           ⅰ. We first need to identify  the immediate successors for each vertex. We identify the root and non-root vertices. We identify
    #              the immediate predecessors for the non-root vertices.
    vertex_in_degree = dict(pgmpy_bayesnet.in_degree())
    root_verts = [vert for (vert, degree) in vertex_in_degree.items() if degree == 0]
    non_root_verts = list(set(pgmpy_bayesnet.nodes()) - set(root_verts))
    non_root_predecessors = dict([(vert, list(nx.ego_graph(G=bayesnet_digraph.reverse(copy=True),
                                                           n=vert,
                                                           radius=1,
                                                           center=False,
                                                           undirected=False).nodes()))
                                  for vert in non_root_verts])  # vert = non_root_verts[0]
    #
    #           ⅱ. Now add tabular CPDs for the root vertices.
    var_state_cardinality = len(var_states) - 1
    for vert_idx in root_verts:
        pgmpy_bayesnet.add_cpds(TabularCPD(variable=vert_idx,
                                           variable_card=var_state_cardinality,
                                           values=np.reshape(
                                               a=CPT_LIST.loc[CPT_LIST['CONSTITUENT_COUNT'] == str(0)]['MEAS'].tolist(),
                                               newshape=(var_state_cardinality, 1))))
    for vert_idx in non_root_verts:  ## vert_idx = non_root_verts[0]
        pgmpy_bayesnet.add_cpds(TabularCPD(variable=vert_idx,
                                           variable_card=var_state_cardinality,
                                           evidence_card=list(np.repeat(a=var_state_cardinality,
                                                                        repeats=vertex_in_degree.get(vert_idx))),
                                           values=np.reshape(a=CPT_LIST.loc[
                                               CPT_LIST['CONSTITUENT_COUNT'] == str(vertex_in_degree.get(vert_idx))][
                                               'MEAS'].tolist(),
                                                             newshape=(var_state_cardinality,
                                                                       var_state_cardinality ** vertex_in_degree.get(
                                                                           vert_idx)),
                                                             order='F'),
                                           evidence=non_root_predecessors.get(vert_idx)))
    pgmpy_bayesnet_compiled = VariableElimination(pgmpy_bayesnet)
    #   pgmpy_bayesnet_compiled = BeliefPropagation(pgmpy_bayesnet)
    #   pgmpy_bayesnet_compiled._calibrate_junction_tree(operation = 'marginalize')
    #   pgmpy_bayesnet_compiled.max_calibrate()
    pgmpy_baysenet_build_time = tit.default_timer() - start_time_state_idx
    #
    #    ⑵ Group subjects according to evidentiary states.  We employ here our locally-defined groupby_evid_state
    #       subroutine. We use wide_evid_datarame, evid_state_cats as our function arguements.
    in_scope_verts = list(set(wide_evid_dataframe.columns.tolist()) \
                          .intersection(set(bayesnet_digraph.nodes())))
    evid_state_by_subj = groupby_evid_state(evid_states_to_be_grouped=wide_evid_dataframe[in_scope_verts],
                                            evid_prof_conformees=evid_prof_conformees,
                                            evid_state_cat_map=var_states.iloc[:-1]['CAT_LEVEL_IDX'])
    #
    #    ⑶ Berore uerying the Bayesian network for each evidentiary state, we require dataframes into which to
    #       collect the results.  One dataframe contains the Bayesian-network response variables. The other
    #       stores execution-time statistics.
    cluster_knowledge_state = pd.DataFrame(columns=['EVID_STATE_SIG', 'LEARNING_STANDARD_ID', 'KNOWLEDGE_LVL_TYPE'] + \
                                                   list(var_states['CAT_LEVEL_IDX'])[:-1])
    clust_exec_time = pd.DataFrame(columns=['EVID_STATE_SIG',
                                            'GRAPH_ORDER',
                                            'EDGE_COUNT',
                                            'MEAS_VERT_COUNT',
                                            'EST_VERT_COUNT',
                                            'TIME_NOW',
                                            'ELAPSED_TIME'],
                                   index=evid_state_by_subj.get('EVID_STATE_SIG'))
    clust_exec_time = clust_exec_time.assign(EDGE_COUNT=len(bayesnet_digraph.edges()))
    clust_exec_time = clust_exec_time.assign(GRAPH_ORDER=len(bayesnet_digraph.nodes()))
    clust_exec_time = clust_exec_time.assign(CLUSTER=clust_idx)
    clust_exec_time = clust_exec_time.assign(INFERENCE_APPROACH='EXACT')
    #
    #    ⑷ Now cycle through the evidentiary states. For each, invoke the locally-defined function query_pom_bayesnet.
    #       Concatenate each of the two items onto the cluster dataframe objects.
    for state_idx in evid_state_by_subj.get('EVID_STATE_SIG'):  # state_idx = evid_state_by_subj.get('EVID_STATE_SIG')[0]
        print('Evidentiary state ' + str(state_idx) + ', ' + str(
            evid_state_by_subj.get('EVID_STATE_SIG').index(state_idx) + 1) + \
              ' of ' + str(
            len(evid_state_by_subj.get('EVID_STATE_SIG'))) + ' evidentiary states for ' + clust_idx + ' at time ' + \
              str(datetime.utcnow().time()) + 'Z')
        start_time_state_idx = tit.default_timer()
        #
        #        ⅰ. Query the pgmpy Bayesian network. The EVID_STATE objects in our evid_state_by_subj dictionary contain the applied
        #           evidence. We must specify the return variables. We seek marginalized conditional probabilities for all non-measured
        #           vertices.
        bayesnet_query_resp = pgmpy_bayesnet_compiled.query(variables=list(set(bayesnet_digraph.nodes()) - \
                                                                           set(evid_state_by_subj.get(state_idx).get(
                                                                               'EVID_STATE').keys())),
                                                            evidence=evid_state_by_subj.get(state_idx).get('EVID_STATE'))
        #
        #        ⅱ. The pgmpy Bayesian-network query returns a dictionary of discrete factor objects in which the marginalized CPDs
        #           are stored. Convert this to a dataframe.
        know_state_state_idx = pd.DataFrame.from_dict(data=dict([(lrn_std, marg_cpd.values.tolist())
                                                                 for (lrn_std, marg_cpd) in
                                                                 bayesnet_query_resp.items()]),
                                                      orient='index')
        #
        #        ⅲ. Our Bayesian-network query response only spans estimated graph vertices. Assign unity to the corresponding
        #           learning-standard, knowledge-level categories for the measured vertices.
        for lrn_std in list(evid_state_by_subj.get(state_idx).get('EVID_STATE').keys()):
            know_state_state_idx.loc[lrn_std, evid_state_by_subj.get(state_idx).get('EVID_STATE').get(lrn_std)] = 1
        know_state_state_idx.fillna(value=0, inplace=True)
        #
        #        ⅳ. Fill out other attributes in our dataframe so that it coincides exactly with the information contained in
        #           the returned dataframe from a Pomegranate Bayesian-network query.
        know_state_state_idx = know_state_state_idx.assign(EVID_STATE_SIG=state_idx)
        know_state_state_idx = know_state_state_idx.assign(LEARNING_STANDARD_ID=know_state_state_idx.index.tolist())
        know_state_state_idx = know_state_state_idx.assign(KNOWLEDGE_LVL_TYPE='ESTIMATED')
        know_state_state_idx.loc[
            list(evid_state_by_subj.get(state_idx).get('EVID_STATE').keys()), 'KNOWLEDGE_LVL_TYPE'] = 'MEASURED'
        #
        #        ⅳ. Concatenate the query response onto our return variable.
        cluster_knowledge_state = pd.concat([cluster_knowledge_state,
                                             know_state_state_idx])
        #
        #        ⅳ. Assign values to the cluster-execution time dataframe.
        clust_exec_time.loc[state_idx,
                            ['EVID_STATE_SIG',
                             'TIME_NOW',
                             'ELAPSED_TIME',
                             'EST_VERT_COUNT',
                             'MEAS_VERT_COUNT']] = [state_idx,
                                                    str(datetime.utcnow().time()),
                                                    tit.default_timer() - start_time_state_idx,
                                                    len(list(set(bayesnet_digraph.nodes()) - \
                                                             set(evid_state_by_subj.get(state_idx).get(
                                                                 'EVID_STATE').keys()))),
                                                    len(evid_state_by_subj.get(state_idx).get('EVID_STATE').keys())]
        print(clust_exec_time.loc[state_idx].T.squeeze())
    #
    #    ⑷ Fill out cluster-execution time values.
    clust_exec_time = clust_exec_time.assign(BAYESNET_BUILD_TIME=pgmpy_baysenet_build_time)
    #
    #    ⑸ Introduce STUDENT_ID by merging the EVID_STATE_CONFORMEES dictionary object from evid_state_by_subj
    #       with our cluster_know_state object.
    cluster_knowledge_state = pd.merge(left=cluster_knowledge_state,
                                       right=evid_state_by_subj.get('EVID_STATE_CONFORMEES'))
    #
    #    ⑹ Package and return the results.
    # os.system('say "Another one bites the dust!"')
    return {'BAYESNET_QUERY_RESP': cluster_knowledge_state,
            'CLUSTER_EXEC_TIME': clust_exec_time}


#
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
                                                                              as_index=True).agg(
        lambda x: list(x)).to_dict(orient='index')
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
        if len(evid_prof.get(dict_idx).get('LEARNING_STANDARD_ID')) == 0: del evid_prof[dict_idx]

    #
    #     Our dictionary evid_prof is now our returned value.
    return evid_prof


#
# |∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕
# ⓜ GROUP BY EVIDENTIARY STATE.  A set of distinct states for a specific evidentiary profile constitutes an
#    evidentiary stae. We want during "batch-processing" to query the Bayesian network only once for
#    each evidentiary state and reuse that result for al subjects conforming to that state.  This subroutine —
#    very similar in logical flow to groupby_evid_profileile prodices a dictionary of evidentiary states
#    and of subject ID's for conformees to that state.
def groupby_evid_state(evid_states_to_be_grouped, evid_prof_conformees, evid_state_cat_map):
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
                                                               inplace=False).drop_duplicates().set_index(
        keys='EVID_STATE_SIG')
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


#
# |∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕
# ⓝ QUERY BAYESIAN NETWORK GIVEN A SPECIFIC EVIDENTIARY STATE, DIRECTED-ACYCLIC GRAPH.  We query the
#    pomegranate bayesnet using the predict_probla function.  Our inputs are:
#    ⧐ pom_bayesnet, the fully-compiled ("baked") Pomegranate Bayesian Network itself; and
#    ⧐ evid_state, a dictionary object with the evidentiary state applied to the Bayesian network.
#    The first input is actually a dictionary object. Its first entry is the compiled Bayesian network. The
#    second is a list containing the vertices in order of which they are reported-out by the query
#    function.
#
#    We return a dictionary containing two objects:
#    ⧐ A dataframe containing the marginal conditional probabilities for each vertex in the
#      DAG underlying the Bayesian network; and
#    ⧐ A single-row datframe containing execution-time statistics.
def query_pom_bayesnet(evid_state, pom_bayesnet, var_states, state_idx):
    #    ⑴ Keeping track of execution time is particular important at this juncture.  So we begin with
    #       our execution-time statistics.
    start_time_state_idx = tit.default_timer()
    CLUST_EXEC_TIME_state_idx = pd.DataFrame(columns=['EVID_STATE_SIG',
                                                      'GRAPH_ORDER',
                                                      'EDGE_COUNT',
                                                      'MEAS_VERT_COUNT',
                                                      'EST_VERT_COUNT',
                                                      'TIME_NOW',
                                                      'ELAPSED_TIME'],
                                             index=[state_idx])
    CLUST_EXEC_TIME_state_idx.loc[state_idx, ['EVID_STATE_SIG',
                                              'GRAPH_ORDER',
                                              'EDGE_COUNT',
                                              'MEAS_VERT_COUNT',
                                              'EST_VERT_COUNT',
                                              'TIME_NOW']] = [state_idx,
                                                              pom_bayesnet.get('Pomegranate_Bayesnet').node_count(),
                                                              pom_bayesnet.get('Pomegranate_Bayesnet').edge_count(),
                                                              len(evid_state),
                                                              pom_bayesnet.get(
                                                                  'Pomegranate_Bayesnet').node_count() - len(
                                                                  evid_state),
                                                              str(datetime.now().time())]
    #
    #    ⑵ Now, query the Bayesian network.	  We use the Pomegranate predict_proba funciton to caluclate a set of marginalized conditional
    #       probability variables.  We apply the returned value as an argument to our locally-defined extract_bayesnet_query
    #       function in order to transform the result into a dataframe. We add the state_idx variable as an EVID_STATE_SIG value for our
    #       dataframe. We also add the vertex labels as a LEARNING_STANDARD_ID attribute.  We also have the opportunity here to
    #       distinguish between measured and estimated vertices. This attribute is stored in the KNOWLEDGE_LVL_TYPE attribute.
    bayesnet_query_resp = extract_bayesnet_query(
        pred_proba_result=pom_bayesnet.get('Pomegranate_Bayesnet').predict_proba(evid_state),
        graph_node_order=pom_bayesnet.get('DiGraph_Vert_Order'),
        evid_state=evid_state,
        var_states=var_states
    )[var_states.iloc[:-1]['CAT_LEVEL_IDX'].tolist()]
    #
    bayesnet_query_resp['EVID_STATE_SIG'] = state_idx
    bayesnet_query_resp['LEARNING_STANDARD_ID'] = bayesnet_query_resp.index.values.tolist()
    bayesnet_query_resp.loc[list(evid_state.keys()), 'KNOWLEDGE_LVL_TYPE'] = 'MEASURED'
    bayesnet_query_resp.loc[pd.isnull(bayesnet_query_resp['KNOWLEDGE_LVL_TYPE']), 'KNOWLEDGE_LVL_TYPE'] = 'ESTIMATED'
    #
    #     ⑶ Record and report the elapsed time for calcuiation of the Bayesnet Query.
    CLUST_EXEC_TIME_state_idx.loc[state_idx, ['TIME_NOW', 'ELAPSED_TIME']] = \
        [str(datetime.now().time()),
         tit.default_timer() - start_time_state_idx]
    print(CLUST_EXEC_TIME_state_idx.T.squeeze())
    #
    #     ⑷ Build up and return the query result.
    return {'BAYESNET_QUERY_RESP': bayesnet_query_resp,
            'CLUSTER_EXEC_TIME': CLUST_EXEC_TIME_state_idx}  #


#
# |∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕
# ⓞ BUILD KNOWLEDGE-STATE DATAFRAME FOR ALL VARIABLES IN A SUBGRAPH FOR ALL CONFORMEES TO AN EVIDENTIARY PROFILE.
#    This subroutine controls the query_pom_bayesnet subroutine defined above. We require for this purpose:
#    ⧐ A SUBGRAPH dictionary object containing a digraph on which to base a Bayesian network;
#    ⧐ A dataframe containing subject (aka "student") evidentiary states for variables contained within
#      the digraph object;
#    ⧐ A list of admissible variable states.
#
#    We produce a dictionary item containing two items:
#    ⧐ A dataframe reporting the knowledge — in terms of marginalized, conditional probabilities —
#      for all subjects, learning targets (aka, students, learning standards) for each
#      admissible evidentiary state;
#    ⧐ A dataframe reporting execution-time statistics for each query of the Bayesian network.
#
#    The subroutine performs the following activities.
#    ⑴ Construct the Pomegranate bayesian-network dictionary object.
#    ⑵ Group subjects according to evidentiary state with respect to all of the learning targets
#       (aka learning standards) wthin the span of the digrap.
#    ⑶ Query the Bayesian network for each evidentiary state.
#    ⑷ Assemble the results and return them to the next-higher hieraarchical work-unit level.
#
def approx_infer_group_know_state(bayesnet_digraph, wide_evid_dataframe, evid_prof_conformees, var_states, clust_idx):
    #    ⑴ Construct the Pomegranate bayesian-network dictionary object.  This occurs from straighforard invocation
    #       of our locally-defined build_pom_bayesnet function.  We apply our bayesnet_digraph and clust-idx
    #       as function arguments.
    start_time_state_idx = tit.default_timer()
    cluster_bayesnet = build_pom_bayesnet(directed_graph=bayesnet_digraph,
                                          var_states=var_states,
                                          bayesnet_label=clust_idx)
    pom_baysenet_build_time = tit.default_timer() - start_time_state_idx
    #   plt.figure(figsize = (14,10))
    #   cluster_bayesnet.get('Pomegranate_Bayesnet').plot()
    #   plt.show()
    #
    #
    #    ⑵ Group subjects according to evidentiary states.  We employ here our locally-defined groupby_evid_state
    #       subroutine. We use wide_evid_datarame, evid_state_cats as our function arguements.
    in_scope_verts = list(set(wide_evid_dataframe.columns.tolist()) \
                          .intersection(set(bayesnet_digraph.nodes())))
    evid_state_by_subj = groupby_evid_state(evid_states_to_be_grouped=wide_evid_dataframe[in_scope_verts],
                                            evid_prof_conformees=evid_prof_conformees,
                                            evid_state_cat_map=var_states.iloc[:-1]['CAT_LEVEL_IDX'])
    #
    #    ⑶ Berore uerying the Bayesian network for each evidentiary state, we require dataframes into which to
    #       collect the results.  One dataframe contains the Bayesian-network response variables. The other
    #       stores execution-time statistics.
    cluster_knowledge_state = pd.DataFrame(columns=['EVID_STATE_SIG', 'LEARNING_STANDARD_ID', 'KNOWLEDGE_LVL_TYPE'] + \
                                                   list(var_states['CAT_LEVEL_IDX'])[:-1])
    clust_exec_time = pd.DataFrame(columns=['EVID_STATE_SIG',
                                            'GRAPH_ORDER',
                                            'EDGE_COUNT',
                                            'MEAS_VERT_COUNT',
                                            'EST_VERT_COUNT',
                                            'TIME_NOW',
                                            'ELAPSED_TIME'])
    #
    #    ⑷ Now cycle through the evidentiary states. For each, invoke the locally-defined function query_pom_bayesnet.
    #       Concatenate each of the two items onto the cluster dataframe objects.
    for state_idx in evid_state_by_subj.get(
            'EVID_STATE_SIG'):  # state_idx = evid_state_by_subj.get(p'EVID_STATE_SIG')[0]
        print('Evidentiary state ' + str(state_idx) + ', ' + str(
            evid_state_by_subj.get('EVID_STATE_SIG').index(state_idx) + 1) + \
              ' of ' + str(
            len(evid_state_by_subj.get('EVID_STATE_SIG'))) + ' evidentiary states for ' + clust_idx + ' at time ' + \
              str(datetime.utcnow().time()) + 'Z')
        bayesnet_query_resp = query_pom_bayesnet(evid_state=evid_state_by_subj.get(state_idx).get('EVID_STATE'),
                                                 pom_bayesnet=cluster_bayesnet,
                                                 var_states=var_states,
                                                 state_idx=state_idx)
        #
        cluster_knowledge_state = pd.concat([cluster_knowledge_state,
                                             bayesnet_query_resp.get('BAYESNET_QUERY_RESP')])
        clust_exec_time = pd.concat([clust_exec_time,
                                     bayesnet_query_resp.get('CLUSTER_EXEC_TIME')])
    #
    #    ⑸ Introduce STUDENT_ID attribute to  cluster_knowledge_state by joining with the
    #       EVID_STATE_CONFORMEES item from evid_state_by_subj.
    cluster_knowledge_state = pd.merge(left=cluster_knowledge_state,
                                       right=evid_state_by_subj.get('EVID_STATE_CONFORMEES')) \
        .sort_values(by=['STUDENT_ID',
                         'LEARNING_STANDARD_ID'],
                     axis=0)
    clust_exec_time['CLUSTER'] = clust_idx
    clust_exec_time['BAYESNET_BUILD_TIME'] = pom_baysenet_build_time
    #
    #    ⑹ Package and return the results.
    # os.system('say "Another one bites the dust!"')
    return {'BAYESNET_QUERY_RESP': cluster_knowledge_state,
            'CLUSTER_EXEC_TIME': clust_exec_time}



#
#
# |∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕
# ⓠ ASSEMBLE ALL KNOWLEDGE-STATE ESTIMATES FROM ALL EVIDENTIARY-PROFILES.  Our groupby_evid_profile
#    creates a dictionary of evidentiary-profile metadata: STUDENT_IDs and measured
#    LEARNING_STANDARD_IDs for each profile.  The est_know_state_for_evid_prof subroutine
#    adds a knowledge-state each dataframe. We now assemble all of our estimates into a
#    single dataframe.  Our inputs are:
#    ⧐ evid_prof_dict_obj, the evidentiary-profile-metadata dictionary object produced
#      by the groupby_evid_profile subroutine;
#    ⧐ wide_evid_dataframe (EoL_WIDE, here) our comprehensive evidentiary-state
#      wide-table-format dataframe; and
#    ⧐ var_states is a dataframe — derived from MASTERY_LEVEL_CAT — containing the
#      correspondance between variable-state category-level indices and labels.
#    We produce a single dataframe KNOWLEDGE_STATE as an output.
#
#    This subroutine performs the following procedure.
#    ⑴ Concatenate all of the KNOWLEDGE_STATE items from each dictionary entry in evid_prof_dict
#       into a single dataframe.
#    ⑵ Calculate the entropy of the marginalized conditional probabilities and write
#       the results into a DEVIATION.
#    ⑶ Expand this dataframe by left-outer-joining it onto a cartesian product of all
#       LEARNING_STANDARD_IDs, STUDENT_IDs in wide_evid_dataframe.
#    ⑷ Conditionally fill in appropriate values for missing values resulting from
#       the join with the cartesian product.
#    ⑸ Rename the variable-state columns with their adjective-descriptive
#       knowledge-state labels as opposed to the numeric ones used by
#       the Pomegranate Bayesian network.
#    ⑹ Return the result as a dataframe for subsequent serial operations.
def assemble_knowledge_state_table(know_state,
                                   vert_list,
                                   digraph_edge_list,
                                   admiss_score_partitions,
                                   analysis_case_parameters):
    #    ⑴ Concatenate all of the KNOWLEDGE_STATE items.  Construct a list of dictionary keys
    #       to use as control-loop index variables.  First extract the first. Then get the rest
    #       by looping through.
    #   evid_prof_keys = list(evid_prof_dict.keys())
    #   know_state = evid_prof_dict.get(evid_prof_keys[0]).get('KNOWLEDGE_STATE_ESTIMATE')
    #   for prof_idx in evid_prof_keys[1:]:
    #       know_state = pd.concat([know_state,
    #                               evid_prof_dict.get(prof_idx).get('KNOWLEDGE_STATE_ESTIMATE')])
    #
    #    ⑵ Calculate the entropy and prevision of the marginalized conditional probabilities and write into
    #       DEVIATION column.
    #       ⒜ For convenience, create a var_state_indecies attribute from the CAT_LVL_IDX
    #          values in var_states. Exclude the final.
    var_state_indces = admiss_score_partitions['CAT_LEVEL_IDX'].tolist()[:-1]
    #
    #       ⒝ Extract the conditional probabilities, the columns of know_state corresponding
    #          to var_state_indces.  Add a negilible offset to the marginal-conditional probabilities
    #          to avoid subsequent divide-by-zero warnings.  Calculate the entropy, as ussual,
    #          as the expected logarithm of the probability. Use a logarithm of base corresponding
    #          length of the var_state_indces list.
    marg_cond_probs = know_state[var_state_indces].as_matrix(columns=var_state_indces) \
                      + np.ones_like(know_state[var_state_indces].as_matrix(columns=var_state_indces)) / 10 ** 12
    entropy = -np.sum(a=np.multiply(marg_cond_probs, np.log(marg_cond_probs)),
                      axis=1) / np.log(len(var_state_indces))
    #
    #       ⒞ Assign etropy as the DEVIATION attribute of know_state.
    know_state['KNOWLEDGE_LVL_DEVIATION'] = entropy
    #
    #       ⒟ Associate each record with a prevision.  Our prevision is the conditional-probability
    #          weighted sum of the knowledge-state partition midpoints.  Construction a prevision
    #          vector and calculate the inner product between that and the conditional probabilities.
    prevision_vector = np.reshape(a=np.asmatrix(np.mean(a=admiss_score_partitions.iloc[0:-1][['LOW_BOUND', 'UP_BOUND']],
                                                        axis=1)),
                                  newshape=(len(admiss_score_partitions) - 1, 1))
    know_state['KNOWLEDGE_LVL_PREVISION'] = know_state[
        admiss_score_partitions.iloc[0:-1]['CAT_LEVEL_IDX'].values.tolist()] \
        .dot(prevision_vector)
    #
    #    ⑶ Associate knowledge state with datestamps based on dates of evidentiary measurements and
    #       knowledge-state estimation.
    #       ⒜ We first associate each proficiency-target (aka "learning standard") knowledge state with
    #          the date on which the measurement was taken. This logic resembles that in build_wide_evid_state_table.
    #          We group our body-of-evidence long-table vert_list by STUDENT_ID, LEARNING_STANDARD_ID, retaining
    #          the most-recent ASSESSMENT_DATE attribute. This gives us the date of evidentiary measurement
    #          for each STUDENT_ID × LEARNING_STANDARD_ID tuple.
    evident_meas_date = vert_list[['STUDENT_ID',
                                   'LEARNING_STANDARD_ID',
                                   'ASSESSMENT_DATE']].groupby(by=['STUDENT_ID',
                                                                   'LEARNING_STANDARD_ID'],
                                                               as_index=False)['ASSESSMENT_DATE'].max()
    know_state = pd.merge(left=know_state,
                          right=evident_meas_date.rename(columns={'ASSESSMENT_DATE': 'KNOWLEDGE_LVL_EVIDENCE_DATE'}),
                          how='left')
    #
    #        ⒝ A subsequent, similar operation yields the KNOWLEDGE_LVL_ASOF_DATE attribute. The knowledge state
    #           effectivity is based on the date of the most-recent of any evidentiary measurements for each subject.
    know_state_date = evident_meas_date.groupby(by=['STUDENT_ID'],
                                                as_index=False)['ASSESSMENT_DATE'].max()
    know_state = pd.merge(left=know_state,
                          right=know_state_date.rename(columns={'ASSESSMENT_DATE': 'KNOWLEDGE_LVL_ASOF_DATE'}),
                          how='left')
    #
    #    ⑷ Replace the observed RAW_SCORE attribute from our long-table body-of-evidence vert_idx as the
    #       KNOWLEDGE_LVL_PREVISION value for measured variables. Identify these by inner-joining
    #       vert_list with evident_meas_date and retaining the lowest value — that which was employed in
    #       evaluate_group_know_state. Left-outer-join the result onto know_state, and overwrite the
    #       KNOWLEDGE_LVL_PREVISION with RAW_SCORE for instances in which the latter is non-null.
    #       Then overwrite all RAW_SCORE values with KNOWLEDGE_LVL_PREVISION, as subsequently required by
    #       for purposes of associating prevision values with a knowledge-state category.
    evid_meas_raw_score = pd.merge(left=vert_list[list(evident_meas_date.columns)
                                                  + ['RAW_SCORE']],
                                   right=evident_meas_date).groupby(by=list(evident_meas_date.columns),
                                                                    as_index=False)['RAW_SCORE'].min()
    #
    #       Join back onto vert_list to get WORK_PRODUCT_TITLE attribute.
    evid_meas_raw_score = pd.merge(left=evid_meas_raw_score,
                                   right=vert_list[list(evid_meas_raw_score.columns) + ['WORK_PRODUCT_TITLE']])
    know_state = pd.merge(left=know_state,
                          right=evid_meas_raw_score,
                          how='left')
    know_state.loc[pd.notnull(know_state['RAW_SCORE']), 'KNOWLEDGE_LVL_PREVISION'] \
        = know_state.loc[pd.notnull(know_state['RAW_SCORE']), 'RAW_SCORE']
    know_state['RAW_SCORE'] = know_state['KNOWLEDGE_LVL_PREVISION']
    #
    #    ⑸ Use internall-defined function numeric0_100_to_cat to assign each knowledge state —
    #       whether measured or observed — to a knowledge-state category.
    know_state = numeric0_100_to_cat(long_evid_dataframe=know_state,
                                     admiss_score_partitions=admiss_score_partitions) \
        .rename(columns={'MASTERY_LEVEL_CAT': 'KNOWLEDGE_LEVEL'})
    #
    #    ⑹ Expand this dataframe by left-outer-joining it onto a cartesian product of all
    #       LEARNING_STANDARD_IDs, STUDENT_IDs in the proficiency-model scope coinciding with the
    #       vertex span of the Bayesian-network digraph.
    proficiency_model_scope = list(set(digraph_edge_list['LEARNING_STANDARD_ID']) \
                                   .union(set(digraph_edge_list['CONSTITUENT_LEARNING_STD_ID'])))
    know_state = pd.merge(right=know_state,
                          left=pd.DataFrame(data=list(it.product(list(set(know_state['STUDENT_ID'])),
                                                                 proficiency_model_scope)),
                                            columns=['STUDENT_ID', 'LEARNING_STANDARD_ID']),
                          how='left')
    #
    #    ⑺ Conditionally fill in appropriate values for missing values resulting from
    #       the join with the cartesian product.
    #       ⒜ Null-values of KNOWLEDGE_LEVEL become "UNMEASURED".
    know_state.loc[pd.isnull(know_state['KNOWLEDGE_LEVEL']), 'KNOWLEDGE_LEVEL'] = 'UNMEASURED'
    #
    #       ⒝ KNOWLEDGE_LVL_PREVISION and KNOWLEDGE_LVL_DEVIATION become negative one and unity, respectively.
    know_state.loc[pd.isnull(know_state['KNOWLEDGE_LVL_PREVISION']), 'KNOWLEDGE_LVL_PREVISION'] = -1.0
    know_state.loc[pd.isnull(know_state['KNOWLEDGE_LVL_DEVIATION']), 'KNOWLEDGE_LVL_DEVIATION'] = 1.0
    know_state = know_state.astype(str)
    #
    #       ⒞ A variety of other columns get populated with NULL.
    for col_idx in ['KNOWLEDGE_LVL_EVIDENCE_DATE',
                    'KNOWLEDGE_LVL_ASOF_DATE',
                    'KNOWLEDGE_LVL_TYPE',
                    'WORK_PRODUCT_TITLE']:
        know_state.loc[pd.isnull(know_state[col_idx]), col_idx] = 'NULL'
    know_state['STDNT_KNWLDG_LVL_COMMENT'] = 'NULL'
    know_state['BASED_ON_MEASUREMENT_YN'] = '1'
    know_state['TENANT_ID'] = analysis_case_parameters.loc['TENANT_ID']['VALUE']
    #
    #    ⑻ Contrive a unique hash-key STUDENT_KNOWLEDGE_LVL_SID attribute.
    know_state['STUDENT_KNOWLEDGE_LVL_SID'] = list(know_state.apply(lambda x: hash(tuple(x)), axis=1))
    #
    #    ⑼ Assign system-administration attributes.
    know_state['LAST_UPDATE_DT'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    know_state['LAST_UPDATE_USER'] = Last_Upd_Usr
    know_state['LAST_UPDATE_TX_ID'] = Last_Upd_Trans
    #
    #    ⑼ Reduce the columns to those specified in the data dictionary. Coerce all attributes to string.  Return
    #       the final result.
    know_state = know_state[['STUDENT_KNOWLEDGE_LVL_SID',
                             'LEARNING_STANDARD_ID',
                             'KNOWLEDGE_LVL_ASOF_DATE',
                             'TENANT_ID',
                             'KNOWLEDGE_LEVEL',
                             'KNOWLEDGE_LVL_EVIDENCE_DATE',
                             'KNOWLEDGE_LVL_TYPE',
                             'BASED_ON_MEASUREMENT_YN',
                             'STDNT_KNWLDG_LVL_COMMENT',
                             'KNOWLEDGE_LVL_DEVIATION',
                             'KNOWLEDGE_LVL_PREVISION',
                             'STUDENT_ID',
                             'WORK_PRODUCT_TITLE',
                             'LAST_UPDATE_DT',
                             'LAST_UPDATE_TX_ID',
                             'LAST_UPDATE_USER']].drop_duplicates()
    return know_state


#
#
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
def evaluate_group_know_state(group_evid_state, digraph_edge_list, var_states):
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
    #    🄱 Break the within-evidentiary-range induced subgraph into 'bite-sized' estimation subgraphs
    cohort_subgraphs = decompose_digraph(composite_digraph=in_range_digraph,
                                         measured_verts=graph_measured_verts)
    #
    #    🄲 We now cycle through each subgraph.
    for subgraph_idx in list(cohort_subgraphs.keys()):  ## subgraph_idx = list(cohort_subgraphs.keys())[1]
        print('Subgraph ' + str(subgraph_idx) + ', ' + str(list(cohort_subgraphs.keys()).index(subgraph_idx) + 1) + \
              ' of ' + str(len(cohort_subgraphs)) + ' subgraphs at time ' + str(datetime.utcnow().time()) + 'Z')
        #       ⑴ First, group subjects into evidentiary profiles for the learning standards in the subgraph_idxᵗʰ subgraph.
        evid_prof_sub_idx = groupby_evid_profile(
            group_evid_state[cohort_subgraphs.get(subgraph_idx).get('SPANNING_SUBGRAPH').nodes()])
        #
        #       ⑵ Next construct and query a Bayesian network for each evidentiary state.  We must check each for the degerate state
        #          in which the all vertices are measured.  Define ℳᵢⱼ as the measured vertices for the jᵗʰ evidentiary state of
        #          the iᵗʰ subgraph 𝒢ᵢ, whose vertices are 𝒳ᵢ.  If ℳᵢⱼ ⊇ 𝒳ᵢ — that is the set of measured vertices ℳᵢⱼ completely
        #          contains the set of vertices 𝒳ᵢ in 𝒢ᵢ, then we don't want to construct and query a Bayesian network. We instead
        #          want to directly transform our group evidentiary state into a set of marginalized conditional-probability tables.
        for prof_idx in list(evid_prof_sub_idx.keys()):  ## prof_idx = list(evid_prof_sub_idx.keys())[1]
            print(
            'Evidentiary profile ' + str(prof_idx) + ', ' + str(list(evid_prof_sub_idx.keys()).index(prof_idx) + 1) + \
            ' of ' + str(len(evid_prof_sub_idx)) + ' evidentiary profiles for ' + str(subgraph_idx) + \
            ' at time ' + str(datetime.utcnow().time()) + 'Z')
            #
            #          ⅰ. Check first to see if all of the vertices are measured. Define
            all_subgraph_verts_meas = set(cohort_subgraphs.get(subgraph_idx).get('SPANNING_SUBGRAPH').nodes()) \
                .issubset(evid_prof_sub_idx.get(prof_idx).get('LEARNING_STANDARD_ID'))
            #
            #
            #          ⅱ. If all subgraph vertices are measured — that is, ℳᵢⱼ ⊇ 𝒳ᵢ — then we transform the group evidentiary state into
            #             a table of conditional-probability distributions, with unit value.
            if all_subgraph_verts_meas:
                evid_state_by_subj = groupby_evid_state(
                    evid_states_to_be_grouped=group_evid_state.loc[
                        evid_prof_sub_idx.get(prof_idx).get('STUDENT_ID'),
                        cohort_subgraphs.get(subgraph_idx).get('SPANNING_SUBGRAPH').nodes()],
                    evid_prof_conformees=evid_prof_sub_idx.get(prof_idx).get('STUDENT_ID'),
                    evid_state_cat_map=var_states['CAT_LEVEL_IDX'][:-1])

                know_state_state_idx = pd.concat([pd.get_dummies(data=pd.DataFrame(
                    data={'LEARNING_STANDARD_ID': list(evid_state_by_subj.get(state_idx).get('EVID_STATE').keys()),
                          'category': pd.Categorical(
                              values=evid_state_by_subj.get(state_idx).get('EVID_STATE').values(),
                              categories=var_states['CAT_LEVEL_IDX'][:-1])}),
                    columns=['category'],
                    prefix='',
                    prefix_sep='') \
                                                 .assign(EVID_STATE_SIG=state_idx) \
                                                  for state_idx in evid_state_by_subj.get('EVID_STATE_SIG')]) \
                    .rename(columns=dict(zip(var_states['CAT_LEVEL_IDX'][:-1].astype(str),
                                             var_states['CAT_LEVEL_IDX'][:-1])))
                know_state_clust_idx = pd.merge(left=know_state_state_idx,
                                                right=evid_state_by_subj.get('EVID_STATE_CONFORMEES')) \
                    .assign(KNOWLEDGE_LVL_TYPE='MEASURED')
                clust_exec_time = pd.DataFrame(data={'EVID_STATE_SIG': evid_state_by_subj.get('EVID_STATE_SIG'),
                                                     'GRAPH_ORDER': cohort_subgraphs.get(subgraph_idx).get(
                                                         'SPANNING_SUBGRAPH').order(),
                                                     'EDGE_COUNT': len(cohort_subgraphs.get(subgraph_idx).get(
                                                         'SPANNING_SUBGRAPH').edges()),
                                                     'MEAS_VERT_COUNT': len(
                                                         evid_prof_sub_idx.get(prof_idx).get('LEARNING_STANDARD_ID')),
                                                     'EST_VERT_COUNT': len(cohort_subgraphs.get(subgraph_idx).get(
                                                         'SPANNING_SUBGRAPH').nodes()) - \
                                                                       len(evid_prof_sub_idx.get(prof_idx).get(
                                                                           'LEARNING_STANDARD_ID')),
                                                     'TIME_NOW': datetime.utcnow().time(),
                                                     'ELAPSED_TIME': 0.0,
                                                     'CLUSTER': subgraph_idx,
                                                     'INFERENCE_APPROACH': 'NONE',
                                                     'BAYESNET_BUILD_TIME': 0.0,
                                                     'EVID_PROF_SIG': prof_idx})
                evid_prof_sub_idx.get(prof_idx).update({'BAYESNET_QUERY_RESP': know_state_clust_idx,
                                                        'CLUSTER_EXEC_TIME': clust_exec_time})
            #
            #          ⅲ. Otherwise construct and query the Bayesian network.
            else:
                evid_prof_est_range_digraph = build_induced_inrange_graph(meas_list=evid_prof_sub_idx.get(prof_idx).get('LEARNING_STANDARD_ID'),
                                                                        evid_radius=2,
                                                                        course_nhbd_graph=cohort_subgraphs.get(subgraph_idx).get('SPANNING_SUBGRAPH'))
            #
                if cohort_subgraphs.get(subgraph_idx).get('IS_STAR_GRAPH'):
                    cluster_bayesnet_query = query_star_graph_Bayesnet(
                        bayesnet_digraph=evid_prof_est_range_digraph,
                        wide_evid_dataframe=group_evid_state.loc[evid_prof_sub_idx.get(prof_idx).get('STUDENT_ID'),
                                                                 list(evid_prof_est_range_digraph.nodes())],
                        evid_prof_conformees=evid_prof_sub_idx.get(prof_idx).get('STUDENT_ID'),
                        var_states=var_states,
                        clust_idx=subgraph_idx)
                elif len(cohort_subgraphs.get(subgraph_idx).get('SPANNING_SUBGRAPH').nodes()) <= 10:
                    cluster_bayesnet_query = exact_infer_group_know_state(
                        bayesnet_digraph=evid_prof_est_range_digraph,
                        wide_evid_dataframe=group_evid_state.loc[evid_prof_sub_idx.get(prof_idx).get('STUDENT_ID'),
                                                                 list(evid_prof_est_range_digraph.nodes())],
                        evid_prof_conformees=evid_prof_sub_idx.get(prof_idx).get('STUDENT_ID'),
                        var_states=var_states,
                        clust_idx=subgraph_idx)
                else:
                    cluster_bayesnet_query = approx_infer_group_know_state(
                        bayesnet_digraph=evid_prof_est_range_digraph,
                        wide_evid_dataframe=group_evid_state.loc[evid_prof_sub_idx.get(prof_idx).get('STUDENT_ID'),
                                                                 list(evid_prof_est_range_digraph.nodes())],
                        evid_prof_conformees=evid_prof_sub_idx.get(prof_idx).get('STUDENT_ID'),
                        var_states=var_states,
                        clust_idx=subgraph_idx)
                evid_prof_sub_idx.get(prof_idx).update(cluster_bayesnet_query)
                evid_prof_sub_idx.get(prof_idx).update(
                    {'CLUSTER_EXEC_TIME': evid_prof_sub_idx.get(prof_idx).get('CLUSTER_EXEC_TIME')\
                            .assign(EVID_PROF_SIG=prof_idx)})
                # os.system('say "Another ones down and another ones gone and"')
                #
                #       ⑶ Store the query-response results in the cohort_subgraphs dictionary.
        cohort_subgraphs.get(subgraph_idx).update({'BAYESNET_QUERY_RESP':
                                                       pd.concat(
                                                           [evid_prof_sub_idx.get(prof_idx).get('BAYESNET_QUERY_RESP')
                                                            for prof_idx in list(evid_prof_sub_idx.keys())])})
        cohort_subgraphs.get(subgraph_idx).update({'CLUSTER_EXEC_TIME':
                                                       pd.concat(
                                                           [evid_prof_sub_idx.get(prof_idx).get('CLUSTER_EXEC_TIME')
                                                            for prof_idx in list(evid_prof_sub_idx.keys())])})
    # os.system('say "Here we go again!"')
    #
    #    🄵 Assemble the Bayesian-network query responses and return the result.
    #       ⑴ First, concatenate the BAYESNET_QUERY_RESP and CLUSTER_EXEC_TIME dataframes for each subgraph into
    bayesnet_query_resp = pd.concat([cohort_subgraphs.get(subgraph_idx).get('BAYESNET_QUERY_RESP')
                                     for subgraph_idx in list(cohort_subgraphs.keys())])
    clust_exec_time = pd.concat([cohort_subgraphs.get(subgraph_idx).get('CLUSTER_EXEC_TIME')
                                 for subgraph_idx in list(cohort_subgraphs.keys())])
    #
    #       ⑵ We now must mean-aggregate know_state by LEARNING_STANDARD_ID, STUDENT_ID. Subgraph edge vertices
    #          can be shared in multiple subgraphs. We mean-aggregate CPD values in order to combine results from
    #          multiple Bayesian-network queries.
    bayesnet_query_agg = \
    bayesnet_query_resp[var_states[:-1]['CAT_LEVEL_IDX'].tolist() + ['LEARNING_STANDARD_ID', 'STUDENT_ID']]\
        .groupby(by=['LEARNING_STANDARD_ID', 'STUDENT_ID'],
                 as_index=False)[var_states[:-1]['CAT_LEVEL_IDX'].tolist()].agg(lambda x: np.mean(x))
    know_state = pd.merge(left=bayesnet_query_resp[['EVID_STATE_SIG',
                                                    'KNOWLEDGE_LVL_TYPE',
                                                    'LEARNING_STANDARD_ID',
                                                    'STUDENT_ID']].drop_duplicates(subset=['LEARNING_STANDARD_ID',
                                                                                           'STUDENT_ID'],
                                                                                   keep='first'),
                          right=bayesnet_query_agg)
    #
    #    Return the updated dictionary object.
    return {'BAYESNET_QUERY_RESP': know_state,
            'CLUSTER_EXEC_TIME': clust_exec_time}
#
#
# |∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕
# ⓢ ASSEMBLE CLUTER-EXECUTION-TIME STATISTICS.  The evaluate_group_know_state subroutine updates each dictionary
#    produced by groupby_evid_profile with knowledge-state estimates for each vertex in the course graphical
#    neighborhod, as well as a table of bayesnet-query execution-time statistics. The assemble_knowledge_state_estimates
#    subroutine extracts, extends, concatenates the former. We seek here to extract and concatenate the
#    latter.
#
#    Our input simply is the dictionary object resulting from updating with results from evaluate_group_know_state
#    that produced by groupby_evid_profile. We return a dataframe of cluster-time execution statistics.
#    We add the evidentiary-profile index as an attribute to each such dataframe was we extract and concatenate.
def assemble_exec_time_states(evid_prof_dict):
    #    First get a list of keys from our input dictionary object evid_prof_dict. These are the evidentiary-profile
    #    signature hash keys EVID_PROFILE.
    dict_keys = list(evid_prof_dict.keys())
    #
    #    Get the first cluster-execution time table. Add to it the EVID_PROF_SIG object.
    clust_exec_time = evid_prof_dict.get(dict_keys[0]).get('CLUSTER_EXEC_TIME')
    clust_exec_time['PROF_IDX'] = dict_keys[0]
    #
    #    Now repeat this for each remaining item, concatenating onto clust_exec_time.
    for key_idx in dict_keys[1:]:
        clust_exec_key_idx = evid_prof_dict.get(key_idx).get('CLUSTER_EXEC_TIME')
        clust_exec_key_idx['PROF_IDX'] = key_idx
        clust_exec_time = pd.concat([clust_exec_time,
                                     clust_exec_key_idx])
    #
    #    Return the result.
    return clust_exec_time


#
#
# |∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕
# ⓣ ASSIGN EVIDENTIARY MEASUREMENT TO KNOWLEDGE-STATE CAGEGORY GIVEN A RAW SCORE.  We get evidentiary measurements
#    as raw numeric scores recorded as 0-100 measurement. Our Pomegranate Bayesian-network approach operates exclusivel on
#    categorical data. We therefore must transform our numeric measurement to categories. We map our numeric to categories
#    based on the exhaustive, contiguous partitions into which they fall.
#
#    We require two inputs:
#    ⧐ A long table long_evid_dataframe of evidentiary measurements with numeric raw scores; and
#    ⧐ A dataframe admiss_score_partitions containing the knowledge-state categories expressed in
#      terms of partitions of our 0-100 numeric scale.
#
#    We return long_evid_dataframe with an additional KNOWLEDGE_LEVEL attribute added containing
#    the implied knowledge state of the numeric score.
def numeric0_100_to_cat(long_evid_dataframe, admiss_score_partitions):
    #    ⑴ Begin by extracting from admiss_score_partitions the partino boundaries by which our knowledge-state


    #       categories are specified.
    partion_boundaries = list(admiss_score_partitions['LOW_BOUND'])[0:-1] + \
                         [admiss_score_partitions['UP_BOUND'][-2] + 1. / 10 ** 12]
    #
    #    ⑵ Associate each value of the RAW_SCORE attribute with partions based it position with respect to the
    #       partiion boundaries. First, ensure that RAW_SCORE is numeric.
    long_evid_dataframe['partition'] = pd.cut(x=long_evid_dataframe['RAW_SCORE'].astype(float),
                                              bins=partion_boundaries,
                                              right=False,
                                              include_lowest=True).astype(str).tolist()
    #
    #     ⑶ Construct a dataframe of partions
    mastery_level_partitions = pd.DataFrame(
        {'partition': pd.cut(x=[cut + 1. / 10 ** 12 for cut in partion_boundaries[:-1]],
                             bins=partion_boundaries,
                             right=False,
                             include_lowest=True).astype(str).tolist(),
         'MASTERY_LEVEL_CAT': list(admiss_score_partitions['MASTERY_LEVEL_CAT'])[:-1]})
    #
    #     ⑷ Left=outer join the partitions onto the evidntiary dataframe to introduce the onto the long evidentiary dataframe, dropping the
    #        join variable partition.  Assign "NUMEASURED" to null-valued MASTERY_LEVEL_CAT instances.
    long_evid_dataframe = pd.merge(left=long_evid_dataframe,
                                   right=mastery_level_partitions,
                                   how='left').drop(labels='partition',
                                                    axis=1,
                                                    inplace=False)
    long_evid_dataframe.loc[pd.isnull(long_evid_dataframe['MASTERY_LEVEL_CAT']), 'MASTERY_LEVEL_CAT'] = 'UNMEASURED'
    #
    #     ⑸ Return the result.
    return long_evid_dataframe


#
#
# |∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕
# ⓤ TRANSLATE THE LONG EVIDENCE OF LEARNING TABLE INTO A WIDE-FORMAT TABLE OF EVIDENTIARY STATES.  Our evidentiary input
#    conmes as a long-format table entitity contaiing the following attributes:
#    ⧐ A STUDENT_ID attribute identifying a disstinct subject (aka student) of evidentiary measurement;
#    ⧐ A LEARNING_STANDARD_ID, representing a particular proficiency target subject to evidentiary
#      measurement;
#    ⧐ A WORK_PRODUCT_TITLE, indicating the instrument yielding the evidentiary measurement;
#    ⧐ A RAW_SCORE indicating the strength of measurement on a 0-100 numeric scale; and
#    ⧐ A date of measurement.
#
#    This table in this context contains evidence from a sample of a population of subjects. We seek
#    here to organize body of evidence so that we can most-efficiently make inferences about the
#    individuals in that group using a batch process.
#
#    We describe our information entities here in terms of art from the field of Probabilistic
#    Graphical Models, the mathematical bases for our inference.  Our essential information entities
#    follow.
#    ⧐ vert_list contains our body of evidence for a group of subjects.  This collection is
#      initially indescriminate, containing subjects and learning targets outside of iour scope of
#      interest.
#    ⧐ digraph_edge_list contains the progressive relationship structure of our proficiency model.
#      It represents the edge list for a directed acyclic graphic.
#    ⧐ enrolless are the subjects of interest, usually distinguished by enrollment in an
#      academic course.
#    ⧐ admiss_score_partitions contains the partitions into categories into which our
#      measurement domain is decomposed.
#
#    The logic herein follows the following major steps.
#    ⑴ "Filter" our body of evidence, retaining only the subjects and evidentiary-targets within
#       the scope of interest.  We limit our subjects to those contained in enrollees and our
#       measurement targets to the vertices in digraph_edge_list.
#    ⑵ Further "prune" our body of evidence so that we have a single evidentiary measurement
#       for each subject × learning-target tuple.  When duplicates occur, we retain the
#       most-recent, weakest measurements.
#    ⑶ Map each measurement to a knowledge-state category based the RAW_SCORE's locations
#       with respect to the partitions.
#    ⑷ Expand the long-table so that a distinct record each for each subject × proficiency-target
#       pair.  Assign "UNMEASURED" resulting null-valued proficiency-state-category attributes.
#    ⑸ Reshape the long table into a wide table, which is returned as a dataframe.
def build_wide_evid_state_table(evid_state_long, digraph_edge_list, enrollees, admiss_score_partitions):
    #    ⑴ "Filter" our body of evidence, retaining only the subjects and evidentiary-targets within
    #       the scope of interest.  Accomplish this throught two inner-join operations.
    prof_model_scope = pd.DataFrame(data=list(set(digraph_edge_list['LEARNING_STANDARD_ID']) \
                                              .union(set(set(digraph_edge_list['CONSTITUENT_LEARNING_STD_ID'])))),
                                    columns=['LEARNING_STANDARD_ID'])
    evid_state_long = pd.merge(left=pd.merge(left=evid_state_long,
                                             right=enrollees),
                               right=prof_model_scope)
    #
    #    ⑵ Further "prune" our body of evidence so that we have a single evidentiary measurement
    #       for each subject × learning-target tuple.
    #       ⒜  Group measurement dates — ASSESSMENT_DATE — by subject, proficiency-measurement target, retaining
    #           only the most-recent.  Inner-join our body of evidence vert_list by this grouping, in order to
    #           reduce the data set.
    evid_state_long['ASSESSMENT_DATE'] = pd.to_datetime(arg=evid_state_long['ASSESSMENT_DATE'],
                                                        infer_datetime_format=True)
    evid_state_long = pd.merge(left=evid_state_long,
                               right=evid_state_long[['STUDENT_ID',
                                                      'LEARNING_STANDARD_ID',
                                                      'ASSESSMENT_DATE']].groupby(by=['STUDENT_ID',
                                                                                      'LEARNING_STANDARD_ID'],
                                                                                  axis=0,
                                                                                  as_index=False)[
                                   'ASSESSMENT_DATE'].max())
    #
    #       ⒝  Perform a similar group-by operation, this time retaining the minimum raw score.  Drop duplicates upon
    #           complietion.
    evid_state_long['RAW_SCORE_int'] = evid_state_long['RAW_SCORE'].astype(float).astype(int)
    evid_state_long = pd.merge(left=evid_state_long,
                               right=evid_state_long[['STUDENT_ID',
                                                      'LEARNING_STANDARD_ID',
                                                      'RAW_SCORE_int']].groupby(by=['STUDENT_ID',
                                                                                    'LEARNING_STANDARD_ID'],
                                                                                axis=0,
                                                                                as_index=False)['RAW_SCORE_int'].min())
    evid_state_long = evid_state_long.drop_duplicates(subset=['STUDENT_ID', 'LEARNING_STANDARD_ID'],
                                                      keep='first')
    #
    #    ⑶ Map each measurement to a knowledge-state category based the RAW_SCORE's locations
    #       with respect to the partitions. We use here internall-defined function numeric0_100_to_cat.
    evid_state_long = numeric0_100_to_cat(long_evid_dataframe=evid_state_long,
                                          admiss_score_partitions=admiss_score_partitions)
    #
    #    ⑷ Expand the long-table so that a distinct record each for each subject × proficiency-target
    #       pair.  Assign "UNMEASURED" resulting null-valued proficiency-state-category attributes.  Use
    #       itertools.product to construct a cartesion product of STUDENT_ID from enrollees and
    #       LEARNING_STANDARD_IDs from vert_list, onto which we join vert_list.
    evid_state_long = pd.merge(
        left=pd.DataFrame(data=list(it.product(list(set(prof_model_scope['LEARNING_STANDARD_ID'])),
                                               list(set(enrollees['STUDENT_ID'])))),
                          columns=['LEARNING_STANDARD_ID', 'STUDENT_ID']),
        right=evid_state_long,
        how='left')
    evid_state_long.loc[pd.isnull(evid_state_long['MASTERY_LEVEL_CAT']), 'MASTERY_LEVEL_CAT'] = 'UNMEASURED'
    #
    #    ⑸ Reshape the long table into a wide table of exhaustive evidentiary states for each subject. This dataframe
    #       represents our returned value.
    group_evid_state = evid_state_long[['STUDENT_ID',
                                        'LEARNING_STANDARD_ID',
                                        'MASTERY_LEVEL_CAT']].pivot(index='STUDENT_ID',
                                                                    columns='LEARNING_STANDARD_ID',
                                                                    values='MASTERY_LEVEL_CAT')
    return group_evid_state


#
# |∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕
# ⓨ CALCULATE KNOWLEDGE STATE FOR  SINGLE EVIDENTIARY STATE.
def evaluate_single_evid_state(vert_list, digraph_edge_list, var_states, analysis_case_parameters):
    wide_evid_dataframe = build_wide_evid_state_table(vert_list=vert_list.rename(columns={'SIH_PERSONPK_ID_ST': 'STUDENT_ID','EVIDENCE_OF_LEARNING_SID': 'WORK_PRODUCT_TITLE'}),
                                                      digraph_edge_list=digraph_edge_list,
                                                      enrollees=vert_list[['SIH_PERSONPK_ID_ST']] \
                                                      .drop_duplicates().rename(columns={'SIH_PERSONPK_ID_ST':'STUDENT_ID'}),
                                                      admiss_score_partitions=var_states)
    group_evid_prof = groupby_evid_profile(wide_evid_dataframe=wide_evid_dataframe)
    group_evid_prof = evaluate_group_know_state(evid_prof_dict=group_evid_prof,
                                                wide_evid_dataframe=wide_evid_dataframe,
                                                digraph_edge_list=digraph_edge_list,
                                                var_states=var_states)
    know_state_estimate = assemble_knowledge_state_table(evid_prof_dict=group_evid_prof,
                                                         vert_list=vert_list.rename(columns={'SIH_PERSONPK_ID_ST': 'STUDENT_ID','EVIDENCE_OF_LEARNING_SID': 'WORK_PRODUCT_TITLE'}),
                                                         digraph_edge_list=digraph_edge_list,
                                                         admiss_score_partitions=var_states,
                                                         analysis_case_parameters=analysis_case_parameters) \
        .rename(columns={'STUDENT_ID': 'SIH_PERSONPK_ID_ST','WORK_PRODUCT_TITLE': 'EVIDENCE_OF_LEARNING_SID'})
    know_state_estimate = know_state_estimate.loc[know_state_estimate['KNOWLEDGE_LVL_TYPE'] == 'ESTIMATED']
    return know_state_estimate


#
def onboard_initialize_student_know_state(vert_list, digraph_edge_list, var_states, analysis_case_parameters):
    wide_evid_dataframe = build_wide_evid_state_table(evid_state_long=vert_list.rename(columns={'SIH_PERSONPK_ID_ST': 'STUDENT_ID','EVIDENCE_OF_LEARNING_SID': 'WORK_PRODUCT_TITLE'}),
                                                      digraph_edge_list=digraph_edge_list,
                                                      enrollees=vert_list[['SIH_PERSONPK_ID_ST']] \
                                                      .drop_duplicates().rename(columns={'SIH_PERSONPK_ID_ST':'STUDENT_ID'}),
                                                      admiss_score_partitions=var_states)

    course_know_state = evaluate_group_know_state(group_evid_state=wide_evid_dataframe,
                                                  digraph_edge_list=digraph_edge_list,
                                                  var_states=var_states)
    course_know_state.update(
        {'BAYESNET_QUERY_RESP': assemble_knowledge_state_table(know_state=course_know_state.get('BAYESNET_QUERY_RESP'),
                                                               vert_list=vert_list.rename(columns={'SIH_PERSONPK_ID_ST': 'STUDENT_ID','EVIDENCE_OF_LEARNING_SID': 'WORK_PRODUCT_TITLE'}),
                                                               digraph_edge_list=digraph_edge_list,
                                                               admiss_score_partitions=var_states,
                                                               analysis_case_parameters=analysis_case_parameters) \
        .rename(columns={'STUDENT_ID': 'SIH_PERSONPK_ID_ST','WORK_PRODUCT_TITLE': 'EVIDENCE_OF_LEARNING_SID'})})

    return course_know_state



# |∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕
# ⓧ DECOMPOSE GRAPH INTO SUBGRAPHS.  We must manage computational complexity associated with large graphs. We decompose them
#    into subgraphs. Some subgraphs are connected components that are not connected to other vertices in the graph.
#    Others are overlapping subgraphs wihtin large, complex connected components. We get these overlapping subgraphs
#    from two appraoches:
#    ⧐ Ego-one subgraphs centered on high-valence verttices (vertices whose degrees exceed a specific threshold); and
#    ⧐ Connected-component subgraphs from the composite graph once the the high-valence vertices are removed.
#    Our graphs are "overlapping" in the sesne that different subgraphs can share adjacent vertices. The overlaps
#    occur when evidence is applied to a vertex that is outside the span of the subgraph but immediately adjacent to
#    a boundary vertex.
#
#    Our function inputs include:
#    ⧐ composite_digraph is a networkx 2.0 digraph object.  All vertices are within two graphical
#      steps from one of the measured vertices, for which evidence is available. We know this because
#      composite_digraph is constructed using our internally-defined build_induced_inrange_graph
#      funtion.
#    ⧐ measured_verts contains all of the vertices for which evidentiary measurements are available.
#
#    Our logic follows the major steps.
#    🄰 Identify the high-valency vertices — degree ≥ 6 — in composite_digraph.
#    🄱 Create a "clean copy" — composite_digraph_reduced —our baseline composite digraph. Remove
#       these vertices from composite_digraph_reduced.
#    🄲 Decompose composite_digraph_reduced into connected components.  Introduce them into a dictionary.
#    🄳 Add ego-one subgraph from composite_digraph centered on each of the high-valency vertices.
#    🄴 Extend each subgraph in voronoi_subgraph_verts to include any measured vertex within
#       unit graphical distance from the boundary vertices. Our bounary vertex set is defined
#       as {boundary_verts} = {root_verts} ∪ {leaf_verts}.  Accomplish this using another internally-
#       defined subtroutine for each item in voronoi_subgraph_verts.  We add our distance-one-from-subgraph-
#       boundary measured vertices to the vertex list for each item in voronoi_subgraph_verts.
#    🄵 Add to each item in vornoi_subgraph_verts a networkx digraph object representing the
#       induced subgraph from composite_digraph specified by its correspodning vertex list.
#    This voronoi_subgraph_verts dictionary object is our returned variable.
#
def decompose_digraph(composite_digraph, measured_verts):
    #    🄰 Identify the high-valency vertices — degree ≥ 6 — in composite_digraph.
    comp_digraph_vert_degrees = dict(composite_digraph.degree())
    high_degree_verts = list(dict((key, val) for key, val in comp_digraph_vert_degrees.items()
                                  if comp_digraph_vert_degrees.get(key) >= 6).keys())
    #    🄱 Create a "clean copy" — composite_digraph_reduced —our baseline composite digraph. Remove
    #       high-degree vertices
    composite_digraph_reduced = nx.DiGraph()
    composite_digraph_reduced.add_nodes_from(list(composite_digraph.nodes()))
    composite_digraph_reduced.add_edges_from(list(composite_digraph.edges()))
    composite_digraph_reduced.remove_nodes_from(high_degree_verts)
    #
    #    🄲 Decompose composite_digraph_reduced into connected-component subgraphs.  Store the result in
    #       a dictionary.  We have to do this in two steps.
    #       ⑴ Convert composite_digraph_reduced to an undirected graph and use networkx' connected_components
    #          function to get a vertex sets for each connected-component subgraph.  Some such subgraphs
    #          will be singleton vertices.
    subgraph_vert_sets = list(nx.connected_components(composite_digraph_reduced.to_undirected()))
    #
    #       ⑵ Consructed a dictionary of induced subgraphs of composite_digraph_reduced from all of
    #          the vertex-set lists in subgraph_vert_sets.  The behavoir of the networkx DiGraph.subgraph
    #          function is a bit messy here when the subgraph is a singleton vertex.  We apparently cannot
    #          construct a single-node nx.DiGraph object. So under such cases we just capture the vertices.
    digraph_subgraphs = {}
    for subgraph_idx in list(range(len(subgraph_vert_sets))):
        subgraph_vert_sets_subgraph_idx = subgraph_vert_sets[subgraph_idx]
        if len(subgraph_vert_sets_subgraph_idx) == 1:
            digraph_subgraphs['SUBGRAPH_' + str(subgraph_idx)] = {
                'INDUCED_SUBGRAPH_VERTS': subgraph_vert_sets_subgraph_idx,
                'MEAS_INDUCED_SUBGRAPH_VERTS': set(measured_verts) \
                    .intersection(set(subgraph_vert_sets_subgraph_idx)),
                'IS_STAR_GRAPH': False}
        else:
            digraph_subgraph_subgraph_idx = composite_digraph.subgraph(subgraph_vert_sets[subgraph_idx])
            digraph_subgraphs['SUBGRAPH_' + str(subgraph_idx)] = {
                'INDUCED_SUBGRAPH': digraph_subgraph_subgraph_idx,
                'INDUCED_SUBGRAPH_VERTS': list(digraph_subgraph_subgraph_idx.nodes()),
                'MEAS_INDUCED_SUBGRAPH_VERTS': set(measured_verts) \
                    .intersection(set(digraph_subgraph_subgraph_idx.nodes())),
                'IS_STAR_GRAPH': False}
            #
            #    🄴 Make first pass at dealing with degenrate cases.
            #       ⑴ Discard singleton graphs.
    digraph_subgraphs = dict((key, value) for key, value in digraph_subgraphs.items()
                             if len(digraph_subgraphs.get(key).get('INDUCED_SUBGRAPH_VERTS')) > 1)
    #
    #    🄵 Update each item in digraph_subgraphs with a measurement-extended spanning subgraph.  Use internally-
    #       defined function meas_extend_subgraph to get the update item.
    for key_idx in list(digraph_subgraphs.keys()):
        digraph_subgraphs.get(key_idx).update(measurement_extend_subgraph(composite_digraph=composite_digraph,
                                                                          subgraph_dict_item=digraph_subgraphs.get(key_idx),
                                                                          measured_verts=measured_verts))
    #
    #    🄳 Add ego-one subgraph from composite_digraph centered on each of the high-valency vertices.
    for vert_idx in high_degree_verts:
        high_degree_ego_graph_vert_idx = nx.ego_graph(G=composite_digraph,
                                                      n=vert_idx,
                                                      radius=1,
                                                      undirected=True)
        if len(nx.ancestors(G=high_degree_ego_graph_vert_idx,
                            source=vert_idx)) <= 9:  # ⬅⬅️Restore after development
            digraph_subgraphs['SUBGRAPH_' + vert_idx] = {'SPANNING_SUBGRAPH': high_degree_ego_graph_vert_idx,
                                                         'SPANNING_SUBGRAPH_VERTS': list(
                                                             high_degree_ego_graph_vert_idx.nodes()),
                                                         'INDUCED_SUBGRAPH': high_degree_ego_graph_vert_idx,
                                                         'INDUCED_SUBGRAPH_VERTS': list(
                                                             high_degree_ego_graph_vert_idx.nodes()),
                                                         'MEAS_INDUCED_SUBGRAPH_VERTS': set(measured_verts) \
                                                             .intersection(set(high_degree_ego_graph_vert_idx.nodes())),
                                                         'MEAS_SPANNING_SUBGRAPH_VERTS': set(measured_verts) \
                                                             .intersection(set(high_degree_ego_graph_vert_idx.nodes())),
                                                         'IS_STAR_GRAPH': True}
    #
    #    🄴 Second Pass at Degenrate Cases:
    #       ⑵ Discard graphs that:
    #          ⅰ. Lack any measured vertices.
    digraph_subgraphs = dict((key, value) for key, value in digraph_subgraphs.items()
                             if len(digraph_subgraphs.get(key).get('MEAS_SPANNING_SUBGRAPH_VERTS')) > 0)
    #
    #          ⅱ. Are not acyclic.
    digraph_subgraphs = dict((key, value) for key, value in digraph_subgraphs.items()
                             if nx.is_directed_acyclic_graph(G=digraph_subgraphs.get(key).get('SPANNING_SUBGRAPH')))
    #
    #       ⑶ Flag as non-complex graphs those for which the order is below a threshold.
    for key_idx in list(digraph_subgraphs.keys()):
        digraph_subgraphs.get(key_idx).update(
            {'NOT_COMPLEX_GRAPH': True if len(digraph_subgraphs.get(key_idx).get('SPANNING_SUBGRAPH_VERTS')) <= 12
            else False})
    #
    # 🄵 Look for pairwise near-isomorphic graphs in our digraph_subgraph list.  If cardinality of symmetric difference of edge
    #    sets is less than two, compose — union edges, vertices — into a single graph and drop one of the pairs.
    near_isomorphs = [graph_pair            ## graph_pair = list(it.combinations(digraph_subgraphs.keys(),2))[0]
                        for graph_pair in list(it.combinations(digraph_subgraphs.keys(),2))
                        if len(set(digraph_subgraphs.get(graph_pair[0]).get('SPANNING_SUBGRAPH').edges())\
                                .symmetric_difference(set(digraph_subgraphs.get(graph_pair[1]).get('SPANNING_SUBGRAPH').edges()) ) ) <= 2]
    for graph_pair in near_isomorphs:           ## graph_pair = near_isomorphs[0]
        digraph_subgraphs.get(graph_pair[0]).update({'SPANNING_SUBGRAPH' : nx.compose(G = digraph_subgraphs.get(graph_pair[0]).get('SPANNING_SUBGRAPH'),
                                                                                    H = digraph_subgraphs.get(graph_pair[1]).get('SPANNING_SUBGRAPH'))})
        del digraph_subgraphs[graph_pair[1]]
    #
    return digraph_subgraphs
#
#
# |∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕
# ⓩ EXTEND GRAPHICAL SPAN OF AN INDUCED SUBGRAPH BY ADDING MEASURED VERTICES WIITHIN ONE GRAPHICAL STEP OF BOUNDARY VERTICES.
#    We require three input objects.
#    ⧐ composite_digraph is the original subgraph object from which the our induced subgraph of interest is derived;
#    ⧐ subgraph_dict_item is a dictionary item with key 'INDUCED_SUBGRAPH' for which we seek an extended span; and
#    ⧐ measured_verts are the vertices from composite_digraph for which evidentiary measurements are available.
#
#    Return a dictionary item for which the key is 'SPANNING_SUBGRAPH' and the value is the networkx digraph
#    object with measurement=-extended span.
#
#    The major steps in our logic
#    🄰 Create spanning_subgraph, a fresh-copy of our graph object on which we can operate without modifying the original.
#    🄱 Identify the boundary vertices, {root vertices} ∪ {edge vertices} resulting from internally-defined
#       function graph_vert_class.
#    🄲 Compose with our induced_subgraph item a radius-one ego subgraph for each boundary vertex. What results is an
#       extended, induced subgraph.
#    🄳 Update our boundary vertices list for the measurement-extended induced subgraph.
#    🄴 Remove from our measurement-extended subgraph boundary vertices for which evidentiary measurements
#       are not indicated in meas_verts.
#    🄵 Construct and return the resulting dictionary item by associating with it the 'SPANNING_SUBGRAPH' key.
#
def measurement_extend_subgraph(composite_digraph, subgraph_dict_item, measured_verts):
    #    Start with a local fresh copy of composite_digraph.

    #
    #    🄰 Create spanning_subgraph, a fresh-copy of our graph object on which we can operate without modifying the original.
    meas_extended_spanning_subgraph = nx.DiGraph()
    meas_extended_spanning_subgraph.add_nodes_from(list(subgraph_dict_item.get('INDUCED_SUBGRAPH').nodes()))
    meas_extended_spanning_subgraph.add_edges_from(list(subgraph_dict_item.get('INDUCED_SUBGRAPH').edges()))
    #
    #    🄱 Identify the boundary vertices. We handle this conditionally.  Some of our meas_extended_spanning_subgraph
    #       are singleton-vertex graphs, for which our sole vertex is the boundary vertex.  Otherwise our boundary
    #       vertex is identified by the union of the root vertices and the leaf vertices resulting from
    #       internally-defined function graph_vert_class.
    induced_subgraph_vert_classes = graph_vert_class(
        edge_list=pd.DataFrame(data=list(map(list, meas_extended_spanning_subgraph.edges())),
                               columns=['CONSTITUENT_LEARNING_STD_ID',
                                        'LEARNING_STANDARD_ID']),
        vert_list=list(meas_extended_spanning_subgraph.nodes()))
    induced_subgraph_boundary_verts = list(set(induced_subgraph_vert_classes.get('LEAF_VERTS')) \
                                           .union(set(induced_subgraph_vert_classes.get('ROOT_VERTS'))))
    #
    #    🄲 Compose with our induced_subgraph item a radius-one ego subgraph for each boundary vertex.
    for vert_idx in induced_subgraph_boundary_verts:
        meas_extended_spanning_subgraph = nx.compose(G=meas_extended_spanning_subgraph,
                                                     H=nx.ego_graph(G=composite_digraph,
                                                                    n=vert_idx,
                                                                    radius=1,
                                                                    undirected=True))
    #
    #    🄳 Update our boundary vertices list for the measurement-extended induced subgraph.  Exclude from the boundary vertices
    #       those already in induced_subgraph_boundary_verts.  We subsequently drop unmeasured boundary vertices and
    #       only want to eliminate those not in our original subgraph.
    spanning_subgraph_vert_classes = graph_vert_class(
        edge_list=pd.DataFrame(data=list(map(list, meas_extended_spanning_subgraph.edges())),
                               columns=['CONSTITUENT_LEARNING_STD_ID',
                                        'LEARNING_STANDARD_ID']),
        vert_list=list(meas_extended_spanning_subgraph.nodes()))
    spanning_subgraph_boundary_verts = list(set(spanning_subgraph_vert_classes.get('LEAF_VERTS')) \
                                            .union(set(spanning_subgraph_vert_classes.get('ROOT_VERTS'))) -
                                            set(induced_subgraph_boundary_verts))
    #
    #    🄴 Identified the unmeasured boundary vertices. Remove them if the diameter of spanning_subgraph_boundary_verts
    #       exceeds four.
    unmeasured_boundary_verts = list(set(spanning_subgraph_boundary_verts) -
                                     set(measured_verts))
    if (nx.diameter(meas_extended_spanning_subgraph.to_undirected()) >= 4):
        meas_extended_spanning_subgraph.remove_nodes_from(unmeasured_boundary_verts)
    #
    #    🄵 Construct and return our return-value dictionary item.
    return {'SPANNING_SUBGRAPH': meas_extended_spanning_subgraph,
            'MEAS_SPANNING_SUBGRAPH_VERTS': list(set(measured_verts) \
                                                 .intersection(set(meas_extended_spanning_subgraph.nodes()))),
            'SPANNING_SUBGRAPH_VERTS': list(meas_extended_spanning_subgraph.nodes())}


#
#
##################⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆ LOCAL FUNCTIONS ⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆#############
# |∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—|∕—\|#
#################################################################################################################################
#
#
#
#
db_credential_dir = '/Users/nahamlet@us.ibm.com/Documents/Documents/Oportunidades actuales'
work_dir = '/Users/nahamlet@us.ibm.com/Box Sync/Analytics-Mastery/R2_5_dev'
os.chdir(work_dir)



session_attributes = pd.read_csv(filepath_or_buffer = os.path.abspath(os.path.join(work_dir, 
                                                                                'SESSION_ATTRIBUTES.csv')),
                                    dtype = str)\
                        .set_index(keys = 'ATTRIBUTE',
                                    drop = True)['VALUE']\
                        .to_dict()
tenant_config_dir_args = pd.read_csv(filepath_or_buffer = os.path.abspath(os.path.join(db_credential_dir, 
                                                                                'TENANT_CONFIG_QUERY_SPECS.csv')),
                                    dtype = str)\
                                    .set_index(keys = 'ATTRIBUTE',
                                                drop = True)\
                                    .to_dict(orient = 'dict').get('VALUE')
tenant_config = pd.DataFrame(data = list(create_engine(URL(**tenant_config_dir_args)).execute(text(' '.join([sql_line 
                                                                                for sql_line in open(file = os.path.abspath(os.path.join(work_dir, 
                                                                                                    'TENANT_CONFIG.sql')) )]))) ),
                            columns = ['host',
                                        'port',
                                        'database',
                                        'username',
                                        'password',
                                        'TENANT_ID'])\
                        .assign(drivername = 'db2+ibm_db')\
                        .set_index(keys = 'TENANT_ID',
                                    drop = False)\
                        .loc[session_attributes.get('TENANT_ID')]\
                        .drop(labels = ['TENANT_ID'])\
                        .to_dict()


course_enroll = pd.DataFrame(data = list(create_engine(URL(**tenant_config)).execute(text(' '.join([sql_line 
                                                                                for sql_line in open(file = os.path.abspath(os.path.join(work_dir, 
                                                                                                    'COURSE_ENROLL.sql')) )])),
                                                                                 sih_coursepk_id = int(session_attributes.get('SIH_COURSEPK_ID')),
                                                                                 tenant_id = session_attributes.get('TENANT_ID'))),
                                    columns = ['CAMPUS',
                                            'COURSE_ID',
                                            'COURSE_TITLE',
                                            'COURSE_SECTION_TITLE',
                                            'TEACHER_NAME',
                                            'STUDENT_ID',
                                            'STUDENT_NAME',
                                            'SIH_COURSEPK_ID',
                                            'SIH_PERSONPK_ID'])\
                    .astype(str)\
                    .set_index(keys = 'STUDENT_NAME',
                               drop = False)

mastery_color = pd.DataFrame(data = list(create_engine(URL(**tenant_config)).execute(text(' '.join([sql_line 
                                                                                for sql_line in open(file = os.path.abspath(os.path.join(work_dir, 
                                                                                                    'MASTERY_COLOR_LIST.sql')) )])),
                                                                                 tenant_id = session_attributes.get('TENANT_ID'))),
                                    columns = ['MASTERY_LEVEL_CAT',
                                            'COLOR_CD',
                                            'LOW_BOUND',
                                            'UP_BOUND'])\
                .sort_values(by = ['LOW_BOUND'])\
                .append(other = pd.DataFrame(data = {'MASTERY_LEVEL_CAT' : ['UNMEASURED'],
                                                    'COLOR_CD' : ['gray'],
                                                    'LOW_BOUND' : [np.nan],
                                                    'UP_BOUND' : [np.nan]} ))
mastery_color = mastery_color.assign(CAT_LEVEL_IDX = range(len(mastery_color)))\
                    .set_index(keys = 'MASTERY_LEVEL_CAT',
                               drop = False)




course_blueprint = pd.DataFrame(data = list(create_engine(URL(**tenant_config)).execute(text(' '.join([sql_line 
                                                                                for sql_line in open(file = os.path.abspath(os.path.join(work_dir, 
                                                                                                    'COURSE_BLUEPRINT.sql')) )])),
                                                                                 sih_coursepk_id = int(session_attributes.get('SIH_COURSEPK_ID')),
                                                                                 tenant_id = session_attributes.get('TENANT_ID')  )   ),
                             columns = ['CAMPUS',
                                        'COURSE_ID',
                                        'COURSE_TITLE',
                                        'SUBJECT_TITLE',
                                        'LEARNING_STANDARD_CD',
                                        'LEARNING_STANDARD_ID',
                                        'STANDARD_IN_PROGRESSION',
                                        'LEARNING_STANDARD_TITLE',
                                        'STANDARD_CONTENT_ID',
                                        'SIH_COURSEPK_ID']).astype(str)



lrn_std_prog = pd.DataFrame(data = list(create_engine(URL(**tenant_config)).execute(text(' '.join([sql_line 
                                                                                for sql_line in open(file = os.path.abspath(os.path.join(work_dir, 
                                                                                                    'LEARNING_STANDARD_PROGRESSION.sql')) )])),
                                                                                 tenant_id = session_attributes.get('TENANT_ID')      )),
                             columns = ['CONSTITUENT_LEARNING_STD_CD',
                                        'LEARNING_STANDARD_CD',
                                        'CONSTITUENT_LEARNING_STD_ID',
                                        'LEARNING_STANDARD_ID',
                                        'SUBJECT_TITLE']).astype(str)

curric_map = nx.DiGraph()
curric_map.add_edges_from(lrn_std_prog[['CONSTITUENT_LEARNING_STD_ID',
                                      'LEARNING_STANDARD_ID']].to_records(index = False).tolist())




cent_vert = list(set(course_blueprint['LEARNING_STANDARD_ID']))[0]


course_map_edge = pd.DataFrame(data = list(set().union(*[set(nx.ego_graph(G = curric_map.reverse(),
                                                                        n = cent_vert,
                                                                        radius = 2,
                                                                        undirected = False).reverse().edges())\
                                                        .union(nx.ego_graph(G = curric_map,
                                                                            n = cent_vert,
                                                                            radius = 1,
                                                                            undirected = False).edges())
                                                        for cent_vert in list(set(course_blueprint['LEARNING_STANDARD_ID'])\
                                                                            .intersection(curric_map.nodes()))]     )),
                                columns = ['CONSTITUENT_LEARNING_STD_ID',
                                            'LEARNING_STANDARD_ID'])
course_map = nx.DiGraph()
course_map.add_edges_from(course_map_edge.to_records(index  = False))


CPT_LIST = pd.DataFrame(data = list(create_engine(URL(**tenant_config)).execute(text(' '.join([sql_line 
                                                                                for sql_line in open(file = os.path.abspath(os.path.join(work_dir, 
                                                                                                    'CPT_LIST.sql')) )])),
                                                                                 tenant_id = session_attributes.get('TENANT_ID'),
                                                                                 max_in_val = max(course_map.in_degree().values()))),
                                    columns = ['CONSTITUENT_COUNT',
                                                'CPT_CELL_IDX',
                                                'MEAS',
                                                'IS_ROOT'])
CPT_LIST = CPT_LIST.assign(CONSTITUENT_COUNT = list(map(str, CPT_LIST['CONSTITUENT_COUNT'])))\
                    .assign(IS_ROOT = list(map(str, CPT_LIST['IS_ROOT'])))


lrn_std_id_cd = pd.merge(left = pd.DataFrame(data = list(create_engine(URL(**tenant_config)).execute(text(' '.join([sql_line 
                                                                                for sql_line in open(file = os.path.abspath(os.path.join(work_dir, 
                                                                                                    'LRN_STD_ID_CD.sql')) )])),
                                                                                 sih_coursepk_id = int(session_attributes.get('SIH_COURSEPK_ID')),
                                                                                 tenant_id = session_attributes.get('TENANT_ID')      )),
                                    columns = ['SUBJECT_TITLE',
                            'LEARNING_STANDARD_ID',
                                                'LEARNING_STANDARD_CD']).astype(str),
            right = pd.DataFrame(data = {'LEARNING_STANDARD_ID' : list(course_map.nodes())} ) )\
                .set_index(keys = 'LEARNING_STANDARD_ID',
                       drop = False)

vertex_list = pd.DataFrame(data = list(create_engine(URL(**tenant_config)).execute(text(' '.join([sql_line 
                                                                                for sql_line in open(file = os.path.abspath(os.path.join(work_dir, 
                                                                                                    'WTD_BODY_OF_EVID.sql')) )])),
                                                                                 sih_coursepk_id = int(session_attributes.get('SIH_COURSEPK_ID'))  )   ),
                            columns = ['SIH_PERSONPK_ID_ST',
                                        'LEARNING_STANDARD_ID',
                                        'LEARNING_STANDARD_CD',
                                        'RAW_SCORE',
                                        'ASSESSMENT_DATE',
                                        'EVIDENCE_OF_LEARNING_SID',
                                        'SUBJECT_TITLE',])


