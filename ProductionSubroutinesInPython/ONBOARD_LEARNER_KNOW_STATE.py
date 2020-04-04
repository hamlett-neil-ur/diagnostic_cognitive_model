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
# import itertools as it
# import seaborn
# import functools as fct
# import matplotlib.pyplot as plt
# import pygraphviz as pgv
import time
import csv
import timeit as tit
from datetime import datetime
import pandas as pd
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
import multiprocessing
import subprocess
import os
from pgmpy.models import BayesianModel
from pgmpy.factors.discrete.CPD import TabularCPD
from pgmpy.inference import VariableElimination
from collections import Counter
from logging.handlers import TimedRotatingFileHandler


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
        CPT_LIST.loc[CPT_LIST['CONSTITUENT_COUNT'] == str(int(cond_dependence.get(vert_idx).get('CONSTITUENT_COUNT')))][
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
#
# |∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕
# ⓖ QUERY STAR-GRAPH BAYESIAN NETWORK.  We experience slow numerical convergence when the directed-acyclic graph (DAG)
#    underlying our Bayesian network conforms to a star-graph (https://en.wikipedia.org/wiki/Star_(graph_theory)) or
#    quasi-star-graph configuration. We believe this occurs because unmeasured, unit-valency boundary vertices
#    are probabilistically independent.  The numerical algorithm performs large numbers of computational iterations
#    looking for non-existent covariations.
#
#    Fortunately the unit-valency boundary vertices in a star graph are also independently distributed. We therefore need
#    only calculate once the marginalized CPDs for each unmeasured, unit-valency root and leaf vertex in our star graph
#    or quasi-star graph.  We can reapply — after Bayesian-network query — the resulting marginalized CPDs from
#    a single unmeasured, unit-valency, root or leaf vertices to all others. Removing such redundant vertices prior to
#    invoking our Bayesian-network query removes the condition resulting in protractive iterative searches for
#    non-existent dependencies.
#
#    This subroutine represents an edge-condition branch from the approx_infer_group_know_state, exact_infer_group_know_state
#    subroutine-logic  threads. We check within the est_know_state_for_evid_prof subroutine whether our Baysenet Digraph
#    has a "IS_STAR_GRAPH" flag set as "True".  If so, this subroutine gets invoked. Otherwise, the algorithm directly
#    invokes approx_infer_group_know_state.
#
#    This logic branch performs some pre- and post-processing around invocation of the approx_infer_group_know_state
#    subroutine.  Its input and outputs are identical.  Its inputs include:
#    ⧐ A SUBGRAPH dictionary object containing a digraph on which to base a Bayesian network;
#    ⧐ A dataframe containing subject (aka "student") evidentiary states for variables contained within
#      the digraph object;
#    ⧐ A list of admissible variable states; and
#    ⧐ A clust_idx label for the subgraph used to associate execution-time statistics with the configuration
#      of the queried Bayesian network.
#
#    Our output consists of a two-item dictionary object:
#    ⧐ A dataframe reporting the knowledge — in terms of marginalized, conditional probabilities —
#      for all subjects, learning targets (aka, students, learning standards) for each
#      admissible evidentiary state;
#    ⧐ A dataframe reporting execution-time statistics for each query of the Bayesian network.
#
#    The subroutine logic steps follow.
#    ⑴ Create a copy of our Bayesian-network DAG.
#    ⑵ Identify within our star graph the unmeasured, unit-valency root and leaf vertices.
#    ⑶ Remove from our Bayesian-network DAG redundant unit-valency, unmeasured root and leaf vertices.
#    ⑷ Invoke approx_infer_group_know_state to query the Bayesian network for each evidentiary state.
#    ⑸ Append onto our BAYESNET_QUERY_RESP dataframe output marginalized CPDs associated with the
#       redundant boundary vertices removed in step ⑶ above.
#
def query_star_graph_Bayesnet(bayesnet_digraph, wide_evid_dataframe, evid_prof_conformees, var_states, clust_idx):
    #    First, define the graph. Bayesian-network
    #    ⑴ Create a copy of our Bayesian-network DAG.  Extract an edge list as a dataframe. We subsequently require this
    #       to classify vertices as roots, leafs using internally-defined graph_vert_class. Function.
    red_star_graph = nx.DiGraph()
    red_star_graph.add_edges_from(list(bayesnet_digraph.edges()))
    red_star_graph_edges = pd.DataFrame(data=list(bayesnet_digraph.edges()),
                                        columns=['CONSTITUENT_LEARNING_STD_ID',
                                                 'LEARNING_STANDARD_ID'])
    #
    #    ⑵ Identify within our star graph the unmeasured, unit-valency root and leaf vertices.   This occurs in three steps.
    #       ⒜ First calssify vertices as root, leaf vertices. Invoke internally-defined function graph_vert_class.
    red_graph_vert_classes = graph_vert_class(edge_list=red_star_graph_edges,
                                              vert_list=list(bayesnet_digraph.nodes()))
    #
    #       ⒝ Next identify the measured vertices. We unfortunately do not have direct access to this information from
    #          our given variables. We must therefore reconstruct this from the evidentiary-state variables we have,
    #          wide_evid_dataframe and evid_prof_conformees.  Our wide_evid_dataframe is an exhaustive inventory of
    #          evidentiary states for our subject sample (aka students). The evid_prof_conformees is a list of subjects
    #          possessing measurements against an identical set of variables.
    #
    #          Measured variables are those for which the value in wide_evid_dataframe has a value other than "UNMEASURED".
    #          We first produce a series of logical (True or False) states for evid_prof_conformees for whom all
    #          variables have values other than 'UNMEASURED'.  We then collect a set of indices for which
    #          the series values are True.
    meas_group_evid_prof = (wide_evid_dataframe.loc[evid_prof_conformees,
                                                    list(bayesnet_digraph.nodes())] != 'UNMEASURED').all(axis=0)
    measured_verts = (set([evid_prof_idx for evid_prof_idx in list(meas_group_evid_prof.index)
                           if meas_group_evid_prof[evid_prof_idx]])).intersection(set(bayesnet_digraph.nodes()))
    unmeasured_verts = (set([evid_prof_idx for evid_prof_idx in list(meas_group_evid_prof.index)
                             if not meas_group_evid_prof[evid_prof_idx]])).intersection(set(bayesnet_digraph.nodes()))
    #
    #       ⒞ Get a list of unit-valency verts.  Use the nx "degree" function to extract a dictionary object for which the
    #          key is the vertex label and the values contain the valencies.
    unit_valency_verts = set([vert_lab for (vert_lab, vert_val) in dict(bayesnet_digraph.degree()).items()
                              if dict(bayesnet_digraph.degree()).get(vert_lab) == 1])
    #
    #       ⒟ measured_verts is now a list of all measured variables for subjects in evid_prof_conformees.  We want to identify
    #          the unmeasured root, leaf vertices.  Retain only the intersection with the set of unit_valency_verts.
    unmeas_root_verts = list(
        (set(red_graph_vert_classes.get('ROOT_VERTS')) - measured_verts).intersection(unit_valency_verts))
    meas_root_verts = list(
        (set(red_graph_vert_classes.get('ROOT_VERTS')) - unmeasured_verts).intersection(unit_valency_verts))
    unmeas_leaf_verts = list(
        (set(red_graph_vert_classes.get('LEAF_VERTS')) - measured_verts).intersection(unit_valency_verts))
    meas_leaf_verts = list(
        (set(red_graph_vert_classes.get('LEAF_VERTS')) - unmeasured_verts).intersection(unit_valency_verts))
    #
    #       ⒠ CONDITIONAL-INDEPENDENCE VALIDITY 😱🧟‍😵. Our assumption that unmeasured root vertices is not valid if any non-root
    #          vertices contain evidence. If this is the case, coerce unmeas_root_verts to an empty list. This causes subsequent
    #          logic to identify and remove iid boundary vertices to pass over the root vertex step.
    if len(
        set(red_graph_vert_classes.get('NONROOT_VERTS')).intersection(set(measured_verts))) > 0: unmeas_leaf_verts = []
    #
    #    ⑶ Remove from our Bayesian-network DAG redundant unit-valency, unmeasured root and leaf vertices.  Our logic here is conditional.
    #       We only remove redundant, unmeasured, unit-valency leaves, roots if such exist.  We first check if the conditions exist.
    #       If they do, we perform the following actions.
    #       ⒜ Create a set-complementary dictionary for which the key is the redundant vertex to retained and the values are a list
    #          of vertices to be removed from red_star_graph for the Bayesian-network query.
    #       ⒝ Remove the redundant vertices from the red_star_graph on which our Bayesian-network query will result.
    if len(unmeas_root_verts) > 1:
        redundant_unit_valency_roots = {unmeas_root_verts[0]: unmeas_root_verts[1:]}
        red_star_graph.remove_nodes_from(list(redundant_unit_valency_roots.values())[0])
    if len(unmeas_leaf_verts) > 1:
        redundant_unit_valency_leaves = {unmeas_leaf_verts[0]: unmeas_leaf_verts[1:]}
        red_star_graph.remove_nodes_from(list(redundant_unit_valency_leaves.values())[0])
    #
    #    ⑷ Invoke approx_infer_group_know_state to query the Bayesian network for each evidentiary state.
    cluster_bayesnet_query = approx_infer_group_know_state(bayesnet_digraph=red_star_graph,
                                                           wide_evid_dataframe=wide_evid_dataframe,
                                                           evid_prof_conformees=evid_prof_conformees,
                                                           var_states=var_states,
                                                           clust_idx=clust_idx)
    #
    #    ⑸ Append onto our BAYESNET_QUERY_RESP dataframe output marginalized CPDs associated with the
    #       redundant boundary vertices removed in step ⑶ above.  This again is executed conditionally for each case.
    #       We only execute respectively if lengths of unmeas_root_verts, unmeas_leaf_verts exceed unity.
    #       ⒜ The the Bayesian-network response dataframe object out of our dictionary item returned by
    #          approx_infer_group_know_state.  The get fhe records associated the "baseline" member
    #          of the unmeasured, unit-valency root and leaf vertex sets. These are distinguished by the
    #          keys of redundant_unit_valency_roots, redundant_unit_valency_leaves.
    #
    if len(unmeas_root_verts) > 1:
        bayesnet_query_resp = cluster_bayesnet_query.get('BAYESNET_QUERY_RESP')
        baseline_root_estimates = bayesnet_query_resp.loc[bayesnet_query_resp['LEARNING_STANDARD_ID'] \
            .isin(list(redundant_unit_valency_roots.keys()))]
        appended_bayesnet_query_resp = pd.concat([bayesnet_query_resp] + \
                                                 [baseline_root_estimates.drop(labels=['LEARNING_STANDARD_ID'],
                                                                               axis=1).assign(
                                                     LEARNING_STANDARD_ID=redundant_vert)
                                                  for redundant_vert in
                                                  redundant_unit_valency_roots.get(unmeas_root_verts[0])])
        cluster_bayesnet_query.update({'BAYESNET_QUERY_RESP': appended_bayesnet_query_resp})
    if len(unmeas_leaf_verts) > 1:
        bayesnet_query_resp = cluster_bayesnet_query.get('BAYESNET_QUERY_RESP')
        baseline_leaf_estimates = bayesnet_query_resp.loc[bayesnet_query_resp['LEARNING_STANDARD_ID'] \
            .isin(list(redundant_unit_valency_leaves.keys()))]
        appended_bayesnet_query_resp = pd.concat([bayesnet_query_resp] + \
                                                 [baseline_leaf_estimates.drop(labels=['LEARNING_STANDARD_ID'],
                                                                               axis=1).assign(
                                                     LEARNING_STANDARD_ID=redundant_vert)
                                                  for redundant_vert in
                                                  redundant_unit_valency_leaves.get(unmeas_leaf_verts[0])])
        cluster_bayesnet_query.update({'BAYESNET_QUERY_RESP': appended_bayesnet_query_resp})
    #
    return cluster_bayesnet_query


#
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
#
# |∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕
# ⓗ EXTRACT BAYESIAN-NETWORK QUERY RESULTS.  The pomegranate bayesian-network query returns results on a json-like format. We want
#    them in an array.
def extract_bayesnet_query(pred_proba_result, graph_node_order):
    #    First, zip the query-response result pred_proba_result into a dictionary.  Initialize a blank dataframe within which
    #    to store in a structured format.
    pred_proba_result_dict = dict(zip(graph_node_order,
                                      pred_proba_result.tolist()))
    #
    #    Loop through the elements of pred_proba_result_dict, extracting the 'parameters' object from the json object.
    #    Extract each object into a dataframe row.
    for dict_key in list(pred_proba_result_dict.keys()):  # dict_key = list(pred_proba_result_dict.keys())[0]
        pred_proba_result_dict.update({dict_key:
                                           json.loads(pred_proba_result_dict.get(dict_key).to_json())['parameters'][0]})
        pred_proba_result_dict.update({dict_key:
                                           dict((int(float(str(key))), value) for key, value in
                                                pred_proba_result_dict.get(dict_key).items())})
    #
    #   Return the result as a list of lists.
    return pd.DataFrame.from_dict(data=pred_proba_result_dict,
                                  orient='index')


#
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
#
#
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
    #	    ⒝ We now specify conditional-probabilities for each vertex in the graph.
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
    #	pgmpy_bayesnet_compiled = BeliefPropagation(pgmpy_bayesnet)
    #	pgmpy_bayesnet_compiled._calibrate_junction_tree(operation = 'marginalize')
    #	pgmpy_bayesnet_compiled.calibrate()
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
    for state_idx in evid_state_by_subj.get(
            'EVID_STATE_SIG'):  # state_idx = evid_state_by_subj.get('EVID_STATE_SIG')[0]
        print('Evidentiary state ' + str(state_idx) + ', ' + str(
            evid_state_by_subj.get('EVID_STATE_SIG').index(state_idx) + 1) + \
              ' of ' + str(
            len(evid_state_by_subj.get('EVID_STATE_SIG'))) + ' evidentiary states for subgraph at time ' + \
              str(datetime.now().time()))
        start_time_state_idx = tit.default_timer()
        #
        #        ⅰ. Query the pgmpy Bayesian network. The EVID_STATE objects in our evid_state_by_subj dictionary contain the applied
        #           evidence. We must specify the return variables. We seek marginalized conditional probabilities for all non-measured
        #           vertices.
        bayesnet_query_resp = pgmpy_bayesnet_compiled.query(variables=list(set(bayesnet_digraph.nodes()) - \
                                                                           set(evid_state_by_subj.get(state_idx).get(
                                                                               'EVID_STATE').keys())),
                                                            evidence=evid_state_by_subj.get(state_idx).get(
                                                                'EVID_STATE'))
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
                                                    str(datetime.now().time()),
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
#
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
    print(CLUST_EXEC_TIME_state_idx.T.squeeze())
    #
    #    ⑵ Now, query the Bayesian network.	  We use the Pomegranate predict_proba funciton to caluclate a set of marginalized conditional
    #       probability variables.  We apply the returned value as an argument to our locally-defined extract_bayesnet_query
    #       function in order to transform the result into a dataframe. We add the state_idx variable as an EVID_STATE_SIG value for our
    #       dataframe. We also add the vertex labels as a LEARNING_STANDARD_ID attribute.  We also have the opportunity here to
    #       distinguish between measured and estimated vertices. This attribute is stored in the KNOWLEDGE_LVL_TYPE attribute.
    bayesnet_query_resp = \
    extract_bayesnet_query(pred_proba_result=pom_bayesnet.get('Pomegranate_Bayesnet').predict_proba(evid_state),
                           graph_node_order=pom_bayesnet.get('DiGraph_Vert_Order')) \
        [var_states.iloc[:-1]['CAT_LEVEL_IDX'].tolist()]
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
            'CLUSTER_EXEC_TIME': CLUST_EXEC_TIME_state_idx}


#
#
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
    #   ≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈
    #   ⛔⛔⛔⛔⛔⛔⛔⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇ DIAGNOSTIC FOR DEVELOPMENT ONLY ⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⛔⛔⛔⛔⛔⛔⛔
    #	graph_plot_dir = '/Users/nahamlet/Box Sync/Redundancy_28thNov'
    #	nx_graph_for_plot = nx.DiGraph()
    #	nx_graph_for_plot.add_edges_from(list(bayesnet_digraph.edges()))
    #	agraph_for_plot = nx.nx_agraph.to_agraph(N = nx_graph_for_plot)
    #	agraph_for_plot.layout('dot')
    #	agraph_for_plot.draw(os.path.abspath(os.path.join(graph_plot_dir,dict_key + str(evid_prof_conformees) + '_SUSPCIOUS_GRAPH.png')))
    #   ⛔⛔⛔⛔⛔⛔⛔⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆ SHORTCUT FOR DEVELOPMENT ONLY ⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⛔⛔⛔⛔⛔⛔⛔
    #   ≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈
    cluster_bayesnet = build_pom_bayesnet(directed_graph=bayesnet_digraph,
                                          var_states=var_states,
                                          bayesnet_label=clust_idx)
    pom_baysenet_build_time = tit.default_timer() - start_time_state_idx
    #	plt.figure(figsize = (14,10))
    #	cluster_bayesnet.get('Pomegranate_Bayesnet').plot()
    #	plt.show()
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
            'EVID_STATE_SIG'):  # state_idx = evid_state_by_subj.get('EVID_STATE_SIG')[0]
        print('Evidentiary state ' + str(state_idx) + ', ' + str(
            evid_state_by_subj.get('EVID_STATE_SIG').index(state_idx) + 1) + \
              ' of ' + str(
            len(evid_state_by_subj.get('EVID_STATE_SIG'))) + ' evidentiary states for subgraph at time ' + \
              str(datetime.now().time()))
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
# ⓟ TRANSLATE EVIDENTIARY PROFILES INTO KNOWLEDGE STATES.  We seek to translate each evidence set associated
#    with subjects conforming to an evidentiary profile into a knowledge state. This subroutine controls
#    logic that:
#    ⑴ Transforms an edge list for the evidentiary neighborhood of learning standards for a
#       course blueprint into a networkx digraph obhect.
#    ⑵ Ascertains the evidentiary range — a distance-derived induced subgraph — of vertices from
#       bmeasurements indicated in the evidentiary profile;
#    ⑶ Decomposes the in-range induced subgraph into computationally-tractable and -expeditious
#       subgraphs;
#    ⑷ Declares dataframes into which we aggregate results to be returned.
#    ⑸ Invokes logic to construct knowledge-state estiamtes for each variable in each subgraph
#       and each conformee to the evidentiary profile; and
#    ⑹ Aggregates the results for subsequent processing.
#
#    Its inputs are:
#    ⧐ digraph_edge_list represennts the learning-standard graphical neighborhood based on
#      the learning standards specified by the curriculum map of an academic course;
#    ⧐ evid_prof_dict_item is a the prof_idxᵗʰ dictionary item resulting from applying the
#      groupby_evid_profile subroutine to wide_evid_dataframe;
#    ⧐ wide_evid_dataframe contains the the evidentiary states for all subjects (aka students)
#      with respect to learning standards in the digraph_edge_list; and
#    ⧐ var_states, a list of admissible variable states for all variables in the digraph.
#
#    The subroutine returns a dictionary object containing two dataframe items:
#    ⧐ The knowledge state for all subjects with respect to variables within range of
#      those measured as indicated by the evidentiary profile;
#    ⧐ An aggregation of execution-time statistics for each
def est_know_state_for_evid_prof(digraph_edge_list, evid_prof_dict_item, wide_evid_dataframe, var_states, prof_idx):
    #    ⑴ First build the digraph for from the edge list.
    course_nhbd_graph = nx.DiGraph()
    course_nhbd_graph.add_nodes_from(list(set(digraph_edge_list['LEARNING_STANDARD_ID']). \
                                          union(set(digraph_edge_list['CONSTITUENT_LEARNING_STD_ID']))))
    course_nhbd_graph.add_edges_from(
        tuple(digraph_edge_list[['CONSTITUENT_LEARNING_STD_ID', 'LEARNING_STANDARD_ID']].itertuples(index=False)))
    #
    #    ⑵ Ascertains the evidentiary range of the course evidentiary neighborhood. We apply the build_induced_inrange_graph
    #       to a networkx digraph object derived from digraph_edge_list.  This is the composition of radius-two ego subgraphs
    #       centered on the variables measured in the evidentiary profile.
    in_range_digraph = build_induced_inrange_graph(meas_list=evid_prof_dict_item.get('LEARNING_STANDARD_ID'),
                                                   evid_radius=2,
                                                   course_nhbd_graph=course_nhbd_graph)
    #
    #    ⑶ Decompose the in_range_digraph object into subgraphs. Use the decompose_digraph to produce a dictionary
    #       of graph objects and metadata.
    evid_prof_subgraphs = decompose_digraph(composite_digraph=in_range_digraph,
                                            measured_verts=evid_prof_dict_item.get('LEARNING_STANDARD_ID'),
                                            prof_idx=prof_idx)
    #
    #   ≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈
    #   ⛔⛔⛔⛔⛔⛔⛔⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇ DIAGNOSTIC FOR DEVELOPMENT ONLY ⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⛔⛔⛔⛔⛔⛔⛔
    #   ⛔⛔⛔⛔⛔⛔⛔⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆ SHORTCUT FOR DEVELOPMENT ONLY ⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⛔⛔⛔⛔⛔⛔⛔
    #   ≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈
    #
    #    ⑷ Before we loop through each subgraph applying the evidentiary states, we first need to initialize dataframes
    #       within which we aggregate results.
    bayesnet_query_resp = pd.DataFrame(
        columns=['EVID_STATE_SIG', 'STUDENT_ID', 'LEARNING_STANDARD_ID', 'KNOWLEDGE_LVL_TYPE']
                + var_states[:-1]['CAT_LEVEL_IDX'].tolist())
    evid_prof_exec_time = pd.DataFrame(columns=['GRAPH_ORDER',
                                                'EDGE_COUNT',
                                                'MEAS_VERT_COUNT',
                                                'EST_VERT_COUNT',
                                                'TIME_NOW',
                                                'ELAPSED_TIME'])
    subgraph_keys = list(evid_prof_subgraphs.keys())
    #
    #   ≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈
    #   ⛔⛔⛔⛔⛔⛔⛔⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇ SHORTCUT FOR DEVELOPMENT ONLY ⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⛔⛔⛔⛔⛔⛔⛔
    #   ⛔⛔⛔⛔⛔⛔⛔⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆ SHORTCUT FOR DEVELOPMENT ONLY ⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⛔⛔⛔⛔⛔⛔⛔
    #   ≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈
    #
    #   ⑸ Loop through the subgraphs, invoking approx_infer_group_know_state for each.
    for subgraph_idx in subgraph_keys:  ## subgraph_idx = 'SUBGRAPH_121700'
        print('Subgraph ' + str(subgraph_idx) + ', ' + str(subgraph_keys.index(subgraph_idx) + 1) + \
              ' of ' + str(len(subgraph_keys)) + ' subgraphs in evidentiary-profile estimation range at time ' + \
              str(datetime.now().time()))
        #      ⒜ Invoke approx_infer_group_know_state to get the knowledge-state estimates associated with subgraph_idxᵗʰ
        #         subgraph in  evid_prof_subgraphs.  Our logic here branches depending on whether  or not the subgraph is a
        #         star graph.
        if evid_prof_subgraphs.get(subgraph_idx).get('IS_STAR_GRAPH'):
            cluster_bayesnet_query = query_star_graph_Bayesnet(
                bayesnet_digraph=evid_prof_subgraphs.get(subgraph_idx).get('SPANNING_SUBGRAPH'),
                wide_evid_dataframe=wide_evid_dataframe,
                evid_prof_conformees=evid_prof_dict_item.get('STUDENT_ID'),
                var_states=var_states,
                clust_idx=subgraph_idx)
        else:
            cluster_bayesnet_query = approx_infer_group_know_state(
                bayesnet_digraph=evid_prof_subgraphs.get(subgraph_idx).get('SPANNING_SUBGRAPH'),
                wide_evid_dataframe=wide_evid_dataframe,
                evid_prof_conformees=evid_prof_dict_item.get('STUDENT_ID'),
                var_states=var_states,
                clust_idx=subgraph_idx)
        #
        #      ⒝ The cluster_bayesnet_query dictionary object contains two dataframe items:  BAYESNET_QUERY_RESP contains the
        #         estimated knowledge state and CLUSTER_EXEC_TIME contains exection-time satistics for each Bayesian-network
        #         query.  Extract these and concatenate onto bayesnet_query_resp and clust_exec_time, respectively.

        bayesnet_query_resp = pd.concat([bayesnet_query_resp,
                                         cluster_bayesnet_query.get('BAYESNET_QUERY_RESP')])
        evid_prof_exec_time = pd.concat([evid_prof_exec_time,
                                         cluster_bayesnet_query.get('CLUSTER_EXEC_TIME')])
    #
    #    ⑹ Aggregates the results for subsequent processing.
    #       ⒜ Now, bayesnet_query_resp can contain multiple estimates for a LEARNING_STANDARD_ID × STUDENT_ID
    #          pair. This arises because our subgraphs in evid_prof_subgraphs can overlap. We want to average
    #          the stimates.  We must do that in two stages. We want to preserve some of the attributes
    #          in bayesnet_query_resp, the KNOWLEDGE_LVL_TYPE for each pair, in particular.  Our mean-aggregation
    #          by pair instances should only apply to the marginal, conditional probabilities.  So we create an
    #          intermediate dataframe consisting of unique instances of LEARNING_STANDARD_ID, STUDENT_ID,
    #          KNOWLEDGE_LVL_TYPE.  We perform our mean-aggregation with the remaining attributes. We then join
    #          our evidentiary-profile attributes back onto the mean-aggregate result.
    if len(bayesnet_query_resp) > 0:
        evid_prof = bayesnet_query_resp[['STUDENT_ID', 'LEARNING_STANDARD_ID', 'KNOWLEDGE_LVL_TYPE']].drop_duplicates()
        evid_prof_know_state = bayesnet_query_resp[var_states[:-1]['CAT_LEVEL_IDX'].tolist() + \
                                                   ['LEARNING_STANDARD_ID', 'STUDENT_ID']]. \
            groupby(by=['LEARNING_STANDARD_ID', 'STUDENT_ID'],
                    as_index=False)[var_states[:-1]['CAT_LEVEL_IDX'].tolist()].agg(lambda x: np.mean(x))
        know_state = pd.merge(left=evid_prof,
                              right=evid_prof_know_state)
    else:
        know_state = bayesnet_query_resp
    know_state = know_state.sort_values(by=['STUDENT_ID',
                                            'LEARNING_STANDARD_ID'],
                                        axis=0)
    #
    #       ⒝ Add EVID_PROF_SIG attribute to evid_prof_exec_time.
    evid_prof_exec_time['EVID_PROF_SIG'] = prof_idx
    #
    #       ⒞ Return the result as a dictionary.
    # os.system('say "Another ones down and another ones gone and"')
    return {'KNOWLEDGE_STATE_ESTIMATE': know_state,
            'CLUSTER_EXEC_TIME': evid_prof_exec_time}


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
def assemble_knowledge_state_table(evid_prof_dict,
                                   vert_list,
                                   digraph_edge_list,
                                   admiss_score_partitions,
                                   analysis_case_parameters):
    #    ⑴ Concatenate all of the KNOWLEDGE_STATE items.  Construct a list of dictionary keys
    #       to use as control-loop index variables.  First extract the first. Then get the rest
    #       by looping through.
    evid_prof_keys = list(evid_prof_dict.keys())
    know_state = evid_prof_dict.get(evid_prof_keys[0]).get('KNOWLEDGE_STATE_ESTIMATE')
    for prof_idx in evid_prof_keys[1:]:
        know_state = pd.concat([know_state,
                                evid_prof_dict.get(prof_idx).get('KNOWLEDGE_STATE_ESTIMATE')])
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
#    a cohort of learners enrolled in an academci course to a Bayesian network.  The evidence of learning is organized
#    according to evidentiary profiles.  Evidentiary profiles are a sets of learning standards for which
#    one or many learners have accrued evidence of learning. The inputs to this function include:
#    ⧐ evid_prof_dict a dictionary of evidentiary profiles, each defined by a set of "measured" learning
#      standard and identity kesy — STUDENT_IDs — of learners fitting the profile;
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
def evaluate_group_know_state(evid_prof_dict, wide_evid_dataframe, digraph_edge_list, var_states):
    #    Begin loo;ing through the keys for evid_prof_dict, invoking est_know_state_for_evid_prof to obtain the
    #    dictionary updates.
    #   ≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈
    #   ⛔⛔⛔⛔⛔⛔⛔⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇ SHORTCUT FOR DEVELOPMENT ONLY ⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⛔⛔⛔⛔⛔⛔⛔
    #   ⛔⛔⛔⛔⛔⛔⛔⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆ SHORTCUT FOR DEVELOPMENT ONLY ⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⛔⛔⛔⛔⛔⛔⛔
    #   ≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈
    for key_idx in list(evid_prof_dict.keys()):  # [0:4]:				# key_idx = list(evid_prof_dict.keys())[0]
        print('Evidentiary profile ' + str(key_idx) + ', ' + str(list(evid_prof_dict.keys()).index(key_idx) + 1) + \
              ' of ' + str(len(evid_prof_dict)) + ' evidentiary profiles at time ' + str(datetime.now().time()))
        #
        #    Invoke est_know_state_for_evid_prof in order to get the two-item dictionary resulting from querying the
        #    Bayesian network given the evidentiary states corresponding to the profile specified by the
        #    key_idxᵗʰ member of evid_prof_dict.
        evid_prof_update_key_idx = est_know_state_for_evid_prof(digraph_edge_list=digraph_edge_list,
                                                                evid_prof_dict_item=evid_prof_dict.get(key_idx),
                                                                wide_evid_dataframe=wide_evid_dataframe,
                                                                var_states=var_states,
                                                                prof_idx=key_idx)
        # os.system('say "Here we go again!"')
        #
        #    Now update the key_idxᵗʰ element of evid_prof_dict individually with each item returned with evid_prof_update_key_idx.
        for item_idx in list(evid_prof_update_key_idx.keys()):  ## item_idx = list(evid_prof_update_key_idx.keys())[0]
            evid_prof_dict.get(key_idx).update({item_idx: evid_prof_update_key_idx.get(item_idx)})
            #
            #    Return the updated dictionary object.
    return evid_prof_dict


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
                                              #right=True,
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
def build_wide_evid_state_table(vert_list, digraph_edge_list, enrollees, admiss_score_partitions):
    #    ⑴ "Filter" our body of evidence, retaining only the subjects and evidentiary-targets within
    #       the scope of interest.  Accomplish this throught two inner-join operations.
    prof_model_scope = pd.DataFrame(data=list(set(digraph_edge_list['LEARNING_STANDARD_ID']) \
                                              .union(set(set(digraph_edge_list['CONSTITUENT_LEARNING_STD_ID'])))),
                                    columns=['LEARNING_STANDARD_ID'])
    vert_list = pd.merge(left=pd.merge(left=vert_list,
                                       right=enrollees),
                         right=prof_model_scope)
    #
    #    ⑵ Further "prune" our body of evidence so that we have a single evidentiary measurement
    #       for each subject × learning-target tuple.
    #       ⒜  Group measurement dates — ASSESSMENT_DATE — by subject, proficiency-measurement target, retaining
    #           only the most-recent.  Inner-join our body of evidence vert_list by this grouping, in order to
    #           reduce the data set.
    vert_list['ASSESSMENT_DATE'] = pd.to_datetime(arg=vert_list['ASSESSMENT_DATE'],
                                                  infer_datetime_format=True)
    vert_list = pd.merge(left=vert_list,
                         right=vert_list[['STUDENT_ID',
                                          'LEARNING_STANDARD_ID',
                                          'ASSESSMENT_DATE']].groupby(by=['STUDENT_ID',
                                                                          'LEARNING_STANDARD_ID'],
                                                                      axis=0,
                                                                      as_index=False)['ASSESSMENT_DATE'].max())
    #
    #       ⒝  Perform a similar group-by operation, this time retaining the minimum raw score.  Drop duplicates upon
    #           complietion.
    vert_list['RAW_SCORE_int'] = vert_list['RAW_SCORE'].astype(float).astype(int)
    vert_list = pd.merge(left=vert_list,
                         right=vert_list[['STUDENT_ID',
                                          'LEARNING_STANDARD_ID',
                                          'RAW_SCORE_int']].groupby(by=['STUDENT_ID',
                                                                        'LEARNING_STANDARD_ID'],
                                                                    axis=0,
                                                                    as_index=False)['RAW_SCORE_int'].min())
    vert_list = vert_list.drop_duplicates(subset=['STUDENT_ID', 'LEARNING_STANDARD_ID'],
                                          keep='first')
    #
    #    ⑶ Map each measurement to a knowledge-state category based the RAW_SCORE's locations
    #       with respect to the partitions. We use here internall-defined function numeric0_100_to_cat.
    vert_list = numeric0_100_to_cat(long_evid_dataframe=vert_list,
                                    admiss_score_partitions=admiss_score_partitions)
    #
    #    ⑷ Expand the long-table so that a distinct record each for each subject × proficiency-target
    #       pair.  Assign "UNMEASURED" resulting null-valued proficiency-state-category attributes.  Use
    #       itertools.product to construct a cartesion product of STUDENT_ID from enrollees and
    #       LEARNING_STANDARD_IDs from vert_list, onto which we join vert_list.
    vert_list = pd.merge(left=pd.DataFrame(data=list(it.product(list(set(prof_model_scope['LEARNING_STANDARD_ID'])),
                                                                list(set(enrollees['STUDENT_ID'])))),
                                           columns=['LEARNING_STANDARD_ID', 'STUDENT_ID']),
                         right=vert_list,
                         how='left')
    vert_list.loc[pd.isnull(vert_list['MASTERY_LEVEL_CAT']), 'MASTERY_LEVEL_CAT'] = 'UNMEASURED'
    #
    #    ⑸ Reshape the long table into a wide table of exhaustive evidentiary states for each subject. This dataframe
    #       represents our returned value.
    group_evid_state = vert_list[['STUDENT_ID',
                                  'LEARNING_STANDARD_ID',
                                  'MASTERY_LEVEL_CAT']].pivot(index='STUDENT_ID',
                                                              columns='LEARNING_STANDARD_ID',
                                                              values='MASTERY_LEVEL_CAT')
    return group_evid_state


#
#
# |∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕
# ⓥ ACQUIRE INPUT ENTTITIES FOR GROUP KNOWLEDGE-STATE EVALUATION.  Read data in from local — or locally-mapped — storage
#    in the form of csv tables. The local tables are "master" tables containing information pertinent to a large number
#    of analysis cases. Select only records pertinent to the case of interest.
#
#    Our inputs follow.
#    ⧐ analysis_case_parameters contains essential distinguishing attributes for an analysis case. These include
#      JURISDICTION_ID specifying the applicable learning-standards regime; TENANT_ID identifying a specific client;
#      and COURSE_ID indicating a coherent body of evidence including subjects (aka students) and proficiency
#      targets for which measurements are collecte.
#    ⧐ directory_path specifies the local-storage location containing the csv files.
#
#    We provide a dictionary-item output containing the following algorithm-ready tables.
#    ⧐ COURSE_MAP_EDGE is an edge list on which the Bayesian Network is based, for which
#      each vertex represents a proficiency-model learning target (aka "learning standard");
#    ⧐ CPT_LIST contains the conditional-probability tables for all vertex-degree scenarios
#      in COURSE_MAP_EDGE;
#    ⧐ VERTEX_LIST contains the collective body of evidencce for the group;
#    ⧐ COURSE_ENROLL indicates the subjects (aka students) of which the group is comprised; and
#    ⧐ MASTERY_COLOR_LIST contains the measurement-domain categorical partitions.
def acquire_local_group_analysis_data(directory_path):
    #    ⑴ First get the analysis_case_parameters from a file named SESSION_ATTRIBUTES. Reindex for
    #      subsequent convenience.
    analysis_case_parameters = pd.read_csv(os.path.abspath(os.path.join(if_tab_dir, 'SESSION_ATTRIBUTES.csv')))
    analysis_case_parameters = analysis_case_parameters.set_index(analysis_case_parameters['ATTRIBUTE'])
    #
    #    ⑵ Now read in and work the subsequent ta tables one at a time.
    #		⒜ Our COURSE_MAP_EDGE and conditional-probability tables CPT_LIST require no modification.  An exception
    #          is that we must coerce the MEAS attribute of CPT_LIST to float.  Designate CPT_LIST as global.
    COURSE_MAP_EDGE = pd.read_csv(filepath_or_buffer=os.path.abspath(os.path.join(if_tab_dir, 'COURSE_MAP_EDGE.csv')),
                                  dtype=str)[['LEARNING_STANDARD_ID',
                                              'CONSTITUENT_LEARNING_STD_ID']]
    global CPT_LIST
    CPT_LIST = pd.read_csv(filepath_or_buffer=os.path.abspath(os.path.join(if_tab_dir, 'CPT_LONG.csv')),
                           dtype=str)
    CPT_LIST['MEAS'] = CPT_LIST['MEAS'].astype(float)
    #
    #       ⒝ COURSE_ENROLL is a master table. We want to reduce it, retaining only records for our
    #          TENANT_ID, COURSE_ID, specifying our coherent subject group.
    COURSE_ENROLL = pd.read_csv(filepath_or_buffer=os.path.abspath(os.path.join(if_tab_dir, 'COURSE_ENROLL.csv')),
                                dtype='str')
    COURSE_ENROLL = \
    COURSE_ENROLL.loc[(COURSE_ENROLL['TENANT_ID'] == analysis_case_parameters.loc['TENANT_ID']['VALUE']) &
                      (COURSE_ENROLL['COURSE_ID'] == analysis_case_parameters.loc['COURSE_ID']['VALUE'])][
        ['STUDENT_ID']]
    #
    #       ⒞ Our evidence-categorization rules, MASTERY_COLOR_LIST, only contiain specifications for the actually
    #          measured-category assignments.  We must — in addition to reindexing — incorporate an UNBMEASURED category.
    #          We also incorporate a second integer-index label, for subsequent compactness.
    MASTERY_LEVEL_CAT = pd.read_csv(os.path.abspath(os.path.join(if_tab_dir, 'MASTERY_COLOR_LIST.csv')))[['TENANT_ID',
                                                                                                          'MASTERY_LEVEL_NAME',
                                                                                                          'FROM_THRESHOLD',
                                                                                                          'TO_THRESHOLD']] \
        .sort_values(by=['FROM_THRESHOLD'],
                     axis=0)
    MASTERY_LEVEL_CAT = MASTERY_LEVEL_CAT.loc[
        MASTERY_LEVEL_CAT['TENANT_ID'] == analysis_case_parameters['VALUE']['TENANT_ID']]
    MASTERY_LEVEL_CAT = pd.concat([MASTERY_LEVEL_CAT[['MASTERY_LEVEL_NAME',
                                                      'FROM_THRESHOLD',
                                                      'TO_THRESHOLD']],
                                   pd.DataFrame(data=[['UNMEASURED',
                                                       np.nan,
                                                       np.nan]],
                                                columns=['MASTERY_LEVEL_NAME',
                                                         'FROM_THRESHOLD',
                                                         'TO_THRESHOLD'])])
    MASTERY_LEVEL_CAT = MASTERY_LEVEL_CAT.set_index(MASTERY_LEVEL_CAT['MASTERY_LEVEL_NAME'])
    MASTERY_LEVEL_CAT.columns = ["MASTERY_LEVEL_CAT", "LOW_BOUND", "UP_BOUND"]
    MASTERY_LEVEL_CAT['CAT_LEVEL_IDX'] = range(len(MASTERY_LEVEL_CAT))
    #
    #       ⒟ VERTEX_LIST, our body-of-evidence table is a master table. We must "filter" it so as to only contain
    #          the records of interest. These are distinguished by the TENANT_ID attribute, and by a LEARNING_STANDARDS
    #          within the scope of the profiiency model specified by COURSE_MAP_EDGE.
    VERTEX_LIST = pd.read_csv(filepath_or_buffer=os.path.abspath(os.path.join(if_tab_dir, 'EoL_MEAS.csv')),
                              dtype='str')[['TENANT_ID', 'STUDENT_ID', 'LEARNING_STANDARD_ID',
                                            'LEARNING_STANDARD_CD', 'WORK_PRODUCT_TITLE',
                                            'ASSESSMENT_DATE', 'RAW_SCORE']]
    VERTEX_LIST = VERTEX_LIST.loc[VERTEX_LIST['TENANT_ID'] == analysis_case_parameters['VALUE']['TENANT_ID']][
        ['STUDENT_ID',
         'LEARNING_STANDARD_ID',
         'LEARNING_STANDARD_CD',
         'WORK_PRODUCT_TITLE',
         'ASSESSMENT_DATE',
         'RAW_SCORE']]
    prof_model_scope = pd.DataFrame(data=list(set(COURSE_MAP_EDGE['LEARNING_STANDARD_ID']) \
                                              .union(set(set(COURSE_MAP_EDGE['CONSTITUENT_LEARNING_STD_ID'])))),
                                    columns=['LEARNING_STANDARD_ID'])
    VERTEX_LIST = pd.merge(left=pd.merge(left=VERTEX_LIST,
                                         right=COURSE_ENROLL),
                           right=prof_model_scope)
    #
    #       ⒠ Construct the return-variable dictionary object and return.
    return {'VERTEX_LIST': VERTEX_LIST,
            'MASTERY_LEVEL_CAT': MASTERY_LEVEL_CAT,
            'COURSE_MAP_EDGE': COURSE_MAP_EDGE,
            'COURSE_ENROLL': COURSE_ENROLL,
            'SESSION_ATTRIBUTES': analysis_case_parameters}


#
#
# |∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕
# ⓦ EXECUTE GROUP KNOWLEDGE-STATE JOB.
def execute_group_know_state_job(directory_path):
    # ⑴ Acquire data for analysis-case of which job is defined. The data are acquired from csv-format tables
    #    in a local-directory location.
    GROUP_ANALYSIS_CASE = acquire_local_group_analysis_data(directory_path=directory_path)
    VERTEX_LIST = GROUP_ANALYSIS_CASE.get('VERTEX_LIST')
    MASTERY_LEVEL_CAT = GROUP_ANALYSIS_CASE.get('MASTERY_LEVEL_CAT')
    COURSE_MAP_EDGE = GROUP_ANALYSIS_CASE.get('COURSE_MAP_EDGE')
    COURSE_ENROLL = GROUP_ANALYSIS_CASE.get('COURSE_ENROLL')
    SESSION_ATTRIBUTES = GROUP_ANALYSIS_CASE.get('SESSION_ATTRIBUTES')
    global analysis_case_sig
    analysis_case_sig = '_'.join(SESSION_ATTRIBUTES['VALUE'])
    #
    # ⑵ Identify the structure of the group body-of-evidence contained in VERTEX_LIST.  First construct
    #    an exhaustive evidentiary-state table, containing the evidentiary state for all subjects
    #    (aka students) and learning-standard targets within the proficiency-model state represented
    #    by COURSE_MAP_EDGE, by which the Bayesian-network directed-acyclic graph is defined.
    #    These body-of-evidence structure-discovery operations are accomplished by locally-defined
    #    subroutines build_wide_evid_state_table and groupby_evid_profile, respectively.
    GROUP_EVID_STATE = build_wide_evid_state_table(vert_list=VERTEX_LIST,
                                                   digraph_edge_list=COURSE_MAP_EDGE,
                                                   enrollees=COURSE_ENROLL,
                                                   admiss_score_partitions=MASTERY_LEVEL_CAT)
    GROUP_EVID_PROFILE = groupby_evid_profile(wide_evid_dataframe=GROUP_EVID_STATE)
    #
    # ⑶ The group knowledge-state estimates result from the structure of the group evidentiary state.
    #    Locally-defined function evaluate_group_know_state orchestrates:
    #    ⧐ Decomposition of the directed-acyclic graph representing our proficiency model into
    #      computiationally-convenient subgraphs for each evidentiary profile;
    #    ⧐ Identification of subject evidentiary states for each subgraph and each conformee
    #      to the evidentiary profile;
    #    ⧐ Construction of a Pomegranate Bayesian Network for each subgraph;
    #    ⧐ Application of each distinct evidentiary state to perform a Bayesian-network query; and
    #      assembly of the Bayesian-network-query results into a knowledge-state table.
    print('Starting ' + ", ".join(SESSION_ATTRIBUTES['VALUE']) + " At Time: " + str(datetime.now().time()))
    # os.system('say "Gentlemen, start your engines!"')
    #
    #   The function evaluate_group_know_state updates the input-argument dictioanry object GROUP_EVID_PROFILE
    #   by adding two additional dataframe objects for each dictionary item:
    #   ⧐ KNOWLEDGE_STATE_ESTIMATE contains the comprehensive knowledge-state estimates for each proficiency-model
    #     target (aka learning standard) for each subject (aka student); and
    #   ⧐ CLUSTER_EXEC_TIME contains the execution-time statistics for each Bayesian-network query.
    GROUP_EVID_PROFILE = evaluate_group_know_state(evid_prof_dict=GROUP_EVID_PROFILE,
                                                   wide_evid_dataframe=GROUP_EVID_STATE,
                                                   digraph_edge_list=COURSE_MAP_EDGE,
                                                   var_states=MASTERY_LEVEL_CAT)
    #
    # ⑷ Construct and return a dictionary comprised of the aggregated, concatenated dataframes KNOWLEDGE_STATE_ESTIMATE
    #    and CLUSTER_EXEC_TIME returned by evaluate_group_know_state.
    return {'KNOWLEDGE_STATE_ESTIMATE': assemble_knowledge_state_table(evid_prof_dict=GROUP_EVID_PROFILE,
                                                                       vert_list=VERTEX_LIST,
                                                                       digraph_edge_list=COURSE_MAP_EDGE,
                                                                       admiss_score_partitions=MASTERY_LEVEL_CAT,
                                                                       analysis_case_parameters=SESSION_ATTRIBUTES),
            'CLUSTER_EXEC_TIME': assemble_exec_time_states(evid_prof_dict=GROUP_EVID_PROFILE)}


#
# |∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕
# ⓨ CALCULATE KNOWLEDGE STATE FOR  SINGLE EVIDENTIARY STATE.
def evaluate_single_evid_state(vert_list, digraph_edge_list, var_states, analysis_case_parameters):
    wide_evid_dataframe = build_wide_evid_state_table(vert_list=vert_list.rename(columns=
                                                                                 {'SIH_PERSONPK_ID_ST': 'STUDENT_ID',
                                                                                  'EVIDENCE_OF_LEARNING_SID': 'WORK_PRODUCT_TITLE'}),
                                                      digraph_edge_list=digraph_edge_list,
                                                      enrollees=vert_list[['SIH_PERSONPK_ID_ST']] \
                                                      .drop_duplicates().rename(columns={'SIH_PERSONPK_ID_ST':
                                                                                             'STUDENT_ID'}),
                                                      admiss_score_partitions=var_states)
    group_evid_prof = groupby_evid_profile(wide_evid_dataframe=wide_evid_dataframe)
    group_evid_prof = evaluate_group_know_state(evid_prof_dict=group_evid_prof,
                                                wide_evid_dataframe=wide_evid_dataframe,
                                                digraph_edge_list=digraph_edge_list,
                                                var_states=var_states)
    know_state_estimate = assemble_knowledge_state_table(evid_prof_dict=group_evid_prof,
                                                         vert_list=vert_list.rename(columns=
                                                                                    {'SIH_PERSONPK_ID_ST': 'STUDENT_ID',
                                                                                     'EVIDENCE_OF_LEARNING_SID': 'WORK_PRODUCT_TITLE'}),
                                                         digraph_edge_list=digraph_edge_list,
                                                         admiss_score_partitions=var_states,
                                                         analysis_case_parameters=analysis_case_parameters).rename(
        columns=
        {'STUDENT_ID': 'SIH_PERSONPK_ID_ST',
         'WORK_PRODUCT_TITLE': 'EVIDENCE_OF_LEARNING_SID'})
    know_state_estimate = know_state_estimate.loc[know_state_estimate['KNOWLEDGE_LVL_TYPE'] == 'ESTIMATED']
    return know_state_estimate


#
def onboard_initialize_student_know_state(vert_list, digraph_edge_list, var_states, analysis_case_parameters):
    wide_evid_dataframe = build_wide_evid_state_table(vert_list=vert_list.rename(columns=
                                                                                 {'SIH_PERSONPK_ID_ST': 'STUDENT_ID',
                                                                                  'EVIDENCE_OF_LEARNING_SID': 'WORK_PRODUCT_TITLE'}),
                                                      digraph_edge_list=digraph_edge_list,
                                                      enrollees=vert_list[['SIH_PERSONPK_ID_ST']] \
                                                      .drop_duplicates().rename(columns={'SIH_PERSONPK_ID_ST':
                                                                                             'STUDENT_ID'}),
                                                      admiss_score_partitions=var_states)
    group_evid_prof = groupby_evid_profile(wide_evid_dataframe=wide_evid_dataframe)
    group_evid_prof = evaluate_group_know_state(evid_prof_dict=group_evid_prof,
                                                wide_evid_dataframe=wide_evid_dataframe,
                                                digraph_edge_list=digraph_edge_list,
                                                var_states=var_states)
    know_state_estimate = assemble_knowledge_state_table(evid_prof_dict=group_evid_prof,
                                                         vert_list=vert_list.rename(columns=
                                                                                    {'SIH_PERSONPK_ID_ST': 'STUDENT_ID',
                                                                                     'EVIDENCE_OF_LEARNING_SID': 'WORK_PRODUCT_TITLE'}),
                                                         digraph_edge_list=digraph_edge_list,
                                                         admiss_score_partitions=var_states,
                                                         analysis_case_parameters=analysis_case_parameters).rename(
        columns=
        {'STUDENT_ID': 'SIH_PERSONPK_ID_ST',
         'WORK_PRODUCT_TITLE': 'EVIDENCE_OF_LEARNING_SID'})
    know_state_estimate = know_state_estimate.loc[know_state_estimate['KNOWLEDGE_LVL_TYPE'] != 'UNMEASURED']
    return know_state_estimate


#
def diagnose_group_know_state():
    global if_tab_dir
    if_tab_dir = '/Users/nahamlet/Box Sync/IBM-Watson ED K12/Pathway-Centric CONOPS/Darwin-Release Realization/WEDC-Schema-Aligned EoL/ONBOARD_TOOL_IF_TABLES'

    GROUP_KNOW_STATE = execute_group_know_state_job(directory_path=if_tab_dir)
    GROUP_KNOW_STATE.get('KNOWLEDGE_STATE_ESTIMATE') \
        .sort_values(by=['STUDENT_ID',
                         'LEARNING_STANDARD_ID'],
                     axis=0).to_csv(path_or_buf=os.path.abspath(os.path.join(if_tab_dir,
                                                                             "STUDENT_KNOWLEDGE_LEVEL.csv")),
                                    index=False,
                                    encoding='utf-8',
                                    line_terminator='\r\n',
                                    quoting=csv.QUOTE_NONNUMERIC)
    GROUP_KNOW_STATE.get('CLUSTER_EXEC_TIME').to_csv(path_or_buf=os.path.abspath(os.path.join(if_tab_dir,
                                                                                              "CLUSTER_EXEC_TIME.csv")),
                                                     index=False,
                                                     encoding='utf-8',
                                                     line_terminator='\r\n',
                                                     quoting=csv.QUOTE_NONNUMERIC)
    return

#
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
def decompose_digraph(composite_digraph, measured_verts, prof_idx):
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
                                                                          subgraph_dict_item=digraph_subgraphs.get(
                                                                              key_idx),
                                                                          measured_verts=measured_verts))
    #
    #    🄳 Add ego-one subgraph from composite_digraph centered on each of the high-valency vertices.
    for vert_idx in high_degree_verts:
        high_degree_ego_graph_vert_idx = nx.ego_graph(G=composite_digraph,
                                                      n=vert_idx,
                                                      radius=1,
                                                      undirected=True)
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
    #   ≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈
    #   ⛔⛔⛔⛔⛔⛔⛔⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇ DIAGNOSTIC FOR DEVELOPMENT ONLY ⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⛔⛔⛔⛔⛔⛔⛔
    #   ⛔⛔⛔⛔⛔⛔⛔⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆ DIAGNOSTIC FOR DEVELOPMENT ONLY ⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⛔⛔⛔⛔⛔⛔⛔
    #   ≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈
    #
    #    🄴 Second Pass at Degenrate Cases:
    #       ⑵ Discard graphs for which all vertices are measured.
    digraph_subgraphs = dict((key, value) for key, value in digraph_subgraphs.items()
                             if len(digraph_subgraphs.get(key).get('INDUCED_SUBGRAPH_VERTS')) !=
                             len(set(digraph_subgraphs.get(key).get('INDUCED_SUBGRAPH_VERTS')) \
                                 .intersection(set(measured_verts))))
    digraph_subgraphs = dict((key, value) for key, value in digraph_subgraphs.items()
                             if len(digraph_subgraphs.get(key).get('MEAS_SPANNING_SUBGRAPH_VERTS')) > 0)
    digraph_subgraphs = dict((key, value) for key, value in digraph_subgraphs.items()
                             if nx.is_directed_acyclic_graph(G=digraph_subgraphs.get(key).get('SPANNING_SUBGRAPH')))
    for key_idx in list(digraph_subgraphs.keys()):
        digraph_subgraphs.get(key_idx).update(
            {'NOT_COMPLEX_GRAPH': True if len(digraph_subgraphs.get(key_idx).get('SPANNING_SUBGRAPH_VERTS')) <= 12
            else False})
    #
    #
    #
    #   ≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈
    #   ⛔⛔⛔⛔⛔⛔⛔⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇ DIAGNOSTIC FOR DEVELOPMENT ONLY ⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⛔⛔⛔⛔⛔⛔⛔
    #       Plot png bitmap graphics of the in-range graphical neighborhood and of each subgraph.
    #	ad_hoc_plot_dir = '/Users/nahamlet/Box Sync/IBM-Watson ED K12/Pathway-Centric CONOPS/Darwin-Release Realization/WEDC-Schema-Aligned EoL/MASTY_ONBOARD_ARTIFACTS/260000_T004_72000/AD_HOC_SUBGRAPH_PLOTS'
    #	for dict_key in list(digraph_subgraphs.keys()):
    #		nx_digraph_dict_key = digraph_subgraphs.get(dict_key).get('INDUCED_SUBGRAPH').fresh_copy()
    #		nx_digraph_dict_key.add_nodes_from(list(digraph_subgraphs.get(dict_key).get('INDUCED_SUBGRAPH').nodes()))
    #		nx_digraph_dict_key.add_edges_from(list(digraph_subgraphs.get(dict_key).get('INDUCED_SUBGRAPH').edges()))
    #		nx_digraph_dict_key.add_nodes_from(nodes = list(set(digraph_subgraphs.get(dict_key).get('INDUCED_SUBGRAPH_VERTS'))\
    #														.intersection(set(measured_verts))),
    #											style = 'filled',
    #											fillcolor = '#d7d2cb')
    #		agraph_dict_key = nx.nx_agraph.to_agraph(N = nx_digraph_dict_key)
    #		agraph_dict_key.layout('dot')
    #		agraph_dict_key.draw(os.path.abspath(os.path.join(ad_hoc_plot_dir,dict_key + '_INDUCED_SUBGRAPH.png')))
    #
    #		nx_digraph_dict_key = digraph_subgraphs.get(dict_key).get('SPANNING_SUBGRAPH').fresh_copy()
    #		nx_digraph_dict_key.add_nodes_from(list(digraph_subgraphs.get(dict_key).get('SPANNING_SUBGRAPH').nodes()))
    #		nx_digraph_dict_key.add_edges_from(list(digraph_subgraphs.get(dict_key).get('SPANNING_SUBGRAPH').edges()))
    #		nx_digraph_dict_key.add_nodes_from(nodes = list(set(digraph_subgraphs.get(dict_key).get('SPANNING_SUBGRAPH_VERTS'))\
    #														.intersection(set(measured_verts))),
    #											style = 'filled',
    #											fillcolor = '#d7d2cb')
    #		agraph_dict_key = nx.nx_agraph.to_agraph(N = nx_digraph_dict_key)
    #		agraph_dict_key.layout('dot')
    #		agraph_dict_key.draw(os.path.abspath(os.path.join(ad_hoc_plot_dir,dict_key + '_MEAS_EXT_SUBGRAPH.png')))
    #
    #   ⛔⛔⛔⛔⛔⛔⛔⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆ DIAGNOSTIC FOR DEVELOPMENT ONLY ⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⛔⛔⛔⛔⛔⛔⛔
    #   ≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈
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
#
#
#
##################⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆ LOCAL FUNCTIONS ⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆#############
# |∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—|∕—\|#
#################################################################################################################################
#
#
#



def worker(lock):
    try:
        global CPT_LIST
        global Last_Upd_Usr
        global Last_Upd_Trans
        # |∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|         #
        # START LOGGING & SCRIPT EXECUTION                                                                                      #
        #
        # Begin calculation of script execution time
        start = time.time()
        #
        # logging.basicConfig(filename='Mastery_LKS_log.log'  # format='%(asctime)s %(message)s'
        #                    , level=logging.INFO)
        logger = logging.getLogger("Rotating Log")
        logger.setLevel(logging.ERROR)

        handler = TimedRotatingFileHandler('logs/Mastery_LKS_log.log',
                                           when="d",
                                           interval=1,
                                           backupCount=365)
        logger.addHandler(handler)
        #
        # ⦾ INITIALIZE CONFIGURATION PARAMETERS                                                                                 #
        #  /Users/nahamlet@us.ibm.com/Box Sync/Analytics-Mastery/POMEGRANATE_R2.3_20171208
        #prov_dir = '/Users/nahamlet@us.ibm.com/Box Sync/Analytics-Mastery/POMEGRANATE_R2.3_20171208'
        #read_config_file = open(os.path.abspath(os.path.join(prov_dir, '180202_1445Z_CUSA_Math6__Mastery_config_STAGING.txt')), "r")
        read_config_file = open('Mastery_config.txt', "r")
        configurations = {}
        for config_data in read_config_file:
            aa = re.sub('\s+', '', config_data)
            ab = ast.literal_eval(aa)
            configurations.update(ab)
        ip_addr = configurations["DB_IP_Address"]
        port = configurations["DB_Port_Address"]
        usr_name = configurations["Username"]
        passwrd = configurations["Password"]
        Database = configurations["Database"]
        schema_name = configurations["Schema_name"]
        Last_Upd_Usr = configurations["Last_Upd_Usr"]
        Last_Upd_Trans = configurations["Last_Upd_Trans"]
        row_config = configurations["Fetch_rows"]
        #
        # DATABASE QUERIES                                                                                                      #
        #
        # EOL_MEAS input query
        add_rows = row_config + " ROWS ONLY ) "
        EOL_MEAS_QRY = "SELECT  DISTINCT TENANT_ID,SUBJECT_TITLE FROM (" + \
                       "SELECT DISTINCT em.TENANT_ID,em.SUBJECT_TITLE,em.EOL_MEAS_PK_ID " + \
                       "FROM IBMSIH.EOL_MEAS em " + \
                       "WHERE em.SUBJECT_TITLE IS NOT NULL " + \
                       " AND em.STATUS='PENDING' AND em.IS_LOCK='N' " + \
                       " ORDER BY em.EOL_MEAS_PK_ID ASC  " + \
                       " FETCH FIRST " + add_rows
        #
        # MASTERY_LEVEL_CAT input query
        MLC_QRY = "SELECT TENANT_ID, MASTERY_LEVEL_NAME, FROM_THRESHOLD, TO_THRESHOLD FROM IBMSIH.MASTERY_COLOR_LIST WHERE TENANT_ID IN "
        #
        # Default ERROR state query in case model fails.
        EOL_UPD_ERR_QRY = "UPDATE IBMSIH.EOL_MEAS SET STATUS ='ERROR' WHERE  STATUS = 'RUNNING' "
        #
        #
        # ① ACQUIRE INPUT DATA.                                                                                                #
        #
        # DATABASE CONNECTION
        #
        conn_str = "db2+ibm_db://" + usr_name + ":" + passwrd + "@" + ip_addr + ":" + port + "/" + Database
        dbEngine = create_engine(conn_str, convert_unicode=True)
        #
        ##----- IBM_DB CONNECTIVITY TO CATER TO REQUIREMENTS OF RUNNGIN SQL UPDATE STATEMENTS -----##
        #
        ibm_db_conn = ibm_db.connect("DATABASE=" + Database + ";HOSTNAME=" + ip_addr + ";PORT=" + port + ";PROTOCOL=TCPIP;UID=" + usr_name + ";PWD=" + passwrd + ";","", "")
        dbEngine_upd = ibm_db_dbi.Connection(ibm_db_conn)
    #
    except Exception as e:
        # raise
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(exc_type, exc_tb.tb_lineno)
        logger.error(str(datetime.utcnow().strftime('%Y-%m-%d %H.%M.%S')) + ' : Error at Line : ' + str(exc_tb.tb_lineno))
        logger.error(str(datetime.utcnow().strftime('%Y-%m-%d %H.%M.%S')) + ' : ERROR ON MAIN TRY : ' + str(e))
        print "ERROR ON MAIN TRY : " + str(e)

    stop_req = stop_requested()
    while stop_req == 1:
        stop_req = stop_requested()
        lock.acquire()
        try:
            try:
                #########################################################################################################################
                # |∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|         #
                #  Evidence of Learning Table
                print (EOL_MEAS_QRY)
                EVIDENCE_OF_LEARNING = pd.read_sql(EOL_MEAS_QRY, dbEngine)
                EVIDENCE_OF_LEARNING.columns = [x.upper() for x in EVIDENCE_OF_LEARNING.columns]
                # |∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|         #
                #########################################################################################################################

            except Exception as er:
                # raise
                exc_type, exc_obj, exc_tb = sys.exc_info()
                print(exc_type, exc_tb.tb_lineno)
                logger.error(str(datetime.utcnow().strftime('%Y-%m-%d %H.%M.%S')) + ' : Error at Line : ' + str(exc_tb.tb_lineno))
                logger.error(str(datetime.utcnow().strftime('%Y-%m-%d %H.%M.%S')) + ' : ERROR ON INPUT QUERY RUN : ' + str(er))
                print ("ERROR ON INPUT QUERY RUN : " + str(er))
                return

        finally:
            lock.release()

        for index, row in EVIDENCE_OF_LEARNING.iterrows():  ## index, row = list(EVIDENCE_OF_LEARNING.iterrows())[5]
            try:
                #########################################################################################################################
                # |∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|         #
                start = time.time()
                #

                # Evidence of Learning Table
                EVIDENCE_OF_LEARNING = pd.DataFrame([row]).astype(str)
                EVIDENCE_OF_LEARNING.columns = [x.upper() for x in EVIDENCE_OF_LEARNING.columns]
                #EVIDENCE_OF_LEARNING.astype(str)
                ################################### Read from JSON file ####################################
                #
                input_dict = json.load(open('sc_map.json'))
                #input_dict = json.load(open(os.path.abspath(os.path.join(prov_dir, '180202_1445Z_CUSA_Math6_sc_map.json'))))
                TENANT_SUBJ_COURSE = dict([(str(list_dict.get(u'tenantId')) + '_' + str(list_dict.get(u'Subject')),
                                            {'SihCoursePk': list_dict.get(u'SihCoursePk'),
                                             'tenantId': str(list_dict.get(u'tenantId')),
                                             'Subject': str(list_dict.get(u'Subject')),
											 'CurrentAcademicYear': str(list_dict.get(u'CurrentAcademicYear'))
											 })
                                           for list_dict in json.load(open('sc_map.json'))])

                print (TENANT_SUBJ_COURSE)

                eoltenant = EVIDENCE_OF_LEARNING.TENANT_ID.unique()
                eoltenant = eoltenant[0]

                eolsub = EVIDENCE_OF_LEARNING.SUBJECT_TITLE.unique()
                eolsub = eolsub[0]
                print (eolsub)

                print (str(EVIDENCE_OF_LEARNING['TENANT_ID'].values[0]))
                print (str(EVIDENCE_OF_LEARNING['SUBJECT_TITLE'].values[0]))
                print (TENANT_SUBJ_COURSE.get(eoltenant + '_' + eolsub).get('SihCoursePk'))

                output_dict = [x for x in input_dict if (x['tenantId'] == eoltenant and x['Subject'] == eolsub)]
                TENANT_COURSE_LIST = TENANT_SUBJ_COURSE.get(str(EVIDENCE_OF_LEARNING['TENANT_ID'].values[0]) + '_' + EVIDENCE_OF_LEARNING['SUBJECT_TITLE'].values[0]).get('SihCoursePk')
                # TENANT_COURSE_LIST = map(str, TENANT_COURSE_LIST)
                TENANT_COURSE_LIST = dict([(course_ID, "(" + str(course_ID) + ")") for course_ID in TENANT_COURSE_LIST])
                # output_dict = [x for x in input_dict if (x['tenantId'] == 'T001' and x['Subject'] == 'RAJ')]

                #print output_dict
                result = output_dict[0]
                print (output_dict)
                courselist = result['SihCoursePk']
                courselist = str(courselist)
                courselist = courselist.replace("[", "(")
                courselist = courselist.replace("]", ")")
                print ("courselist :" + courselist)
                CRS = courselist

                CurAcaYr = str(result['CurrentAcademicYear'])
                print ("CurrentAcademicYear :" + CurAcaYr)
                #
                ###########################################################################################

                # TENANT_IDs
                TNT_ID = EVIDENCE_OF_LEARNING.TENANT_ID.unique()
                print ("TENANT_ID : " + str(TNT_ID))

                # SUBJECT TITLEs
                SBJ = str(EVIDENCE_OF_LEARNING.SUBJECT_TITLE.unique())
                SBJ = "('" + SBJ + "')"
                SBJ = SBJ.replace("'[u'", "'")
                SBJ = SBJ.replace("'['", "'")
                SBJ = SBJ.replace("']'", "'")
                print ("SUBJECT_TITLE : " + SBJ)
                #print str(SBJ)

                # Declare Tenant & Course for ERROR logging in case it errors out here
                T_ID = TNT_ID
                cr = CRS
                #
                #
                #
                # |∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|         #
                #########################################################################################################################


                # Loop through each Tenants
                for t in TNT_ID:  # t = TNT_ID[0]
                    print ("##########################################")
                    T_ID = "('" + t + "')"
                    print (T_ID)
                    print ("##########################################")

                    # Loop through each Course
                    for cr in list(TENANT_COURSE_LIST.keys()):  ## cr = list(TENANT_COURSE_LIST.keys())[0]
                        course_run_start = time.time()
                        print ("##########################################")
                        # C = "'" + s + "'"
                        print (cr, T_ID)
                        #
                        # Begin Data Prep activites  from input queries
                        print ("################### COURSE_LEARNING_STANDARD #######################")
                        #
                        # GetLearningStandardForCourse
                        COURSE_LEARNING_STANDARD = pd.read_sql(
                                                    "SELECT DISTINCT cls.TENANT_ID,cls.SIH_COURSEPK_ID,cls.LEARNING_STANDARD_ID "
                                                    "FROM IBMSIH.COURSE_LEARNING_STD_MAP cls "
                                                    "JOIN IBMSIH.SIHLEARNING_STANDARD sls ON cls.LEARNING_STANDARD_ID=sls.LEARNING_STANDARD_ID "
                                                    "WHERE cls.TENANT_ID = " + T_ID +
                                                    " AND cls.SIH_COURSEPK_ID = " + TENANT_COURSE_LIST.get(cr) +
                                                    "AND sls.CLUSTER_YN=0 AND sls.DOMAIN_YN=0"
                                                , dbEngine).rename(columns={'learning_standard_id': 'LEARNING_STANDARD_ID'})['LEARNING_STANDARD_ID']
                        #
                        #
                        CLS = COURSE_LEARNING_STANDARD.empty
                        #
                        if COURSE_LEARNING_STANDARD.size == 0:
                            COURSE_LEARNING_STANDARD = str("(NULL)")
                        else:
                            COURSE_LEARNING_STANDARD = str(tuple(COURSE_LEARNING_STANDARD))
                            COURSE_LEARNING_STANDARD = COURSE_LEARNING_STANDARD.replace("()", "('')")
                            COURSE_LEARNING_STANDARD = COURSE_LEARNING_STANDARD.replace(",)", ")")
                        if CLS:
                            logger.error(str(datetime.utcnow().strftime('%Y-%m-%d %H.%M.%S')) + " GetLearningStandardForCourse DataFrame is empty for Tenant: " + T_ID + " & Course : " + TENANT_COURSE_LIST.get(cr))
                            print("GetLearningStandardForCourse DataFrame is empty for Tenant: " + T_ID + " & Course : " + TENANT_COURSE_LIST.get(cr))

                            eol_updatetime = "'" + str(datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')) + "'"
                            print eol_updatetime
                            EOL_SUBCRS_MAP_QRY = " UPDATE IBMSIH.EOL_MEAS_SUB_CRS_MAP em SET STATUS ='ERROR',LAST_UPDATE_DT= " + eol_updatetime + \
                                                 " WHERE TENANT_ID = " + T_ID + \
                                                 " AND SUBJECT = " + SBJ + \
                                                 " AND SIH_COURSEPK_ID= " + TENANT_COURSE_LIST.get(cr) + \
                                                 " AND STATUS IS NULL "

                            print EOL_SUBCRS_MAP_QRY
                            ibm_db.exec_immediate(ibm_db_conn, EOL_SUBCRS_MAP_QRY)

                            continue
                        #
                        print ("##########################################")
                        #
                        # GetStudentsForCourse
                        print ("################### COURSE_ENROLLEES #######################")
                        # NEEDS to be Updated
                        # if a student is DONE for 1 Course and is Pending for other in same SUBJECT - the record will NOT be picked for that student.
                        COURSE_ENROLLEES = pd.read_sql(
                            "SELECT DISTINCT spr.SIH_PERSONPK_ID "
                            "FROM IBMSIH.SIHCOURSE sc  "
                            "JOIN IBMSIH.COURSE_SECTION cs ON sc.SIH_COURSEPK_ID=cs.SIH_COURSEPK_ID AND sc.TENANT_ID=cs.TENANT_ID "
                            "JOIN IBMSIH.COURSE_SECTION_STUDENT cs2 ON cs.COURSE_SECTION_SID=cs2.COURSE_SECTION_SID "
                            "JOIN IBMSIH.SIHORGPERSONROLE spr ON cs2.SIH_ORG_PERSON_ROLEPK_ID=spr.SIH_ORG_PERSON_ROLEPK_ID "
                            "WHERE sc.TENANT_ID = " + T_ID +
                            " AND sc.SIH_COURSEPK_ID = " + TENANT_COURSE_LIST.get(cr) +
                            " AND cs.IS_ACTIVE_YN=1 AND cs2.IS_ACTIVE_YN=1 "
                            " AND spr.SIH_PERSONPK_ID IN "
                            " ( SELECT DISTINCT em.SIHPERSON_PKID FROM IBMSIH.EOL_MEAS em "
                            "  WHERE em.TENANT_ID = " + T_ID +
                            "  AND em.SUBJECT_TITLE =  " + SBJ +
                            "  AND em.STATUS ='PENDING' ) "
                            , dbEngine).rename(columns={'sih_personpk_id': 'SIH_PERSONPK_ID'})['SIH_PERSONPK_ID']
                        #
                        #
                        CE = COURSE_ENROLLEES.empty
                        if COURSE_ENROLLEES.size == 0:
                            COURSE_ENROLLEES = str("(NULL)")
                        else:
                            COURSE_ENROLLEES = str(tuple(COURSE_ENROLLEES))
                            COURSE_ENROLLEES = COURSE_ENROLLEES.replace("()", "('')")
                            COURSE_ENROLLEES = COURSE_ENROLLEES.replace(",)", ")")
                        if CE:
                            logger.error(str(datetime.utcnow().strftime('%Y-%m-%d %H.%M.%S')) + " GetStudentsForCourse DataFrame is empty for Tenant: " + T_ID + " & Course : " + TENANT_COURSE_LIST.get(cr))
                            print("GetStudentsForCourse DataFrame is empty for Tenant: " + T_ID + " & Course : " + TENANT_COURSE_LIST.get(cr))

                            eol_updatetime = "'" + str(datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')) + "'"
                            print eol_updatetime
                            EOL_SUBCRS_MAP_QRY = " UPDATE IBMSIH.EOL_MEAS_SUB_CRS_MAP em SET STATUS ='ERROR',LAST_UPDATE_DT= " + eol_updatetime + \
                                                 " WHERE TENANT_ID = " + T_ID + \
                                                 " AND SUBJECT = " + SBJ + \
                                                 " AND SIH_COURSEPK_ID= " + TENANT_COURSE_LIST.get(cr) + \
                                                 " AND STATUS IS NULL "

                            print EOL_SUBCRS_MAP_QRY
                            ibm_db.exec_immediate(ibm_db_conn, EOL_SUBCRS_MAP_QRY)

                            continue
                        #
                        #
                        # EOL_MEAS Update set Status = DONE
                        EOL_UPD_DONE_QRY_1 = "UPDATE IBMSIH.EOL_MEAS em SET STATUS ='DONE' WHERE STATUS = 'RUNNING' " \
                                             " AND em.TENANT_ID = " + T_ID + \
                                             " AND em.SIHPERSON_PKID IN " + COURSE_ENROLLEES + \
                                             " AND em.LEARNING_STANDARD_ID IN " + COURSE_LEARNING_STANDARD

                        #
                        # EOL_MEAS Update set Status = ERROR
                        EOL_UPD_ERR_QRY_1 = "UPDATE IBMSIH.EOL_MEAS em SET STATUS ='ERROR' WHERE STATUS = 'RUNNING' " \
                                            " AND em.TENANT_ID = " + T_ID + \
                                            " AND em.SIHPERSON_PKID IN " + COURSE_ENROLLEES + \
                                            " AND em.LEARNING_STANDARD_ID IN " + COURSE_LEARNING_STANDARD
                        #

                        #

                        #
                        print ("##########################################")

                        #
                        print ("################### EDGE_LIST #######################")

                        COURSE_MAP_EDGE = pd.read_sql(
                                        "WITH firstlevel_predecessor AS "
                                        "( "
                                        "SELECT DISTINCT  "
                                        "t1.TENANT_ID AS TENANT_ID , sc.SUBJECT_TITLE,  "
                                        "t1.CONSTITUENT_LEARNING_STD_ID AS LEARNING_STANDARD_FROM , t2.LEARNING_STANDARD_CD AS FROM_CODE, "
                                        "t0.LEARNING_STANDARD_ID AS LEARNING_STANDARD_TO , t0.LEARNING_STANDARD_CD AS TO_CODE "
                                        "FROM IBMSIH.SIHLEARNING_STANDARD t0 "
                                        "LEFT JOIN IBMSIH.SIHLEARNING_STANDARD_HIERARCHY t1 ON t0.LEARNING_STANDARD_ID=t1.LEARNING_STANDARD_ID "
                                        "LEFT JOIN IBMSIH.SIHLEARNING_STANDARD t2 ON t1.CONSTITUENT_LEARNING_STD_ID=t2.LEARNING_STANDARD_ID "
                                        "LEFT JOIN IBMSIH.SIHSTANDARD_CONTENT sc ON sc.STANDARD_CONTENT_ID = t0.STANDARD_CONTENT_ID "
                                        "										AND t2.CLUSTER_YN=0 AND t2.DOMAIN_YN=0 "
                                        "WHERE t0.CLUSTER_YN=0 AND t0.DOMAIN_YN=0 AND t1.GRAPH_TYPE='PROGRESSION' "
                                        "AND t0.LEARNING_STANDARD_ID IN " + COURSE_LEARNING_STANDARD +
                                        "AND t1.TENANT_ID = " + T_ID +
                                        "AND sc.SUBJECT_TITLE = " + SBJ +
                                        " ), "
                                        "secondlevel_predecessor AS "
                                        "( "
                                        "SELECT DISTINCT t1.TENANT_ID AS TENANT_ID , sc.SUBJECT_TITLE, "
                                        "t1.CONSTITUENT_LEARNING_STD_ID AS LEARNING_STANDARD_FROM , t2.LEARNING_STANDARD_CD AS FROM_CODE, "
                                        "t0.LEARNING_STANDARD_ID AS LEARNING_STANDARD_TO , t0.LEARNING_STANDARD_CD AS TO_CODE "
                                        "FROM IBMSIH.SIHLEARNING_STANDARD t0 "
                                        "LEFT JOIN IBMSIH.SIHLEARNING_STANDARD_HIERARCHY t1 ON t0.LEARNING_STANDARD_ID=t1.LEARNING_STANDARD_ID "
                                        "LEFT JOIN IBMSIH.SIHLEARNING_STANDARD t2 ON t1.CONSTITUENT_LEARNING_STD_ID=t2.LEARNING_STANDARD_ID AND t2.CLUSTER_YN=0 AND t2.DOMAIN_YN=0 "
                                        "LEFT JOIN IBMSIH.SIHSTANDARD_CONTENT sc ON sc.STANDARD_CONTENT_ID = t0.STANDARD_CONTENT_ID "
                                        "WHERE t0.CLUSTER_YN=0 AND t0.DOMAIN_YN=0 AND t2.CLUSTER_YN=0 AND t2.DOMAIN_YN=0 "
                                        "AND t1.GRAPH_TYPE='PROGRESSION' "
                                        "AND t0.LEARNING_STANDARD_ID IN (SELECT DISTINCT LEARNING_STANDARD_FROM FROM firstlevel_predecessor) "
                                        "AND t1.TENANT_ID = " + T_ID +
                                        "AND sc.SUBJECT_TITLE = " + SBJ +
                                        " ), "
                                        "firstlevel_successor AS "
                                        "( "
                                        "SELECT DISTINCT "
                                        "t1.TENANT_ID AS TENANT_ID , sc.SUBJECT_TITLE, "
                                        "t0.LEARNING_STANDARD_ID AS LEARNING_STANDARD_FROM ,t0.LEARNING_STANDARD_CD AS FROM_CODE, "
                                        "t1.LEARNING_STANDARD_ID AS LEARNING_STANDARD_TO,t2.LEARNING_STANDARD_CD AS TO_CODE "
                                        "FROM IBMSIH.SIHLEARNING_STANDARD t0 "
                                        "LEFT JOIN IBMSIH.SIHLEARNING_STANDARD_HIERARCHY t1 ON t0.LEARNING_STANDARD_ID=t1.CONSTITUENT_LEARNING_STD_ID "
                                        "LEFT JOIN IBMSIH.SIHLEARNING_STANDARD t2 ON t1.LEARNING_STANDARD_ID=t2.LEARNING_STANDARD_ID "
                                        "LEFT JOIN IBMSIH.SIHSTANDARD_CONTENT sc ON sc.STANDARD_CONTENT_ID = t0.STANDARD_CONTENT_ID "
                                        "WHERE t0.CLUSTER_YN=0 AND t0.DOMAIN_YN=0 AND t2.CLUSTER_YN=0 AND t2.DOMAIN_YN=0 AND t1.GRAPH_TYPE='PROGRESSION' "
                                        "AND t0.LEARNING_STANDARD_ID IN  " + COURSE_LEARNING_STANDARD +
                                        "AND t2.LEARNING_STANDARD_ID NOT IN  " + COURSE_LEARNING_STANDARD +
                                        #"AND t2.LEARNING_STANDARD_ID IN  " + COURSE_LEARNING_STANDARD +
                                        "AND t1.TENANT_ID = " + T_ID +
                                        "AND sc.SUBJECT_TITLE = " + SBJ +
                                        " ), "
                                        "secondlevel_successor AS "
                                        "( "
                                        "SELECT DISTINCT "
                                        "t1.TENANT_ID AS TENANT_ID , sc.SUBJECT_TITLE, "
                                        "t0.LEARNING_STANDARD_ID AS LEARNING_STANDARD_FROM ,t0.LEARNING_STANDARD_CD AS FROM_CODE, "
                                        "t1.LEARNING_STANDARD_ID AS LEARNING_STANDARD_TO,t2.LEARNING_STANDARD_CD AS TO_CODE "
                                        "FROM IBMSIH.SIHLEARNING_STANDARD t0 "
                                        "LEFT JOIN IBMSIH.SIHLEARNING_STANDARD_HIERARCHY t1 ON t0.LEARNING_STANDARD_ID=t1.CONSTITUENT_LEARNING_STD_ID "
                                        "LEFT JOIN IBMSIH.SIHLEARNING_STANDARD t2 ON t1.LEARNING_STANDARD_ID=t2.LEARNING_STANDARD_ID "
                                        "LEFT JOIN IBMSIH.SIHSTANDARD_CONTENT sc ON sc.STANDARD_CONTENT_ID = t0.STANDARD_CONTENT_ID "
                                        "WHERE t0.CLUSTER_YN=0 AND t0.DOMAIN_YN=0 AND t2.CLUSTER_YN=0 AND t2.DOMAIN_YN=0 AND t1.GRAPH_TYPE='PROGRESSION' "
                                        "AND t0.LEARNING_STANDARD_ID IN (SELECT DISTINCT LEARNING_STANDARD_TO FROM firstlevel_successor) "
                                        "AND t2.LEARNING_STANDARD_ID NOT IN (SELECT DISTINCT LEARNING_STANDARD_TO FROM firstlevel_successor) "
                                        #"AND t2.LEARNING_STANDARD_ID IN " + COURSE_LEARNING_STANDARD +
                                        "AND t1.TENANT_ID = " + T_ID +
                                        "AND sc.SUBJECT_TITLE = " + SBJ +
                                        " ), "
                                        "overall_neigbourhood AS "
                                        "( "
                                        "SELECT * FROM firstlevel_predecessor UNION "
                                        "SELECT * FROM firstlevel_successor UNION "
                                        "SELECT * FROM secondlevel_predecessor UNION "
                                        "select * from secondlevel_successor) "
                                        "SELECT * FROM overall_neigbourhood  ORDER BY FROM_CODE DESC "
                            , dbEngine).rename(columns={'tenant_id': 'TENANT_ID',
                                                        'subject_title': 'SUBJECT_TITLE',
                                                        'learning_standard_from': 'CONSTITUENT_LEARNING_STD_ID',
                                                        'from_code': 'CONSTITUENT_LEARNING_STD_CD',
                                                        'learning_standard_to': 'LEARNING_STANDARD_ID',
                                                        'to_code': 'LEARNING_STANDARD_CD'}).applymap(str)

                        #
                        CME_1 = COURSE_MAP_EDGE['LEARNING_STANDARD_ID']
                        CME_2 = COURSE_MAP_EDGE['CONSTITUENT_LEARNING_STD_ID']
                        CME_3 = CME_1.append(CME_2)
                        CME_3 = CME_3.unique()
                        #
                        # CHECK COURSE_MAP_EDGE FOR ACYCILICITY.
                        COURSE_MAP_GRAPH = nx.DiGraph()
                        COURSE_MAP_GRAPH.add_edges_from(COURSE_MAP_EDGE[['CONSTITUENT_LEARNING_STD_ID',
                                                                                'LEARNING_STANDARD_ID']].to_records(index=False).tolist())
                        COURSE_MAP_GRAPH_IS_NOT_DAG = not nx.is_directed_acyclic_graph(G=COURSE_MAP_GRAPH)
                        #
                        # Derive from COURSE_MAP_EDGE the course graphical neighborhood.
                        COURSE_GRAPHICAL_NEIGHBORHOOD = str(tuple(set(COURSE_MAP_EDGE['CONSTITUENT_LEARNING_STD_ID']) \
                                                                  .union(set(set(COURSE_MAP_EDGE['LEARNING_STANDARD_ID'])))))
                        print ('COURSE_GRAPHICAL_NEIGHBORHOOD : ')
                        print (COURSE_GRAPHICAL_NEIGHBORHOOD)
                        #
                        if CME_3.size == 0:
                            CME_3 = str("(NULL)")
                        else:
                            CME_3 = str(tuple(CME_3))
                        #
                        #
                        CME = COURSE_MAP_EDGE.empty
                        if CME:
                            print("COURSE_MAP_EDGE DataFrame is empty for Tenant: " + T_ID + " & Course : " + TENANT_COURSE_LIST.get(cr))
                        #
                        print ("################### Update EOL_MEAS to DONE/ERROR #######################")
                        # UPDATE RECORDS TAKEN FROM EOL_MEAS TO HAVE STATUS OF RUNNING & IS_LOCK OF S
                        # Used COURSE_GRAPHICAL_NEIGHBORHOOD instead of COURSE_LEARNING_STANDARD to update status
                        # this is done in case neighborhood Learning_Std does not exist in CLS_MAP but is in EOL_MEAS

                        # EOL_MEAS Update set Status = DONE
                        EOL_UPD_DONE_QRY_1 = "UPDATE IBMSIH.EOL_MEAS em SET STATUS ='DONE' WHERE STATUS = 'RUNNING' " \
                                             " AND em.TENANT_ID = " + T_ID + \
                                             " AND em.SIHPERSON_PKID IN " + COURSE_ENROLLEES + \
                                             " AND ( em.LEARNING_STANDARD_ID IN " + COURSE_GRAPHICAL_NEIGHBORHOOD + \
                                             " OR em.LEARNING_STANDARD_ID IN " + COURSE_LEARNING_STANDARD + " ) "

                        #
                        # EOL_MEAS Update set Status = ERROR
                        EOL_UPD_ERR_QRY_1 = "UPDATE IBMSIH.EOL_MEAS em SET STATUS ='ERROR' WHERE STATUS = 'RUNNING' " \
                                            " AND em.TENANT_ID = " + T_ID + \
                                            " AND em.SIHPERSON_PKID IN " + COURSE_ENROLLEES + \
                                            " AND ( em.LEARNING_STANDARD_ID IN " + COURSE_GRAPHICAL_NEIGHBORHOOD + \
                                            " OR em.LEARNING_STANDARD_ID IN " + COURSE_LEARNING_STANDARD + " ) "
                        #
                        #
                        print ("################### Update EOL_MEAS to RUNNING #######################")
                        # UPDATE RECORDS TAKEN FROM EOL_MEAS TO HAVE STATUS OF RUNNING & IS_LOCK OF S
                        # Used COURSE_GRAPHICAL_NEIGHBORHOOD instead of COURSE_LEARNING_STANDARD to update status
                        # this is done in case neighborhood Learning_Std does not exist in CLS_MAP but is in EOL_MEAS
                        mid_level_flg = "UPDATE IBMSIH.EOL_MEAS em SET STATUS ='RUNNING' , IS_LOCK='S' " \
                                        " WHERE em.STATUS='PENDING' AND em.IS_LOCK='N' " \
                                        " AND em.TENANT_ID = " + T_ID + \
                                        " AND em.SIHPERSON_PKID IN " + COURSE_ENROLLEES + \
                                        " AND ( em.LEARNING_STANDARD_ID IN " + COURSE_GRAPHICAL_NEIGHBORHOOD + \
                                        " OR em.LEARNING_STANDARD_ID IN " + COURSE_LEARNING_STANDARD + " ) "
                        print mid_level_flg
                        MID_LVL_FLG_QRY = ibm_db.exec_immediate(ibm_db_conn, mid_level_flg)
                        #
                        #
                        print ("##########################################")
                        #
                        print ("################### VERTEX_LIST #######################")

                        VERTEX_LIST = pd.read_sql(
                                   "WITH ORDERED AS "
                                    " (SELECT lcb.TENANT_ID, sc.SUBJECT_TITLE, lcb.JURISDICTION_ID, lcb.LEARNING_STANDARD_ID, lcb.VERT_CPT_BRIDGE_IDX,  "
                                    " x.SIH_PERSONPK_ID_ST as SIH_PERSONPK_ID_ST, x.RAW_SCORE,  "
                                    " (case WHEN x.RAW_SCORE IS NULL THEN 'Yes' ELSE 'No' END ) AS IS_ESTIMATED ,  "
                                    " x.ASSESSMENT_DATE as ASSESSMENT_DATE, x.EVIDENCE_OF_LEARNING_SID as EVIDENCE_OF_LEARNING_SID,  "
                                    " ROW_NUMBER() OVER (PARTITION BY x.SIH_PERSONPK_ID_ST,lcb.LEARNING_STANDARD_ID ORDER BY  x.ASSESSMENT_DATE  DESC) AS rn  "
                                    " FROM IBMSIH.LEARN_STD_CPT_BRIDGE lcb  "
                                    " JOIN IBMSIH.SIHLEARNING_STANDARD sls ON sls.LEARNING_STANDARD_ID = lcb.LEARNING_STANDARD_ID  "
                                    " JOIN IBMSIH.SIHORGPERSONROLE spr ON 1=1  "
                                    " AND spr.SIH_PERSONPK_ID IN " + COURSE_ENROLLEES +
                                    " JOIN IBMSIH.SIHSTANDARD_CONTENT sc ON sc.STANDARD_CONTENT_ID = sls.STANDARD_CONTENT_ID  "
                                    " LEFT JOIN "
                                    " (SELECT * FROM IBMSIH.EVIDENCE_OF_LEARNING eol  "
                                    " JOIN IBMSIH.WRK_PRDCT_LRNG_STD  wp ON  wp.WORK_PRODUCT_SID=eol.WORK_PRODUCT_SID) x  "
                                    " ON  spr.SIH_PERSONPK_ID=x.SIH_PERSONPK_ID_ST  AND sls.LEARNING_STANDARD_ID=x.LEARNING_STANDARD_ID  "
                                    " WHERE lcb.TENANT_ID = " + T_ID +
                                    " AND sls.LEARNING_STANDARD_ID IN " + COURSE_GRAPHICAL_NEIGHBORHOOD +
                                    " ) "
                                    " SELECT * FROM  ORDERED WHERE rn = 1  "
                                    " ORDER BY LEARNING_STANDARD_ID DESC  "
                                    , dbEngine)
                        VERTEX_LIST.columns = [x.upper() for x in VERTEX_LIST.columns]
                        VERTEX_LIST_unmeasured = VERTEX_LIST.loc[np.logical_and(np.isnan(VERTEX_LIST['RAW_SCORE']),
                                                                                np.isfinite(VERTEX_LIST[
                                                                                                'EVIDENCE_OF_LEARNING_SID'])),
                                                                 ['EVIDENCE_OF_LEARNING_SID',
                                                                  'SIH_PERSONPK_ID_ST',
                                                                  'LEARNING_STANDARD_ID']]
                        VERTEX_LIST_measured = VERTEX_LIST.dropna(axis=0,
                                                                  subset=['RAW_SCORE'])
                        # VERTEX_LIST = VERTEX_LIST.dropna(axis=0,subset=['RAW_SCORE'])
                        VERTEX_LIST_measured['LEARNING_STANDARD_ID'] = VERTEX_LIST_measured[
                            'LEARNING_STANDARD_ID'].astype(str)
                        VERTEX_LIST_measured['SIH_PERSONPK_ID_ST'] = VERTEX_LIST_measured['SIH_PERSONPK_ID_ST'].astype(
                            int).astype(str)
                        VERTEX_LIST_measured['EVIDENCE_OF_LEARNING_SID'] = VERTEX_LIST_measured[
                            'EVIDENCE_OF_LEARNING_SID'].astype(int).astype(str)
                        VL = VERTEX_LIST_measured.empty
                        if VL:
                            print(
                            "VERTEX_LIST DataFrame is empty for Tenant : " + T_ID + " & Course : " + TENANT_COURSE_LIST.get(
                                cr))
                        VRT_ID = VERTEX_LIST_measured.VERT_CPT_BRIDGE_IDX.unique()
                        VRT = str(tuple(VRT_ID))
                        VRT = VRT.replace("u'", "'")
                        VRT = VRT.replace("',)", "')")
                        VRT = VRT.replace("()", "(NULL)")
                        #
                        print (VERTEX_LIST_measured)
                        print "##########################################"
                        # Check if Thresholds differ in CPT_LONG & Mastery_Color_List
                        THRES_CHECK = pd.read_sql(" SELECT (CASE WHEN cpt_cnt=COALESCE(mcl_cnt,0) THEN 1 ELSE 0 END) "
                                                    " FROM (SELECT c1.TENANT_ID,Count(*)  mcl_cnt FROM IBMSIH.MASTERY_COLOR_LIST c1 WHERE c1.TENANT_ID IN " + T_ID +
                                                    " GROUP BY c1.TENANT_ID) y "
                                                    " LEFT JOIN (SELECT c1.TENANT_ID,Count(*) cpt_cnt FROM IBMSIH.CPT_LONG c1 WHERE c1.TENANT_ID IN " + T_ID +
                                                    " AND c1.CONSTITUENT_COUNT=0 GROUP BY c1.TENANT_ID,c1.CONSTITUENT_COUNT) x ON x.TENANT_ID=y.TENANT_ID "
                                                    , dbEngine)
                        THRES_CHECK = int(THRES_CHECK.iloc[0])
                        if THRES_CHECK==1:
                            print 'Mastery Thresholds matches for CPT_LONG & Mastery_Color_List'
                        else:
                            print 'Mastery Thresholds DO NOT match for CPT_LONG & Mastery_Color_List'
                            logger.error(str(datetime.utcnow().strftime('%Y-%m-%d %H.%M.%S')) + ' : ####################### ERROR!! #######################')
                            logger.error(str(datetime.utcnow().strftime(
                                '%Y-%m-%d %H.%M.%S')) + 'Mastery Thresholds DO NOT match for CPT_LONG & Mastery_Color_List for Tenant : ' + T_ID + ' & Course : ' + TENANT_COURSE_LIST.get(cr) )
                            EOL_UPDATE_ERR_1 = EOL_UPD_ERR_QRY_1
                            print EOL_UPDATE_ERR_1
                            logger.error(str(datetime.utcnow().strftime('%Y-%m-%d %H.%M.%S')) + ' : Exiting from loop for Tenant : ' + T_ID + ' & Course : ' + TENANT_COURSE_LIST.get(cr) + ' due to missing input data')
                            logger.error(str(datetime.utcnow().strftime('%Y-%m-%d %H.%M.%S')) + ' : Query : ' + EOL_UPDATE_ERR_1)
                            EOL_UPDATE_QRY = ibm_db.exec_immediate(ibm_db_conn, EOL_UPDATE_ERR_1)
                            continue
                        #
                        if not VL:
                            CONSTITUENT_COUNT = str(tuple(range(int(
                                COURSE_MAP_EDGE.groupby(by='LEARNING_STANDARD_ID', as_index=True)[
                                    'CONSTITUENT_LEARNING_STD_ID'].agg(
                                    'count').max() + 1))))
                            print ("Constituent Count : " + CONSTITUENT_COUNT)
                            CPT_LIST = pd.read_sql("SELECT * FROM IBMSIH.CPT_LONG cpt "
                                                   "WHERE cpt.CONSTITUENT_COUNT IN " + CONSTITUENT_COUNT +
                                                   "AND cpt.TENANT_ID IN " + T_ID +
                                                   "ORDER BY CONSTITUENT_COUNT ASC, CPT_CELL_IDX ASC"
                                                   , dbEngine)
                            CPT_LIST.columns = [x.upper() for x in CPT_LIST.columns]
                            CPT_LIST['CPT_CELL_IDX'] = CPT_LIST['CPT_CELL_IDX'].astype(str)
                            CPT_LIST['IS_ROOT'] = CPT_LIST['IS_ROOT'].astype(str)
                            CPT_LIST['CONSTITUENT_COUNT'] = CPT_LIST['CONSTITUENT_COUNT'].astype(str)
                            # CPT_LIST = CPT_LIST.sort_values(by = ['CPT_CELL_IDX'],axis = 0)
                            CPTL = CPT_LIST.empty
                            if CPTL:
                                print("CPT_LIST DataFrame is empty for Tenant: " + T_ID + " & Course : " + TENANT_COURSE_LIST.get(cr))
                                #
                                #
                        ML = MLC_QRY + T_ID + " ORDER BY FROM_THRESHOLD ASC"
                        MASTERY_LEVEL_CAT = pd.read_sql(ML, dbEngine).sort_values(by='from_threshold', axis=0)
                        MASTERY_LEVEL_CAT.columns = [x.upper() for x in MASTERY_LEVEL_CAT.columns]
                        #       We need to account for an UNMEASURED knowledge state. Add a corresponding row to MASTERY_LEVEL_CAT.
                        MASTERY_LEVEL_CAT = pd.concat([MASTERY_LEVEL_CAT[['MASTERY_LEVEL_NAME',
                                                                          'FROM_THRESHOLD',
                                                                          'TO_THRESHOLD']],
                                                       pd.DataFrame(data=[['UNMEASURED',
                                                                           np.nan,
                                                                           np.nan]],
                                                                    columns=['MASTERY_LEVEL_NAME',
                                                                             'FROM_THRESHOLD',
                                                                             'TO_THRESHOLD'])])
                        MASTERY_LEVEL_CAT = MASTERY_LEVEL_CAT.set_index(MASTERY_LEVEL_CAT['MASTERY_LEVEL_NAME'])
                        MASTERY_LEVEL_CAT.columns = ["MASTERY_LEVEL_CAT", "LOW_BOUND", "UP_BOUND"]
                        MASTERY_LEVEL_CAT['CAT_LEVEL_IDX'] = range(len(MASTERY_LEVEL_CAT))
                        MLC = MASTERY_LEVEL_CAT.empty
                        if MLC:
                            print("MASTERY_LEVEL_CAT DataFrame is empty for Tenant: " + T_ID + " & Course : " + TENANT_COURSE_LIST.get(cr))
                        SESSION_ATTRIBUTES = pd.DataFrame(data=[['TENANT_ID', t]],
                                                          columns=['ATTRIBUTE', 'VALUE'],
                                                          index=['TENANT_ID'])
                        print ('SESSION ATTRIBUTES : ')
                        print (SESSION_ATTRIBUTES)
                        #
                        #
                        #
                        # |∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|         #
                        #########################################################################################################################
                        #
                        # Check if any input dataframes are empty and if so, dont proceed for the combination of Tenant x Subject.
                        # Empty CME is acceptable as we will be estimating for SINGLETON vertices in case of missing EDGEs.
                        ERR_CHK = False
                        if (VL or CPTL or MLC):  ## Removed empty edge list from loop-exit criterion.
                            ERR_CHK = True
                            logger.error(str(datetime.utcnow().strftime('%Y-%m-%d %H.%M.%S')) + ' : ####################### ERROR!! #######################')
                            print ("Exiting from loop for Tenant : " + T_ID + " & Course : " + TENANT_COURSE_LIST.get(cr) + " due to missing input data")
                            # UPDATE RECORDS TAKEN FROM EOL_MEAS TO HAVE STATUS OF RUNNING.
                            EOL_UPDATE_ERR_1 = EOL_UPD_ERR_QRY_1
                            print EOL_UPDATE_ERR_1
                            logger.error(str(datetime.utcnow().strftime('%Y-%m-%d %H.%M.%S')) + ' : Exiting from loop for Tenant : ' + T_ID + ' & Course : ' + TENANT_COURSE_LIST.get(cr) + ' due to missing input data')
                            logger.error(str(datetime.utcnow().strftime('%Y-%m-%d %H.%M.%S')) + ' : Query : ' + EOL_UPDATE_ERR_1)
                            EOL_UPDATE_QRY = ibm_db.exec_immediate(ibm_db_conn, EOL_UPDATE_ERR_1)
                            #continue
                        #
                        if CME:
                            ERR_CHK = True
                            print ("SINGLETON VERTEX for Tenant : " + T_ID + " & Course : " + TENANT_COURSE_LIST.get(cr) + " due to missing EDGE_LIST data.")
                            logger.error(str(datetime.utcnow().strftime('%Y-%m-%d %H.%M.%S')) + ' : SINGLETON VERTEX for Tenant : ' + T_ID + ' & Course : ' + TENANT_COURSE_LIST.get(cr) + ' due to missing EDGE_LIST data.')
                            EOL_UPDATE_UPD_1 = EOL_UPD_DONE_QRY_1
                            print EOL_UPDATE_UPD_1
                            EOL_UPDATE_QRY1 = ibm_db.exec_immediate(ibm_db_conn, EOL_UPDATE_UPD_1)
                            #continue
                        #
                        if COURSE_MAP_GRAPH_IS_NOT_DAG:
                            ERR_CHK = True
                            print ("COURSE_MAP_EDGE has cycles : " + T_ID + " & Course : " + TENANT_COURSE_LIST.get(cr))
                            logger.error(str(datetime.utcnow().strftime('%Y-%m-%d %H.%M.%S')) + 'COURSE_MAP_EDGE has cycles : ' + T_ID + ' & Course : ' + TENANT_COURSE_LIST.get(cr))
                            EOL_UPDATE_UPD_1 = EOL_UPD_ERR_QRY_1
                            print EOL_UPDATE_UPD_1
                            EOL_UPDATE_QRY1 = ibm_db.exec_immediate(ibm_db_conn, EOL_UPDATE_UPD_1)
                            #continue
                        #########################################################################################################################
                        #
                        #########################################################################################################################
                        # |∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|         #
                        #########################################################################################################################
                        # |∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|∕—\|         #
                        #   ≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈
                        #   ⛔⛔⛔⛔⛔⛔⛔⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇ REAL WORK OCCURS HERE! ⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⛔⛔⛔⛔⛔⛔⛔

                        if ERR_CHK == True:
                            print '######## Missing Data Error as ERR_CHK flag is TRUE #########'
                            logger.error(str(datetime.utcnow().strftime('%Y-%m-%d %H.%M.%S')) + ' : ERROR as ERR_CHK is TRUE' )

                            eol_updatetime = "'" + str(datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')) + "'"
                            print eol_updatetime
                            EOL_SUBCRS_MAP_QRY = " UPDATE IBMSIH.EOL_MEAS_SUB_CRS_MAP em SET STATUS ='ERROR',LAST_UPDATE_DT= " + eol_updatetime + \
                                                 " WHERE TENANT_ID = " + T_ID + \
                                                 " AND SUBJECT = " + SBJ + \
                                                 " AND SIH_COURSEPK_ID= " + TENANT_COURSE_LIST.get(cr) + \
                                                 " AND STATUS IS NULL "

                            print EOL_SUBCRS_MAP_QRY
                            ibm_db.exec_immediate(ibm_db_conn, EOL_SUBCRS_MAP_QRY)
                            #continue

                        elif ERR_CHK == False:
                            #Default ERROR state query in case model fails.
                            EOL_UPD_ERR_QRY = "UPDATE IBMSIH.EOL_MEAS SET STATUS ='ERROR' WHERE  STATUS = 'RUNNING' "

                            KNOWLEDGE_STATE = onboard_initialize_student_know_state(vert_list=VERTEX_LIST,
                                                                                digraph_edge_list=COURSE_MAP_EDGE,
                                                                                var_states=MASTERY_LEVEL_CAT,
                                                                                analysis_case_parameters=SESSION_ATTRIBUTES)

                            KNOWLEDGE_STATE = KNOWLEDGE_STATE.assign(LAST_UPDATE_USER=Last_Upd_Usr) \
                                                            .assign(LAST_UPDATE_TX_ID=Last_Upd_Trans)

                            KNOWLEDGE_STATE['ACADEMIC_YEAR'] = CurAcaYr

                            PROCESSED_EOL_RECORDS = KNOWLEDGE_STATE.loc[KNOWLEDGE_STATE['KNOWLEDGE_LVL_TYPE'] == 'MEASURED',
                                                                    ['EVIDENCE_OF_LEARNING_SID',
                                                                     'SIH_PERSONPK_ID_ST',
                                                                     'LEARNING_STANDARD_ID']]

                            NOT_PROCESSED_EOL_RECORDS = pd.merge(left=VERTEX_LIST,
                                                                 right=pd.DataFrame(data=list(
                                                                     (set(PROCESSED_EOL_RECORDS.to_records(
                                                                         index=False).tolist()) - \
                                                                      set(VERTEX_LIST[
                                                                              PROCESSED_EOL_RECORDS.columns].to_records(
                                                                          index=False).tolist())) \
                                                                         .union(set(VERTEX_LIST_unmeasured.to_records(
                                                                         index=False).tolist()))),
                                                                     columns=PROCESSED_EOL_RECORDS.columns.tolist()))
                            #
                            print "##########################################"
                            print 'PROCESSED_EOL_RECORDS : '
                            print PROCESSED_EOL_RECORDS
                            print '############################'
                            print 'NOT_PROCESSED_EOL_RECORDS : '
                            print NOT_PROCESSED_EOL_RECORDS
                            print '############################'
                            #
                            # Data from PROCESSED_EOL
                            R_SIHPERSONID = str(tuple(PROCESSED_EOL_RECORDS['SIH_PERSONPK_ID_ST'].drop_duplicates().values.tolist()))
                            R_SIHPERSONID = R_SIHPERSONID.replace("'", "")
                            R_SIHPERSONID = R_SIHPERSONID.replace(",)", ")")

                            R_LSID = str(tuple(PROCESSED_EOL_RECORDS['LEARNING_STANDARD_ID'].drop_duplicates().values.tolist()))
                            R_LSID = R_LSID.replace("'", "")
                            R_LSID = R_LSID.replace(",)", ")")

                            R_EOLSID = str(tuple(PROCESSED_EOL_RECORDS['EVIDENCE_OF_LEARNING_SID'].drop_duplicates().values.tolist()))
                            R_EOLSID = R_EOLSID.replace("'", "")
                            R_EOLSID = R_EOLSID.replace(",)", ")")

                            # Data from NOTPROCESSED_EOL
                            R1_SIHPERSONID = str(tuple(NOT_PROCESSED_EOL_RECORDS['SIH_PERSONPK_ID_ST'].drop_duplicates().values.tolist()))
                            R1_SIHPERSONID = R1_SIHPERSONID.replace("'", "")
                            R1_SIHPERSONID = R1_SIHPERSONID.replace(",)", ")")

                            R1_LSID = str(tuple(NOT_PROCESSED_EOL_RECORDS['LEARNING_STANDARD_ID'].drop_duplicates().values.tolist()))
                            R1_LSID = R1_LSID.replace("'", "")
                            R1_LSID = R1_LSID.replace(",)", ")")

                            R1_EOLSID = str(tuple(NOT_PROCESSED_EOL_RECORDS['EVIDENCE_OF_LEARNING_SID'].drop_duplicates().values.tolist()))
                            R1_EOLSID = R1_EOLSID.replace("'", "")
                            R1_EOLSID = R1_EOLSID.replace(",)", ")")
                            #
                            # EOL_MEAS Update set Status = DONE
                            #EOL_UPD_DONE_QRY = "UPDATE IBMSIH.EOL_MEAS SET STATUS ='DONE' WHERE STATUS = 'RUNNING' " \
                            #                   " AND SIHPERSON_PKID IN " + R_SIHPERSONID + \
                            #                   " AND LEARNING_STANDARD_ID IN " + R_LSID
                            #
                            # EOL_MEAS Update set Status = ERROR
                            #EOL_UPD_ERR_QRY = "UPDATE IBMSIH.EOL_MEAS SET STATUS ='ERROR' WHERE  STATUS = 'RUNNING' " \
                            #                  " AND SIHPERSON_PKID IN " + R_SIHPERSONID + \
                            #                  " AND LEARNING_STANDARD_ID IN " + R_LSID
                            #
                            # EOL_MEAS Update set Status = DONE
                            EOL_UPD_DONE_QRY = "UPDATE IBMSIH.EOL_MEAS SET STATUS ='DONE' WHERE STATUS = 'RUNNING' " \
                                               " AND SIHPERSON_PKID IN " + R_SIHPERSONID + \
                                               " AND LEARNING_STANDARD_ID IN " + R_LSID
                            #
                            # EOL_MEAS Update set Status = ERROR
                            EOL_UPD_ERR_QRY = "UPDATE IBMSIH.EOL_MEAS SET STATUS ='ERROR' WHERE  STATUS = 'RUNNING' " \
                                              " AND ( SIHPERSON_PKID IN " + R_SIHPERSONID + \
                                              " OR SIHPERSON_PKID IN " + R1_SIHPERSONID + \
                                              " ) AND ( LEARNING_STANDARD_ID IN " + R_LSID + \
                                              " AND LEARNING_STANDARD_ID IN " + R1_LSID + \
                                              " ) "
                            #
                            KNOWLEDGE_STATE = KNOWLEDGE_STATE.loc[KNOWLEDGE_STATE['KNOWLEDGE_LVL_TYPE'] == 'ESTIMATED']
                            #   ⛔⛔⛔⛔⛔⛔⛔⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆ REAL WORK OCCURS HERE! ⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⛔⛔⛔⛔⛔⛔⛔
                            # print KNOWLEDGE_STATE
                            #   ≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈
                            #
                            if not KNOWLEDGE_STATE.empty:

                                # Convert the column EVIDENCE_OF_LEARNING_SID to Numeric and coerce to Nulls if required.
                                KNOWLEDGE_STATE.loc[:, ['EVIDENCE_OF_LEARNING_SID']] = pd.to_numeric(KNOWLEDGE_STATE['EVIDENCE_OF_LEARNING_SID'], errors='coerce').tolist()

                                # Convert the column KNOWLEDGE_LVL_EVIDENCE_DATE to coerce to Nulls if required.
                                KNOWLEDGE_STATE.loc[:, ['KNOWLEDGE_LVL_EVIDENCE_DATE']] = pd.to_datetime(KNOWLEDGE_STATE['KNOWLEDGE_LVL_EVIDENCE_DATE'], errors='coerce').tolist()
                                KNOWLEDGE_STATE.loc[:, ['KNOWLEDGE_LVL_ASOF_DATE']] = pd.to_datetime(KNOWLEDGE_STATE['KNOWLEDGE_LVL_ASOF_DATE'], errors='coerce').tolist()
                                # KNOWLEDGE_STATE.loc[:,['LAST_UPDATE_DT']] = pd.to_datetime(KNOWLEDGE_STATE['LAST_UPDATE_DT'], errors='coerce').tolist()

                                KNOWLEDGE_STATE['KNOWLEDGE_LVL_EVIDENCE_DATE'] = KNOWLEDGE_STATE['KNOWLEDGE_LVL_EVIDENCE_DATE'].replace(np.nan, '', regex=True)

                                # Convert Datetime columns
                                KNOWLEDGE_STATE['KNOWLEDGE_LVL_EVIDENCE_DATE'] = pd.to_datetime(KNOWLEDGE_STATE['KNOWLEDGE_LVL_EVIDENCE_DATE'])
                                KNOWLEDGE_STATE['KNOWLEDGE_LVL_ASOF_DATE'] = pd.to_datetime(KNOWLEDGE_STATE['KNOWLEDGE_LVL_ASOF_DATE'])
                                # KNOWLEDGE_STATE['LAST_UPDATE_DT'] = pd.to_datetime(KNOWLEDGE_STATE['LAST_UPDATE_DT'])

                                # Codefix to update for ERROR on 13Dec - Shan & Shankar
                                KNOWLEDGE_STATE['KNOWLEDGE_LVL_EVIDENCE_DATE'] = KNOWLEDGE_STATE['KNOWLEDGE_LVL_ASOF_DATE']

                                # Take records only with KNOWLEDGE_LVL_TYPE = 'ESTIMATED'
                                SKL_DF = KNOWLEDGE_STATE[KNOWLEDGE_STATE['KNOWLEDGE_LVL_TYPE'] == 'ESTIMATED']
                                # SKL_DF['EVIDENCE_OF_LEARNING_SID'] = EOLSID

                                del SKL_DF['STUDENT_KNOWLEDGE_LVL_SID']

                                print SKL_DF
                                # BULK-INSERT all records into database table STUDENT_KNOWLEDGE_LEVEL
                                SKL_DF.to_sql(name="STUDENT_KNOWLEDGE_LEVEL", schema="IBMSIH", con=dbEngine,if_exists='append', chunksize=1000, index=False)

                                # UPDATE records in EOL_MEAS to have STATUS of DONE which were processed in this run.
                                EOL_UPDATE_DONE = EOL_UPD_DONE_QRY
                                print EOL_UPDATE_DONE
                                EOL_UPDATE_QRY = ibm_db.exec_immediate(ibm_db_conn, EOL_UPDATE_DONE)

                                # Set NOT PROCESSED records in EOL_MEAS for records which are left and did not get picked.
                                # Cases like missing Vertex List for a sub-cluster in course where other cluster had Vertex List will be updated
                                EOL_UPD_NOTPR_QRY_1 = "UPDATE IBMSIH.EOL_MEAS em SET STATUS ='NOTPROCESSED' WHERE STATUS = 'RUNNING' " \
                                                     " AND em.TENANT_ID = " + T_ID + \
                                                     " AND em.SIHPERSON_PKID IN " + COURSE_ENROLLEES + \
                                                     " AND ( em.LEARNING_STANDARD_ID IN " + COURSE_GRAPHICAL_NEIGHBORHOOD + \
                                                     " OR em.LEARNING_STANDARD_ID IN " + COURSE_LEARNING_STANDARD + " ) "
                                print EOL_UPD_NOTPR_QRY_1
                                EOL_NOTPROCESS_QRY = ibm_db.exec_immediate(ibm_db_conn, EOL_UPD_NOTPR_QRY_1)

                                eol_updatetime = "'" + str(datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')) + "'"
                                print eol_updatetime
                                EOL_SUBCRS_MAP_QRY = " UPDATE IBMSIH.EOL_MEAS_SUB_CRS_MAP em SET STATUS ='COMPLETED',LAST_UPDATE_DT= " + eol_updatetime + \
                                                     " WHERE TENANT_ID = " + T_ID + \
                                                     " AND SUBJECT = " + SBJ + \
                                                     " AND SIH_COURSEPK_ID= " + TENANT_COURSE_LIST.get(cr) + \
                                                     " AND STATUS IS NULL "

                                print EOL_SUBCRS_MAP_QRY
                                ibm_db.exec_immediate(ibm_db_conn, EOL_SUBCRS_MAP_QRY)

                                print "################### TOTAL EXECUTION TIME #######################"
                                print 'Elapsed Execution Time: ', time.time() - start, 'seconds.'
                                print "################################################################"

                            else:
                                print(str(datetime.utcnow().strftime('%Y-%m-%d %H.%M.%S')) + " : KNOWLEDGE_STATE Dataframe empty for COURSE:" + CRS)
                                EOL_UPDATE_DONE = EOL_UPD_DONE_QRY
                                print EOL_UPDATE_DONE
                                EOL_UPDATE_QRY = ibm_db.exec_immediate(ibm_db_conn, EOL_UPDATE_DONE)


            except Exception as err:
                # raise
                exc_type, exc_obj, exc_tb = sys.exc_info()
                print(exc_type, exc_tb.tb_lineno)
                print "ERROR!!"
                logger.error(str(datetime.utcnow().strftime('%Y-%m-%d %H.%M.%S')) + ' : ####################### ERROR!! #######################')
                logger.error(str(datetime.utcnow().strftime('%Y-%m-%d %H.%M.%S')) + ' : Error at Line : ' + str(exc_tb.tb_lineno))
                logger.error(str(datetime.utcnow().strftime('%Y-%m-%d %H.%M.%S')) + ' : Error : ' + str(err))
                logger.error(str(datetime.utcnow().strftime('%Y-%m-%d %H.%M.%S')) + ' : Elapsed Execution Time: ' + str(time.time() - start) + 'seconds.')
                # UPDATE RECORDS TAKEN FROM EOL_MEAS TO HAVE STATUS OF RUNNING.
                EOL_UPDATE_ERR = EOL_UPD_ERR_QRY
                print EOL_UPDATE_ERR
                logger.error(str(datetime.utcnow().strftime('%Y-%m-%d %H.%M.%S')) + ' : TENANT_ID : ' + T_ID)
                logger.error(str(datetime.utcnow().strftime('%Y-%m-%d %H.%M.%S')) + ' : Course : ' + TENANT_COURSE_LIST.get(cr))
                logger.error(str(datetime.utcnow().strftime('%Y-%m-%d %H.%M.%S')) + ' : Query : ' + EOL_UPDATE_ERR)
                EOL_UPDATE_QRY = ibm_db.exec_immediate(ibm_db_conn, EOL_UPDATE_ERR)
                logger.error(str(datetime.utcnow().strftime('%Y-%m-%d %H.%M.%S')) + ' : ####################### ERROR!! #######################')

                eol_updatetime = "'" + str(datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')) + "'"
                print eol_updatetime
                EOL_SUBCRS_MAP_QRY = " UPDATE IBMSIH.EOL_MEAS_SUB_CRS_MAP em SET STATUS ='ERROR',LAST_UPDATE_DT= " + eol_updatetime + \
                                     " WHERE TENANT_ID = " + T_ID + \
                                     " AND SUBJECT = " + SBJ + \
                                     " AND SIH_COURSEPK_ID= " + TENANT_COURSE_LIST.get(cr) + \
                                     " AND STATUS IS NULL "

                print EOL_SUBCRS_MAP_QRY
                ibm_db.exec_immediate(ibm_db_conn, EOL_SUBCRS_MAP_QRY)

    else:
        T_nul = 0
        return


#


def stop_requested():
    read_config_file1 = open('Mastery_config.txt', "r")
    configurations1 = {}
    for config_data1 in read_config_file1:
        aa1 = re.sub('\s+', '', config_data1)
        ab1 = ast.literal_eval(aa1)
        configurations1.update(ab1)
    stop_close = configurations1["Diagnosis"]
    if stop_close == "start":
        ret = 1
        return ret
    elif stop_close == "stop":
        ret = 2
        return ret