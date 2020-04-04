# coding: utf-8
import os
import pandas as pd
import numpy as np
import math as mt
import csv
import subprocess
import networkx as nx
from datetime import datetime, date, timedelta
from collections import Counter
import time
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.engine.url import URL
import ibm_db
import ibm_db_dbi
from pandas.util import hash_pandas_object
import itertools as it
import functools as fct
import json
import re
import pysftp
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.style
import matplotlib.font_manager as font_manager
import matplotlib.patches as patches
#
source_dir = '/Users/nahamlet@us.ibm.com/Box Sync/IBM-Watson ED K12/Clients/Vancouver/T015 Start of AY2018-19'
work_dir = '/Users/nahamlet@us.ibm.com/Box Sync/IBM-Watson ED K12/Pathway-Centric CONOPS/Mastery-Readiness Offering/Solution'
db_credential_dir = '/Users/nahamlet@us.ibm.com/Documents/Documents/Oportunidades actuales'
desktop_dir = '/Users/nahamlet@us.ibm.com/Desktop'
font_path = '/Library/Fonts'
prop = font_manager.FontProperties(fname=font_path)
#
## PURPOSE:  CONCATENATE COURSE_UNIT TABLES WITH MEASURED LEARNING STANDARDS NOT PREVIOUSLY IN COURSE BLUEPRINTs.
##           We frequently encounter instances in which student/learning-standard measurements against learning standards
##           not within the a priori proficiency scope for a given course. This might occur for at least two
##           reasons:
##           ⧐ The measured learning standard resides at a different hierarchical level than that specified in the
##             course blueprint; or
##           ⧐ The measured learning standard is otherwise ommitted from the course blueprint or learning-standard
##             progressions.
##           We want here to identify by course the learning-standard measurements that are not in the course blueprint
##           or in the learning standard progressions. If those learning standards are in the SUBJECT × GRADE_LEVEL
##           "cover" of the course-graphical neighborhood.
##
## APPROACH: 
## ⓪ Set up session. This includes reading in session_attributes, querying IBMSIHG to get the logical 
##    location of the client DB, and creating the query-connection specifications.
## Ⓐ Query the production database for three tables:  
##    ⧐ proficiency_span exhaustively lists by (course, section, student, learning_standard) the intended
##      curriculum;
##    ⧐ algmt_detail contains a mapping of the assessed curriculum onto the intended curriculum; and 
##    ⧐ stg_course_unit contains the baseline artifact that we want to modify.
## Ⓑ Deconvolve the stg_course_unit table into two components:
##    ⧐ course_blueprint contains patterns of instructional-unit, learning-standard scope common to 
##      multiple, distinct courses; and
##    ⧐ course_blueprint_bridge associates individual buleprints to the courses for which they apply.
##    We employ pre-existing logic from a distinct piece of logic to accomplish this.  This is instantiated as 
##    a function.
## Ⓒ Identify and classify measured (sih_coursepk_id, learning_standard_id) tuples from the assessed-curriculum algmt_detail
##    that don't appear in stg_course_unit. 
## Ⓓ Concatenate the appropriately-classified (sih_coursepk_id, learning_standard_id) tuples to the course_blueprints corresponding
##    to the respective ccourses.
##
##################################################################################################################################
#🤓😎🤡👻👽🧐🤓😎🤡👻👽🧐🤓😎🤡👻👽🧐🤓😎🤡👻👽🧐🤓😎🤡👻👽🧐🤓😎🤡👻👽🧐🤓😎🤡👻👽🧐🤓😎🤡👻👽🧐🤓🐵🙈👽🙉🙊🐒🐍#
##################⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇ LOCAL FUNCTIONS ⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇##############
def course_unit_deconvolution(stg_course_unit):
## PURPOSE:  DECONVOLVE COURSE_UNIT TABLE INTO A COURSE-BLUEPRINT BRIDGE TABLE AND A COURSE-BLUEPRINT TABLE.
##           The full course-unit table results from joining the course blueprint onto a bridge table. 
##           A course blueprint — a policy statement about the intended curriculum — can apply to multiple
##           courses.  We separate it so that the blueprint can be defined once and subsequently mapped to each distinct
##           course to which it is applied.
##
##           We use the term "deconvolve" to denote reversing the join operation.  We separate out the course-invariant attributes
##           from the course blueprint — that is invariant for instances in which distinct courses share a common blueprint —
##           from those that are course-distinct.  We hash the course-invariant attributes, so that we can recognize their 
##           recurrences. We use this has function as the foreign key by which associate courses with their blueprints.
# Ⓑ Separate the course-unit table.
#    ① Concatenate the course-distinct attributes into a single attribute and set it as the dataframe index.
	stg_course_unit = stg_course_unit_query(session_attributes, tenant_config, work_dir)
	stg_course_unit = stg_course_unit.assign(course_distinct = stg_course_unit[['COURSE_TITLE','COURSE_ID','SIH_COURSEPK_ID']]\
																			.astype(str)\
																			.apply(lambda x: '_'.join(x), axis = 1))\
									  .set_index(keys = 'course_distinct',
												 drop = False)

#
#    ② Calculate the hash for the course-invariant components for each index in the dataframe.  We use the hash_pandas_object
#       function. The tricky part is to eradicate any course-distinct attributes. We must reset the index to accomplish this.
#       We collect a list of course-index, blueprint-hash tuples into a dataframe, which we join back onto our stg_course_unit.
	stg_course_unit = pd.merge(left = stg_course_unit.reset_index(drop = True),
							   right = pd.DataFrame(data = [(course_key, hash_pandas_object(stg_course_unit.loc[course_key, 
																												['UNIT_NM',
																												'LEARNING_STANDARD_CD',
																												'SUBJECT',
																												'GRADE_LEVEL',
																												'UNIT_START_DT',
																												'UNIT_FINISH_DT',
																												'LEARNING_STANDARD_ID',
																												'TENANT_ID']]\
																										.reset_index(drop = True)).sum())
															for course_key in list(sorted(set(stg_course_unit.index)))],
												columns = ['course_distinct','blueprint_hash']))
#
#    ③ We now separate out the bridge and the course-unit attributes. We simply partition the stg_course_unit dataframe
#       into course-distinct and course-invariant components, dropping duplicates in each partition.
	course_blueprint_bridge = stg_course_unit[['COURSE_ID',
											'COURSE_TITLE',
											'SIH_COURSEPK_ID',
											'blueprint_hash']].drop_duplicates()

	course_blueprint = stg_course_unit[['UNIT_NM',
										'LEARNING_STANDARD_CD',
										'SUBJECT',
										'GRADE_LEVEL',
										'UNIT_START_DT',
										'UNIT_FINISH_DT',
										'LEARNING_STANDARD_ID',
										'TENANT_ID',
										'blueprint_hash']].drop_duplicates()
#
##################################################################################################################################
#🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛#
##################⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇ MARCH 19, 2019 LOGIC FIX ① ⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇###############
# We fix here the assignment of the UNIT_NUMBER in the course blueprint.  Our course blueprints may contain
# units for learning standards that were measured but not prescribed.  These appear in a unit titled
# "Assessed Not in Blueprint".  We want to number other course units first. Then we assign to 
# "Assessed Not in Blueprint" the UNIT_NUMBER incremented by one from all of the prescribed units.
# We do this by partitioning course_blueprint into prescribed and unprescribed course units through
# recognizing the preceeding condition. We assign integer-increasing unit numbers for prescribed 
# units first.
# ① First look for pre-defined UNIT_NUMBER vales.  These attributes n the IBMSIH.COURSE_LEARNING_STANDARD_MAP table
#    from which the stg_course_unit is derived are prepended into the UNIT_NM attribute.  We
#    look a the leading three-character substring of UNIT_NM for a UNIT_NUMBER.  If this leading substring
#    is a number we take that as the UNIT_NUMBER value.  Otherwise, we assign placeholder value "unnumbered",
#    to be updated later.
	course_blueprint = pd.merge(right = pd.DataFrame(data =  [(unit_nm,
															   unit_nm[:3].strip(), 
															   unit_nm[3:].strip(), 
															   bp_hash) if unit_nm[:3].strip().isdigit()
															 else (unit_nm,
																   'unnumbered', 
																   unit_nm, 
																   bp_hash)
																for (unit_nm, bp_hash) in course_blueprint[['UNIT_NM',
																											'blueprint_hash']]\
																										  .drop_duplicates()\
																										  .to_records(index = False)\
																										  .tolist() ],
													columns = ['UNIT_NM',
															   'UNIT_NUMBER',
															   'UNIT_NM_split',
															   'blueprint_hash']),
								left = course_blueprint ).drop(labels = 'UNIT_NM',
															   axis = 1)\
														 .rename(columns = {'UNIT_NM_split' : 'UNIT_NM'})\
														 .sort_values(by = ['blueprint_hash',
																			'UNIT_NUMBER',
																			'UNIT_START_DT',
																			'UNIT_NM'])
#
# ② We now partition course_bluperint into prescribed units and nonprescribed_units. The former are defined — with their
#    constituent learning standards — as part of curriculum policy.  The latter previously-identified 
#    course/learning-standard measurements that are prescribed by policy, but for which measurements have been
#    received.  Our convention holds that unprescribed courese/learning-standard measurements are assigned to a unit
#    named "Assessed Not in Blueprint".
	blueprint_partition = {'prescribed_units' : course_blueprint.loc[[not bool(re.search('Assessed Not in Blueprint', unit_nm)) 
																		for unit_nm in course_blueprint['UNIT_NM']]],
						   'nonprescribed_units' : course_blueprint.loc[[bool(re.search('Assessed Not in Blueprint', unit_nm)) 
																		for unit_nm in course_blueprint['UNIT_NM']]] }
#
# ③ Assign the UNIT_NUMBER value. 
#    ⓐ For prescribed_units we construct an unit-incrementally-increasing integer index for
#       each (blueprint_hash, UNIT_NM) couple. Because of the manner in which course_blueprint was previously sorted,
#       our value coincides with the pre-existing UNIT_NUMBER.  We specifically sorted according to blueprint_hash, UNIT_NUMBER, UNIT_START_DT,
#       UNIT_NM.  If UNIT_NUMBER is therefore "unassigned" for all, our integral order is UNIT_START_DT, UNIT_NM
	blueprint_partition.update({'prescribed_units' : pd.merge(left = blueprint_partition.get('prescribed_units').drop(labels = 'UNIT_NUMBER',
																													  axis = 1),
															  right =  pd.concat([val.assign(blueprint_hash = key)
																					for (key, val) in blueprint_partition.get('prescribed_units')\
																										[['UNIT_NM','blueprint_hash']]\
																										.drop_duplicates()\
																										.groupby(by = 'blueprint_hash')['UNIT_NM']\
																										.aggregate(lambda x : pd.DataFrame(data = list(zip(map(lambda x: x+1, range(len(set(x)))),
																																						   sorted(set(x)))),
																																			columns = ['UNIT_NUMBER', 'UNIT_NM'])  )\
																										.to_dict().items()]))\
															[['UNIT_NUMBER', 
															  'UNIT_NM',
															  'LEARNING_STANDARD_CD',
															  'SUBJECT',
															  'GRADE_LEVEL',
															  'UNIT_START_DT',
															  'UNIT_FINISH_DT',
															  'LEARNING_STANDARD_ID',
															  'TENANT_ID',
															  'blueprint_hash']]    })
#
#    ⓑ We assign the UNIT_NUMBER for unprescribed units — titled "Assessed Not in Blueprint" — as a unit-increment to the maximum-value
#       UNIT_NUMBER for all prescribed units.
	blueprint_partition.update({'nonprescribed_units' : pd.merge(left = blueprint_partition.get('nonprescribed_units').drop(labels = 'UNIT_NUMBER',
																														  axis = 1),
																right =  blueprint_partition.get('prescribed_units')[['blueprint_hash',
																													  'UNIT_NUMBER']]\
																							.groupby(by = 'blueprint_hash',
																									 as_index = False)\
																							.agg(lambda x : max(x) + 1))\
															[['UNIT_NUMBER', 
															  'UNIT_NM',
															  'LEARNING_STANDARD_CD',
															  'SUBJECT',
															  'GRADE_LEVEL',
															  'UNIT_START_DT',
															  'UNIT_FINISH_DT',
															  'LEARNING_STANDARD_ID',
															  'TENANT_ID',
															  'blueprint_hash']]    })
#
# ④ Concatenate the partitions.  In the process of concatenation, reconcile the UNIT_NUMBER and GRADE_LEVEL attributes
#    to conform to specifications. Specifically, these attributes must be fixed-width strings with leading zeros.  UNIT_NUMBER
#    is three characters in width, and GRADE_LEVEL two.
	course_blueprint = pd.concat(objs = [partition.assign(UNIT_NUMBER = [('00' + str(unit_no))[-3:]
																			for unit_no in partition['UNIT_NUMBER']],
														  GRADE_LEVEL = [('00' + str(grd_lvl))[-2:]
																			for grd_lvl in partition['GRADE_LEVEL']]) 
										 for partition in blueprint_partition.values()],
								 sort = True)
#
##################⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆ MARCH 19, 2019 LOGIC FIX ① ⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆##############
#🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛#
###################################################################################################################################
#
#    ⑤ We now synthesize a blueprint title for each distinct blueprint. We want our title to be a concatenation of subject,
#       grade, and an integer index for each distinct blueprint corresponding to (subject, grade) pairs. This is somewhat
#       complicated because individual blueprints can span multiple points in the (subject, grade) space. So, we accomplish this
#       in two steps, each using the pandas groupby feature.
#       ⓐ First, condstruct a root for the blueprint name based on its subject, grade scope.
#		   Begin by identifying the (subject, grade)-space span for each blueprint, distinguished by its blueprint_hash. 
#          Our pandas groupby aggregates using a concatenation operation realized by a lambda operator. We truncate
#          to five letters the subject-dimension variable for character-length constraints. We then concatenate together
#          our subject and grade strings.  To illustrate, a blueprint spanning (Math, Language Arts), (4, 5) in the
#          subject and grade spaces, respectively, would have a blueprint name root 'Math_Lang_4_5'.  We end up with
#          a bridge table associating each blueprint_hash with a name_root corresponding to its span.
	blueprint_hash_name_root =  pd.concat(
					objs = [blueprint_bridge.assign(name_root = ['_'.join((subj, grade)) for (subj, grade) in blueprint_bridge[['SUBJECT',
																														   'GRADE_LEVEL']]\
																														.to_records(index = False)\
																														.tolist()])\
										.drop(labels = ['SUBJECT',
														'GRADE_LEVEL'],
												axis = 1)\
										.sort_values(by = 'name_root')
							 for blueprint_bridge in [course_blueprint[['blueprint_hash', 'SUBJECT', 'GRADE_LEVEL']]\
																.assign(SUBJECT = [subj[:4] for subj in course_blueprint['SUBJECT']])\
																.drop_duplicates()\
																.groupby(by = 'blueprint_hash',
																		as_index = False)\
																		.agg(lambda x : '_'.join(set(x[:5])).replace(' ','') )]],
					sort = True)
#
#       ⓑ Next, construct the blueprint names themselves by concatenating an integer index onto each blueprint_hash-distinct
#          name_root.  The logic somewhat resembles that above, only this time we groupby the hash_list and list-aggregate.
#          This gives us for each name root a list of corresponding blueprint_hash instances. We have to associate each of
#          these with an index. We construct a list of integer indices based on the length of the list of hashes. We use
#          the built-in zip function to get (blueprint_hash, name_idx) tuples. We then concatenate the name_idx onto the
#          the name root. We assemble our results into a dataframe that can be joined onto our course_blueprint and 
#          our course_blueprint_bridge using the blueprint_hash as a join key.
	blueprint_hash_name = pd.DataFrame(
					data = list(it.chain(*[
											[(bp_hash, bp_name + '_bp_' + str(name_idx))  
											for (bp_hash, name_idx) in 
											list(zip(hash_list,
													[name_idx + 1 for name_idx in range(len(hash_list))])) ]
								for (bp_name, hash_list ) in 
								blueprint_hash_name_root.groupby(by = 'name_root',
																 as_index  = False)\
														.agg(list)\
														.to_records(index = False)\
														.tolist()])),
					columns = ['blueprint_hash',
								'BLUEPRINT_TITLE'])
#
##################################################################################################################################
#🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛#
##################⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇ MARCH 15, 2019 LOGIC FIX ② ⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇###############
# LOGIC BLOCK NO LONGER REQUIRED.  Accomplished above in MARCH 15, 2019 LOGIC FIX ①.
#     🤪 DATA CLEANUP.  We get the stg_course_unit from production.  The load process in to the production database prepends
#        the unit number off of unit name.  We want to strip it off, if it is detected.  We do this by conditional text-string
#        manipulation to produce a bridge table between the production version of UNIT_NM and the truncated — with the
#        prepended unit number removed. We join the result back onto course_blueprint and drop the original. 
	# course_blueprint = pd.merge(left = course_blueprint.rename(columns = {'UNIT_NM' : 'ORIG_UNIT_NM'}),
	# 							right = pd.DataFrame(data = [(unit_nm.split(' ')[0], unit_nm.split(' ')[0][0:1], unit_nm, ' '.join(unit_nm.split(' ')[1:]), unit_nm.split(' ')[0][0:1].isdigit())
	# 														if unit_nm.split(' ')[0][0:1].isdigit()
	# 														else (unit_nm.split(' ')[0], unit_nm.split(' ')[0][0:1], unit_nm, unit_nm, unit_nm.split(' ')[0][0:1].isdigit())
	# 														for unit_nm in set(course_blueprint['UNIT_NM'])],
	# 												columns = ['first_substr',
	# 															'first_char',
	# 															'ORIG_UNIT_NM',
	# 															'UNIT_NM',
	# 															'condition'])[['ORIG_UNIT_NM',
	# 																			'UNIT_NM']]).drop(labels = 'ORIG_UNIT_NM',
	# 																								axis = 1)

#
##################⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆ MARCH 15, 2019 LOGIC FIX ② ⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆##############
#🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛#
###################################################################################################################################
#
#     ⑥ We finally join our blueprint title back onto the course_blueprint_bridge dataframe.  Drop the blueprint_hash attribute.
	course_blueprint_bridge = pd.merge(left = course_blueprint_bridge,
									   right = blueprint_hash_name)\
								.drop(labels = ['blueprint_hash',
												'SIH_COURSEPK_ID'],
									  axis = 1)
#
#     ⑦ Drop blueprint_hash, blueprint_idx attributes from course_blueprint.
	course_blueprint = pd.merge(left = course_blueprint,
								right = blueprint_hash_name)\
							.assign(SIH_COURSEPK_ID = '')\
							.drop(labels = 'blueprint_hash',
								  axis = 1)[['BLUEPRINT_TITLE',
											 'UNIT_NUMBER',
											 'UNIT_NM',
											 'LEARNING_STANDARD_CD',
											 'SUBJECT',
											 'GRADE_LEVEL',
											 'UNIT_START_DT',
											 'UNIT_FINISH_DT',
											 'SIH_COURSEPK_ID',
											 'LEARNING_STANDARD_ID',
											 'TENANT_ID']]\
							.sort_values(by = ['BLUEPRINT_TITLE',
											   'UNIT_NUMBER',
											   'LEARNING_STANDARD_CD'])
#
	return {'STG_COURSE_UNIT_BLUEPRINT' : course_blueprint.drop_duplicates(),
			'COURSE_UNIT_BRIDGE' : course_blueprint_bridge.drop_duplicates()}
#
#
def derive_tenant_config(tenant_config_dir_args, work_dir, session_attributes):
	tenant_config = pd.DataFrame(data = list(create_engine(URL(**tenant_config_dir_args)).execute(text(' '.join([sql_line 
																				for sql_line in open(file = os.path.abspath(os.path.join(work_dir, 
																									'TENANT_CONFIG.sql')) )]))) ),
							columns = ['host',
										'port',
										'database',
										'username',
										'password',
										'tenant_id'])\
						.assign(drivername = 'db2+ibm_db')\
						.set_index(keys = 'tenant_id',
									drop = True)\
						.to_dict(orient = 'index').get(session_attributes.get('TENANT_ID'))

	tenant_config.update({'port' : 50000})
	return tenant_config



def sst_query(session_attributes, tenant_config, work_dir):
	return pd.DataFrame(data = list(create_engine(URL(**tenant_config)).execute(text(' '.join([sql_line 
																										for sql_line in open(file = os.path.abspath(os.path.join(work_dir, 
																															'STUDENT_STANDARDIZED_TEST.sql')) )])),
																						tenant_id = session_attributes.get('TENANT_ID')      )),
										columns = ['STUDENT_ID',
													'SUBJECT',
													'LEARNING_STANDARD_CD',
													'TEST_NAME',
													'MEASUREMENT_APPROACH',
													'MEAS_EVIDENCE',
													'TEST_DATE',
													'DIST_QTR_NAME',
													'GRADE',
													'LANGUAGE',
													'TEST_VERSION',
													'BRAILLE',
													'TENANT_ID',
													'LEARNING_STANDARD_ID',
													'MAX_RAW_SCORE',
													'RAW_SCORE',
													'WORK_PRODUCT_TITLE']).astype(str)


def prof_span_query(session_attributes, tenant_config, work_dir):
	proficiency_span = pd.DataFrame(data = list(create_engine(URL(**tenant_config)).execute(text(' '.join([sql_line 
																										for sql_line in open(file = os.path.abspath(os.path.join(work_dir, 
																															'CAMPUS_COURSE_SECTION_PROFICIENCY_SPAN.sql')) )])),
																						tenant_id = session_attributes.get('TENANT_ID')      )),
							columns = ['CAMPUS',
										'COURSE_SUBJECTS',
										'COURSE_GRADE_LVL',
										'COURSE_ID',
										'COURSE_TITLE',
										'COURSE_SECTION_TITLE',
										'TEACHER_LNAME',
										'TEACHER_FNAME',
										'GRADE_ENROLLED',
										'STUDENT_ID',
										'STUDENT_LNAME',
										'STUDENT_FNAME',
										'STANDARD_SUBJECT',
										'STANDARD_GRADE_LVL',
										'LEARNING_STANDARD_CD',
										'BLUEPRINT_ALGNED_LRN_STD',
										'PROG_ALGNED_LRN_STD',
										'SIH_PERSONPK_ID',
										'LEARNING_STANDARD_ID',
										'SIH_COURSEPK_ID',
										'COURSE_SECTION_SID'])\
						.drop_duplicates()\
						.set_index(keys = 'COURSE_SUBJECTS',
									drop = False)
	return proficiency_span.assign(STANDARD_GRADE_LVL = ['0' + std_grade.strip() 
																if (std_grade.strip() in list(map(str, range(9))))
																else std_grade.strip()
																for std_grade in ['00' if std_grade.strip() == 'K'
																				else std_grade
																				for std_grade in proficiency_span['STANDARD_GRADE_LVL'] ] ]).astype(str)

def lrn_std_id_cd_query(session_attributes, tenant_config, work_dir):
	return [std_cd_id_bridge.assign(GRADE_LEVEL =  ['0' + std_grade.strip() 
															if (std_grade.strip() in list(map(str, range(9))))
															else std_grade.strip()
															for std_grade in ['00' if std_grade.strip() == 'K'
																				else std_grade
																				for std_grade in std_cd_id_bridge['GRADE_LEVEL'] ] ] )
					for std_cd_id_bridge in [pd.DataFrame(data = list(create_engine(URL(**tenant_config)).execute(text(' '.join([sql_line 
																																for sql_line in open(file = os.path.abspath(os.path.join(work_dir, 
																																											'LRN_STD_ID_CD.sql')) )])),
																																	 juris_id = session_attributes.get('JURISDICTION_ID')       )),
														columns = ['SUBJECT_TITLE',
																	'GRADE_LEVEL',
																	'LEARNING_STANDARD_ID',
																	'LEARNING_STANDARD_CD',
																	'MEASUREABLE_STANDARD_YN',
																	'LEARNING_STANDARD_TITLE'])]][0]\
					.sort_values(by = ['SUBJECT_TITLE',
										'GRADE_LEVEL',
										'LEARNING_STANDARD_ID'],
								axis = 0)\
					.astype(str)\
					.reset_index(drop = True)

def enrollment_query(session_attributes, tenant_config, work_dir): 
	return pd.DataFrame(data = list(create_engine(URL(**tenant_config)).execute(text(' '.join([sql_line 
																										for sql_line in open(file = os.path.abspath(os.path.join(work_dir, 
																															'STUDENT_CAMPUS_ENROLLMT.sql')) )])),
																						tenant_id = session_attributes.get('TENANT_ID')      )),
									columns = ['CAMPUS',
												'STUDENT_ID'])\
							.drop_duplicates()\
							.astype(str)
#
##################################################################################################################################
#🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛#
##################⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇ MARCH 15, 2019 LOGIC FIX ④ ⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇###############
# The encoding of the learning-standard grade level — employed as a join key — does not conform to a standard convention.
# We want the GRADE_LEVEL for the 𝘕ᵗʰ grade to appear as "0𝘕". The query reutnrs a value formatted as "𝘕 ". We fix that 
# here.  We transform the provided GRADE_LEVEL — specifically the STANDARD_GRADE_LVL attribute — cross-mapping data frame.
# We join-substitute the non-conforming attribute to one that conforms to our specification.
def evid_algmt_query(session_attributes, tenant_config, work_dir):
	evid_algmt = pd.DataFrame(data = list(create_engine(URL(**tenant_config)).execute(text(' '.join([sql_line 
																							   for sql_line in open(file = os.path.abspath(os.path.join(work_dir, 
																														   'ALGMT_ANALYSIS—BODY_OF_EVIDENCE_ALIGNMENT_DETAIL.sql')) )])),
																				 tenant_id = session_attributes.get('TENANT_ID')      )),
							columns = ['CAMPUS',
										'COURSE_SUBJECTS',
										'COURSE_GRADE_LVL',
										'COURSE_ID',
										'COURSE_TITLE',
										'COURSE_SECTION_TITLE',
										'TEACHER_LNAME',
										'TEACHER_FNAME',
										'GRADE_ENROLLED',
										'STUDENT_ID',
										'STUDENT_LNAME',
										'STUDENT_FNAME',
										'STANDARD_SUBJECT',
										'STANDARD_GRADE_LVL',
										'LEARNING_STANDARD_CD',
										'WORK_PRODUCT_TITLE',
										'RAW_SCORE',
										'ASSESSMENT_DATE',
										'AY_OF_MEASUREMENT',
										'LEARNING_STANDARD_AY',	
										'COURSE_STANDARD_ALIGNMENT',
										'MEASUREABLE_STANDARD_YN',
										'SIH_COURSEPK_ID',
										'COURSE_SECTION_SID',
										'SIH_PERSONPK_ID',
										'LEARNING_STANDARD_ID',
										'DATE_MEASUREMENT_LOADED',
										'EVID_TUPLE']).astype(str)\
													  .set_index(keys = 'AY_OF_MEASUREMENT',
																 drop = False)\
													  .loc['CURRENT_AY_MEASURMENT']\
													  .reset_index(drop = True)
	return pd.merge(left = evid_algmt.rename(columns = {'STANDARD_GRADE_LVL' : 'grd_lvl_nonconforming'}),
					right = pd.DataFrame(data = [(grd_lvl, ('00' + grd_lvl.strip())[-2:])
													for grd_lvl in set(evid_algmt['STANDARD_GRADE_LVL'])],
										columns = ['grd_lvl_nonconforming',
												   'STANDARD_GRADE_LVL']))\
							.drop(labels = 'grd_lvl_nonconforming',
								  axis = 1)
#
##################⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆ MARCH 15, 2019 LOGIC FIX ④ ⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆##############
#🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛#
###################################################################################################################################
#
def course_cat_query(session_attributes, tenant_config, work_dir):
	return pd.DataFrame(data = list(create_engine(URL(**tenant_config)).execute(text(' '.join(
																				[sql_line 
																				for sql_line in open(file = os.path.abspath(os.path.join(work_dir, 
																														'REGISTERED_COURSE_CATALOGUE.sql')) )])),
																				tenant_id =  session_attributes.get('TENANT_ID'))),
								columns = ['SIH_COURSEPK_ID',
											'COURSE_ID',
											'COURSE_TITLE']).astype(str)

def stg_course_unit_query(session_attributes, tenant_config, work_dir):
	return pd.DataFrame(data = list(create_engine(URL(**tenant_config)).execute(text(' '.join(
																				[sql_line 
																				for sql_line in open(file = os.path.abspath(os.path.join(work_dir, 
																														'STG_COURSE_UNIT.sql')) )])),
																				tenant_id =  session_attributes.get('TENANT_ID'))),
								columns = ['COURSE_ID',
											'COURSE_TITLE',
											'UNIT_NM',
											'LEARNING_STANDARD_CD',
											'SUBJECT',
											'GRADE_LEVEL',
											'UNIT_START_DT',
											'UNIT_FINISH_DT',
											'SIH_COURSEPK_ID',
											'LEARNING_STANDARD_ID',
											'TENANT_ID']).astype(str)

def acad_yr_start_end_query(session_attributes, tenant_config, work_dir):
	return pd.DataFrame(data = list(create_engine(URL(**tenant_config)).execute(text(' '.join(
																				[sql_line 
																				for sql_line in open(file = os.path.abspath(os.path.join(work_dir, 
																														'ACADEMIC_YEAR_LOOKUP.sql')) )])),
																				tenant_id =  session_attributes.get('TENANT_ID'))),
								columns = ['ACADEMIC_YEAR',
											'ACADEMIC_YEAR_START',
											'ACADMEMIC_YEAR_END']).astype(str)

#    We partion student_standardized_test according to three categories.
#    ⓐ With explicitly-aligned measurements, (student, learning_standard) tuples from student_standardized_test have exact correspondents 
#       have exact correspondents in proficiency_span.  In set-relation notation, 
#                 explicitly_algnd_meas prof_span(student, learning_standard) student_standardized_test(student, learning_standard).
#       We further partition explicitly_algnd_meas into blueprint_aligned and otherwise progression_aligned categories. These
#       partitions distinguish between current-course learning standards in the course blueprints and their graphical
#       prerequisites in the learning-standard progressions, respectively.
#    ⓑ Possibly-inscope measurements from student_standardized_test records are not explicitly-aligned. These are measurements
#       not in ⓐ, but are within the subject × grade_level span of the graphical neighborhood for the course.
#       This appears set-relation notation as 
#                possibly_inscope prof_span(student, subject, grade_level) student_standardized_test(student, subject, grade_level)
#    ⓒ unattributable — unattributable — measurements are the set complement of ⓐ∪ⓑ, as defined above. These are measurements that cannot be associated
#       with a course-graphical neighborhood for a course in which the measured student is enrolled.
#
def student_standardized_test_partition(student_standardized_test, proficiency_span):
	sst_partition = dict()
	sst_partition.update({'explicitly_aligned' : pd.merge(left = student_standardized_test.drop_duplicates()\
																				.reset_index(drop = True),
													right = proficiency_span[['STUDENT_ID',
																			'LEARNING_STANDARD_ID']].drop_duplicates()\
																									.reset_index(drop = True))\
																									[student_standardized_test.columns]   })
	sst_partition.update({'possibly_inscope' : pd.merge(left = pd.DataFrame(data = list(set(student_standardized_test.drop_duplicates()\
																												.to_records(index = False)\
																												.tolist()) -\
																					set(sst_partition.get('explicitly_aligned')[student_standardized_test.columns]\
																															.to_records(index = False)\
																															.tolist())  ),
																		columns = student_standardized_test.columns),
													right = proficiency_span[['STUDENT_ID',
																			'STANDARD_SUBJECT',
																			'STANDARD_GRADE_LVL']].drop_duplicates()\
																								.astype(str)\
																								.rename(columns = {'STANDARD_SUBJECT' : 'SUBJECT',
																													'STANDARD_GRADE_LVL' : 'GRADE'}) )\
																								[student_standardized_test.columns]  })
	sst_partition.update({'unattributable' : pd.DataFrame(data = list(set(student_standardized_test.drop_duplicates()\
																								.to_records(index = False)\
																								.tolist()) -\
																	set().union(*[partition[student_standardized_test.columns].to_records(index = False).tolist() 
																					for partition in list(sst_partition.values())])),
														columns = student_standardized_test.columns)    })
	return sst_partition

#    ⓶ We now do campus×course alignment analysis. This is accomplished by again joining the student_standardized_test
#       partitions in the sst_alignmt dictionary with appropriate subsets of proficiency_span.  That is the case at least with 
#       explicitly_aligned and possibly_inscope partitions. With unaligned cases, we must construct our alignment picture and 
#       concatenate on the others.
#
#       In each case we take column slices of our partitioned student_standardized_test dataframes, incorporate 
#       campus, course, alignment logical attributes, and then perform set-aggregation grouping.
#       
#       ⓐ Introduce alignment indicators for each partition.
#          ⅰ. With explicitly_aligned partition we get our grouping attributes directly by joining onto 
#             proficiency_span by student_id, learning_standard_id.  Proficiency_span contains the
#             logical alignment flags — BLUEPRINT_ALGNED_LRN_STD, PROG_ALGNED_LRN_STD — for each case
#             directly.
def sst_alignment_analysis(sst_partition, proficiency_span, stud_campus_enrolmt):
	campus_course_algmt = dict()
	campus_course_algmt.update({'explicitly_aligned' :  pd.merge(left = sst_partition.get('explicitly_aligned')[['STUDENT_ID',
																											'LEARNING_STANDARD_ID',
																											'LEARNING_STANDARD_CD',
																											'WORK_PRODUCT_TITLE']],
															right = proficiency_span[['CAMPUS',
																						'SIH_COURSEPK_ID',
																						'STUDENT_ID',
																						'LEARNING_STANDARD_ID',
																						'BLUEPRINT_ALGNED_LRN_STD',
																						'PROG_ALGNED_LRN_STD']]\
																					.drop_duplicates()\
																					.astype(str))\
																.drop(labels = ['STUDENT_ID',
																				'LEARNING_STANDARD_ID'],
																		axis = 1)\
																.drop_duplicates()\
																.groupby(by = ['CAMPUS',
																				'SIH_COURSEPK_ID',
																				'BLUEPRINT_ALGNED_LRN_STD',
																				'PROG_ALGNED_LRN_STD'],
																		as_index = False)\
																.agg(set)     })
#       
#          ⅱ. With possibly_inscope we get campus × course attributes by joining on the student_ID × standard_subject × standard_grade_lvl
#             tuples. We assign our logical alignment-indicator flags  — BLUEPRINT_ALGNED_LRN_STD, PROG_ALGNED_LRN_STD — 
#             in the negative case 'N'.
	campus_course_algmt.update({'possibly_inscope' : pd.merge(left = sst_partition.get('possibly_inscope')[['STUDENT_ID',
																										'SUBJECT',
																										'GRADE',
																										'LEARNING_STANDARD_CD',
																										'WORK_PRODUCT_TITLE']],
															right = proficiency_span[['CAMPUS',
																						'SIH_COURSEPK_ID',
																						'STUDENT_ID',
																						'STANDARD_SUBJECT',
																						'STANDARD_GRADE_LVL']]\
																	.rename(columns = {'STANDARD_SUBJECT' : 'SUBJECT',
																						'STANDARD_GRADE_LVL' : 'GRADE'}))\
																	.drop(labels = ['STUDENT_ID',
																					'SUBJECT',
																					'GRADE'],
																			axis = 1)\
																	.drop_duplicates()\
																	.groupby(by = ['CAMPUS',
																					'SIH_COURSEPK_ID'],
																			as_index = False)\
																	.agg(set)\
																	.assign(BLUEPRINT_ALGNED_LRN_STD = 'N')\
																	.assign(PROG_ALGNED_LRN_STD = 'N')   })
#       
#          ⅲ. With unattributable, associating a student_id with a campus is the best we can do. Our query stud_campus_enrolmt
#             provides student-enrollment by campus.  We join this onto our student_standardized_test partition. 
#             We construct a proxy for SIH_COURSEPK_ID, our synthetic key for course_id, by concatenating SUBJECT and GRADE
#             values. We assign our logical alignment-indicator flags  — BLUEPRINT_ALGNED_LRN_STD, PROG_ALGNED_LRN_STD — 
#             as the negative for each. 
	campus_course_algmt.update({'unattributable' : pd.merge(left = sst_partition.get('unattributable')[['STUDENT_ID',
																									'LEARNING_STANDARD_CD',
																									'WORK_PRODUCT_TITLE']]\
																									.assign(SIH_COURSEPK_ID = ['_'.join((subj, grade)) 
																																	for (subj, grade) 
																																	in sst_partition.get('unattributable')[['SUBJECT','GRADE']]\
																																											.to_records(index = False)\
																																											.tolist()]),
														right = stud_campus_enrolmt)\
																	.drop(labels = 'STUDENT_ID',
																			axis = 1)\
																	.drop_duplicates()\
																	.groupby(by = ['CAMPUS',
																					'SIH_COURSEPK_ID'],
																			as_index = False)\
																	.agg(set)\
																	.assign(BLUEPRINT_ALGNED_LRN_STD = 'N')\
																	.assign(PROG_ALGNED_LRN_STD = 'N')  } )
	return campus_course_algmt


def session_initialization(db_credential_dir, work_dir):
	session_attributes  = pd.read_csv(filepath_or_buffer = os.path.abspath(os.path.join(work_dir, 
																				'SESSION_ATTRIBUTES.csv')),
									dtype = str)\
									.set_index(keys = 'ATTRIBUTE',
												drop = True)\
									.to_dict(orient = 'dict').get('VALUE') 
	session_attributes.update({'work_dir' : work_dir,
							   'source_dir' : source_dir})
	tenant_config_dir_args = pd.read_csv(filepath_or_buffer = os.path.abspath(os.path.join(db_credential_dir, 
																				'TENANT_CONFIG_QUERY_SPECS.csv')),
									dtype = str)\
									.set_index(keys = 'ATTRIBUTE',
												drop = True)\
									.to_dict(orient = 'dict').get('VALUE')
	sftp_login_params = pd.read_csv(filepath_or_buffer = os.path.abspath(os.path.join(db_credential_dir, 
																				'SFTP_LOGIN_PARAMS.csv')))\
									.set_index(keys = 'ATTRIBUTE',
												drop = True)\
									.to_dict(orient = 'dict')\
									.get('VALUE')
	execution_time = datetime.utcnow().strftime('%y%m%d_%H%MZ_')
	if not pd.isnull(session_attributes.get('benchmark_time')):
		benchmark_time = session_attributes.get('benchmark_time')
	else: 
		benchmark_time = execution_time
	session_attributes.update({'benchmark_time' : benchmark_time})
	

	tenant_config = derive_tenant_config(tenant_config_dir_args, work_dir, session_attributes)
	# pysftp.Connection(**sftp_login_params).makedirs(session_attributes.get('sftp_destination'))

	return {'tenant_config' : tenant_config,
			'session_attributes' :  session_attributes,
			'sftp_login_params' : sftp_login_params,
			'benchmark_time' : benchmark_time} 

#
##################⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆ LOCAL FUNCTIONS ⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆##############
#🤓😎🤡👻👽🧐🤓😎🤡👻👽🧐🤓😎🤡👻👽🧐🤓😎🤡👻👽🧐🤓😎🤡👻👽🧐🤓😎🤡👻👽🧐🤓😎🤡👻👽🧐🤓😎🤡👻👽🧐🤓🐵🙈👽🙉🙊🐒🐍#
##################################################################################################################################
#
##################################################################################################################################
#🚛⚒⛏📇🔑🔭🔬🔩⚙️🗜📡🔌🚛⚒⛏📇🔑🔭🔬🔩⚙️🗜📡🔌🚛⚒⛏📇🔑🔭🔬🔩⚙️🗜📡🔌🚛⚒⛏📇🔑🔭🔬🔩⚙️🗜📡🔌🚛⚒⛏📇🔑🔭🔬🔩⚙️🗜📡#
##################⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇ MAIN PROGRAM ⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇##############
#
# ⓪ Set up session. We use a session_initializaiton subroutine to generate essential session arguments used in subsequent
#    logic.  These are:
#    ⧐ session_attributes is a dictionary of client-configuration-specific parameters, such as sftp-directory 
#      paths and file names;
#    ⧐ tenant_config contains the attributes required for SQL queries of the production database;
#    ⧐ sftp_login_params provides attributes required for an sftp transfer session; and
#    ⧐ benchmark_time is a session UTC timestamp used appended/prepended to outputs  in order to
#      uniquely distinguish for a particular session.
session_specs = session_initialization(db_credential_dir, work_dir)
session_attributes = session_specs.get('session_attributes')
tenant_config = session_specs.get('tenant_config')
sftp_login_params = session_specs.get('sftp_login_params')
benchmark_time = session_specs.get('benchmark_time')
#

proficiency_span = prof_span_query(session_attributes, tenant_config, work_dir)[['SIH_COURSEPK_ID',
																				 'SIH_PERSONPK_ID',
																				 'LEARNING_STANDARD_ID',
																				 'COURSE_ID',
																				 'COURSE_TITLE']]\
																			   .drop_duplicates()



learner_know_state = pd.concat(objs = [pd.read_csv(filepath_or_buffer = os.path.abspath(os.path.join(session_attributes.get('source_dir'), 
																						file_name)),
												   usecols = ['SIH_PERSONPK_ID_ST',
															  'LEARNING_STANDARD_ID']).drop_duplicates()
										for file_name in os.listdir(session_attributes.get('source_dir'))
										if bool(re.search('_STUDENT_KNOWLEDGE_LEVEL.csv', file_name))],
							   sort = False).astype(str)\
											.rename(columns = {'SIH_PERSONPK_ID_ST' : 'SIH_PERSONPK_ID'})


sum(pd.merge(left = proficiency_span,
			 right = learner_know_state).drop(labels = 'LEARNING_STANDARD_ID',
											  axis = 1)\
										.drop_duplicates()\
										.assign(enrollee_count = 1)\
										.groupby(by = ['SIH_COURSEPK_ID',
													   'COURSE_ID',
													   'COURSE_TITLE'],
												 as_index = False)\
										.agg(sum)['enrollee_count'])

len(set(pd.merge(left = proficiency_span,
				 right = learner_know_state)['SIH_COURSEPK_ID']) )


cluster_exec_time = pd.concat(objs = [pd.read_csv(filepath_or_buffer = os.path.abspath(os.path.join(session_attributes.get('source_dir'), 
																		file_name))).drop_duplicates()\
												.assign(file_name = file_name.split('.')[0],
														EVID_PROF_SIG = lambda x : x['EVID_PROF_SIG'].astype(str),
														EVID_STATE_SIG = lambda x : x['EVID_STATE_SIG'].astype(str))
										for file_name in os.listdir(session_attributes.get('source_dir'))
										if bool(re.search('_CLUSTER_EXEC_TIME.csv', file_name))],
							  sort = False)


cluster_exec_time = fct.reduce(lambda x, y: pd.merge(left = x,
													 right = y),
											 [cluster_exec_time[['EVID_PROF_SIG',
																 'EVID_STATE_SIG']].assign(evid_state_count = 1)\
																				   .groupby(by = 'EVID_PROF_SIG',
																							as_index = False)\
																				   .agg(sum),
											 cluster_exec_time[['EVID_PROF_SIG',
																'BAYESNET_BUILD_TIME']]\
															   .drop_duplicates(),
											 cluster_exec_time]).assign(net_query_time = lambda x : 10**(-5) + np.asarray(x['ELAPSED_TIME'] +\
																											   np.asarray(x['BAYESNET_BUILD_TIME'])\
																											  /np.asarray(x['evid_state_count'])))\
																.sort_values(by = 'net_query_time')\
																.assign(cum_exec_time = lambda x : np.cumsum(x['net_query_time'])/3600)\
																.assign(marg_cum_increase = lambda x : np.append([0],np.diff(x['cum_exec_time'])))\
																.reset_index(drop = True)



exec_dist_plot = cluster_exec_time[['cum_exec_time']]\
								  .reset_index()\
								  .plot(kind = 'line',
									    color = '#0051ba',
									    legend = False,
									    x = 'index',
									    grid = True)
exec_dist_plot.set_title(label = 'Cumulative BayesNet Query-Execution Time', 
						 color = '#003459',
						 fontweight = 'heavy')
exec_dist_plot.set_xlabel(xlabel = 'Bayesnet Queries (counts)', 
						  color = '#003459',
						  fontsize = 'small',
						  fontweight = 'light')
exec_dist_plot.set_ylabel(ylabel = 'Execution-Time (hours)', 
						  color = '#003459',
						  fontsize = 'small',
						  fontweight = 'light')
exec_dist_plot.set_aspect(.5*max(cluster_exec_time.index)/max(cluster_exec_time['cum_exec_time']))
exec_dist_plot.grid(color = '#d7d2cb',
					linestyle = '--',
					linewidth = 1)
[val.set_color('#003459') for (key, val) in exec_dist_plot.spines.items()]
[val.set_linewidth(1.75) for (key, val) in exec_dist_plot.spines.items()]
exec_dist_plot.tick_params(axis = 'both',
						   direction='out', 
						   length=3, 
						   width=1, 
						   colors='#003459',
						   labelsize = 'x-small')
exec_dist_plot.add_patch(patches.Rectangle(xy=[0,0],
										   width = 6099,
										   height = cluster_exec_time.loc[6099,'cum_exec_time'],
										   fill = True,
										   color = '#73cbf250',
										   linestyle = '--',
										   linewidth = 0.75))
exec_dist_plot.add_patch(patches.Rectangle(xy=[6100,cluster_exec_time.loc[6100,'cum_exec_time']],
										   width = len(cluster_exec_time)-6100,
										   height = max(cluster_exec_time['cum_exec_time']) - cluster_exec_time.loc[6100,'cum_exec_time'],
										   fill = True,
										   color = '#971b2f50',
										   linestyle = '--',
										   linewidth = 0.75))
exec_dist_plot.text(s = 'First {} Bayesnet Queries: {} hours'.format(6100, round(cluster_exec_time.loc[6099,'cum_exec_time'],1)),
					x = 6000,
					y = cluster_exec_time.loc[6099,'cum_exec_time'],
					color = '#2767ff',
					ha = 'right',
					fontsize = 'xx-small',
					fontweight = 'light')
exec_dist_plot.text(s = 'Last {} Bayesnet Queries: {} hours'.format(len(cluster_exec_time)-6100, 
																	round(max(cluster_exec_time['cum_exec_time']) -\
																		   cluster_exec_time.loc[6099,'cum_exec_time'],1)),
					x = 6100,
					y = max(cluster_exec_time['cum_exec_time'])-1,
					color = '#971b2f',
					ha = 'right',
					fontsize = 'xx-small',
					fontweight = 'light',
					rotation = 90)
exec_dist_plot.text(s = 'Client:  Vancouver Public Schools',
					x = 0,
					y = max(cluster_exec_time['cum_exec_time'])-1,
					color = '#003459',
					ha = 'left',
					fontsize = 'xx-small',
					fontweight = 'light')
exec_dist_plot.text(s = 'Total Classes: {}'.format(len(set(pd.merge(left = proficiency_span,
																	right = learner_know_state)['SIH_COURSEPK_ID']) )),
					x = 0,
					y = max(cluster_exec_time['cum_exec_time'])-2,
					color = '#003459',
					ha = 'left',
					fontsize = 'xx-small',
					fontweight = 'light')
exec_dist_plot.text(s = 'Total Students: {}'.format(len(set(pd.merge(left = proficiency_span,
																	 right = learner_know_state)['SIH_PERSONPK_ID']) )),
					x = 0,
					y = max(cluster_exec_time['cum_exec_time'])-3,
					color = '#003459',
					ha = 'left',
					fontsize = 'xx-small',
					fontweight = 'light')
exec_dist_plot.text(s = 'Student-Course Enrollments: {}'.format(sum(pd.merge(left = proficiency_span,
																			 right = learner_know_state).drop(labels = 'LEARNING_STANDARD_ID',
																											  axis = 1)\
																										.drop_duplicates()\
																										.assign(enrollee_count = 1)\
																										.groupby(by = ['SIH_COURSEPK_ID',
																													   'COURSE_ID',
																													   'COURSE_TITLE'],
																												 as_index = False)\
																										.agg(sum)['enrollee_count'])),
					x = 0,
					y = max(cluster_exec_time['cum_exec_time'])-4,
					color = '#003459',
					ha = 'left',
					fontsize = 'xx-small',
					fontweight = 'light')
exec_dist_plot.text(s = 'Total Evidentiary Measurements: {}'.format(len(pd.merge(left = proficiency_span,
																				 right = learner_know_state)[['SIH_PERSONPK_ID',
																											  'LEARNING_STANDARD_ID']]\
																											.drop_duplicates()) ),
					x = 0,
					y = max(cluster_exec_time['cum_exec_time'])-5,
					color = '#003459',
					ha = 'left',
					fontsize = 'xx-small',
					fontweight = 'light')
exec_dist_plot.text(s = 'Evidentiary Profiles: {}'.format(len(set(cluster_exec_time['EVID_PROF_SIG'])) ),
					x = 0,
					y = max(cluster_exec_time['cum_exec_time'])-6,
					color = '#003459',
					ha = 'left',
					fontsize = 'xx-small',
					fontweight = 'light')
exec_dist_plot.text(s = 'Evidentiary States: {}'.format(len(set(cluster_exec_time[['EVID_STATE_SIG',
																									  'EVID_PROF_SIG',
																									  'CLUSTER']]\
																									.to_records(index = False)\
																									.tolist())) ),
					x = 0,
					y = max(cluster_exec_time['cum_exec_time'])-7,
					color = '#003459',
					ha = 'left',
					fontsize = 'xx-small',
					fontweight = 'light')
exec_dist_plot.text(s = 'Bayesnet Queries: {}'.format(len(cluster_exec_time) ),
					x = 0,
					y = max(cluster_exec_time['cum_exec_time'])-8,
					color = '#003459',
					ha = 'left',
					fontsize = 'xx-small',
					fontweight = 'light')
plt.savefig(fname = os.path.abspath(os.path.join(desktop_dir, 
												benchmark_time + 'CumQueryExecTime.png')),
			transparent = True,
			bbox_inches = 'tight',
			dpi = 256)




plt.plot([0,6099,6099],
		 [cluster_exec_time.loc[6099,'cum_exec_time'],cluster_exec_time.loc[6099,'cum_exec_time'],0],
		 linestyle = '--',
		 color = '#c66e4eaa',
		 linewidth = 0.5)

exec_dist_plot = cluster_exec_time[['marg_cum_increase']]\
								 .reset_index()\
								 .plot(kind = 'line',
									   color = '#0051ba',
									   legend = False,
									   x = 'index',
									   grid = True)
exec_dist_plot.set_title(label = 'Marginal Query-Time Increase', 
						 color = '#003459')
exec_dist_plot.set_xlabel(xlabel = 'Bayesnet Query', 
						  color = '#003459')
exec_dist_plot.set_ylabel('Execution-Time Increase (hours)', color = '#003459')
exec_dist_plot.set_aspect(.5*max(cluster_exec_time.index)/max(cluster_exec_time['marg_cum_increase']))
exec_dist_plot.grid(color = '#d7d2cb',
					linestyle = '--',
					linewidth = 1)
[val.set_color('#003459') for (key, val) in exec_dist_plot.spines.items()]
[val.set_linewidth(1.75) for (key, val) in exec_dist_plot.spines.items()]
exec_dist_plot.tick_params(axis = 'both',
						   direction='out', 
						   length=3, 
						   width=1, 
						   colors='#003459')
plt.show()


[(key, val) for (key, val) in exec_dist_plot.spines.items()]



## ‼️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️‼️#		
## ‼️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️‼️#		






#
##################⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆ MAIN PROGRAM ⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆##############
#🚛⚒⛏📇🔑🔭🔬🔩⚙️🗜📡🔌🚛⚒⛏📇🔑🔭🔬🔩⚙️🗜📡🔌🚛⚒⛏📇🔑🔭🔬🔩⚙️🗜📡🔌🚛⚒⛏📇🔑🔭🔬🔩⚙️🗜📡🔌🚛⚒⛏📇🔑🔭🔬🔩⚙️🗜📡#
##################################################################################################################################

##################################################################################################################################
#🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛#
##################⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇ MARCH 13, 2019 LOGIC FIX ⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇###############

#
##################⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆ MARCH 13, 2019 LOGIC FIX ⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆##############
#🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛#
###################################################################################################################################



## ‼️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️‼️#		
## ‼️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️‼️#		
## ‼️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️‼️#		
## ‼️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️‼️#		




## ‼️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️‼️#		
## ‼️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️‼️#		
## ‼️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️‼️#		
## ‼️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️‼️#		
#
## ‼️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️‼️#		



## ‼️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️‼️#		


