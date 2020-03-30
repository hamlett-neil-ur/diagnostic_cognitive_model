# coding: utf-8
import os
import pandas as pd
import numpy as np
import math as mt
import csv
import subprocess
import networkx as nx
from datetime import datetime
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
#
work_dir = '/Users/nahamlet@us.ibm.com/Box Sync/IBM Watson Education Delivery/Data/Partner/Mastery Run Status/MEAS_NOT_SPEC_BLUEPRINT_EXTENSION'
db_credential_dir = '/Users/nahamlet@us.ibm.com/Documents/Documents/Oportunidades actuales'
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
# ② We now partition course_bluperint into prescribed units and unprescribed_units. The former are defined — with their
#    constituent learning standards — as part of curriculum policy.  The latter previously-identified 
#    course/learning-standard measurements that are prescribed by policy, but for which measurements have been
#    received.  Our convention holds that unprescribed courese/learning-standard measurements are assigned to a unit
#    named "Assessed Not in Blueprint".
	blueprint_partition = {'prescribed_units' : course_blueprint.loc[[not bool(re.search('Assessed Not in Blueprint', unit_nm)) 
																		for unit_nm in course_blueprint['UNIT_NM']]],
						   'unprescribed_units' : course_blueprint.loc[[bool(re.search('Assessed Not in Blueprint', unit_nm)) 
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
	blueprint_partition.update({'unprescribed_units' : pd.merge(left = blueprint_partition.get('unprescribed_units').drop(labels = 'UNIT_NUMBER',
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


def cpt_query(session_attributes, tenant_config, work_dir): 
	return pd.DataFrame(data = list(create_engine(URL(**tenant_config)).execute(text(' '.join([sql_line 
																										for sql_line in open(file = os.path.abspath(os.path.join(work_dir, 
																															'CPT_QUERY.sql')) )])),
																						tenant_id = session_attributes.get('TENANT_ID')      )),
									columns = ['CONSTITUENT_COUNT',
											   'CPT_CELL_IDX',
											   'MEAS',
											   'IS_ROOT'])\
							.sort_values(by = ['CONSTITUENT_COUNT',
											   'CPT_CELL_IDX'])\
							.drop_duplicates()\
							.astype(str)


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
	session_attributes.update({'work_dir' : work_dir})
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
	session_attributes.update({'sftp_destination' : '/'.join([session_attributes.get('sftp_destination'), 'STG_COURSE_UNIT_exhaustive_scan' + '_' + benchmark_time[:-1]]),
							   'benchmark_time' : benchmark_time})
	

	tenant_config = derive_tenant_config(tenant_config_dir_args, work_dir, session_attributes)
	pysftp.Connection(**sftp_login_params).makedirs(session_attributes.get('sftp_destination'))

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

# print(datetime.utcnow())
# print(len(cpt_query(session_attributes, tenant_config, work_dir)))
# print(datetime.utcnow())

#
# Ⓐ Query the production database for curriculum-specification tables, and for the working copy of student_standardized_test.
#    We use functions to invoke production-dtabase query logic.
lrn_std_id_cd = lrn_std_id_cd_query(session_attributes, tenant_config, work_dir)
acad_yr_start_end = acad_yr_start_end_query(session_attributes, tenant_config, work_dir)  # ℕ𝕖𝕨 ℚ𝕦𝕖𝕣𝕪: We emply the "official" academic-year
																						  #           start, end dates to specify default start,
																						  #           end dates for the 'Assessed Not in Blueprint'
																						  #           unit created here.
stud_campus_enrolmt = enrollment_query(session_attributes, tenant_config, work_dir)  # ℕ𝕖𝕨 ℚ𝕦𝕖𝕣𝕪: We need to associate a student with a campus
																					 #           for purposes of evidentiary alignment.
course_catalogue = course_cat_query(session_attributes, tenant_config, work_dir)  # ℕ𝕖𝕨 ℚ𝕦𝕖𝕣𝕪: We compare courses from the sftp-server 'staged'
																				  #           course-blueprint tables — from which we get the
																				  #           authoritative values of BLUEPRINT_TITLE — with
																				  #           the authoritative course catalogue in the production
																				  #           database. 
proficiency_span = prof_span_query(session_attributes, tenant_config, work_dir)   # 𝕄𝕠𝕕𝕚𝕗𝕚𝕖𝕕 ℚ𝕦𝕖𝕣𝕪: Minor resequencing of "filtering" logic from 
#                                                                                 #               a previonsly productionized baseline is applied. 
																				  #               This potentially affects a marginal improvement in
																				  #               efficiency.
stg_course_unit = stg_course_unit_query(session_attributes, tenant_config, work_dir)  # ℕ𝕖𝕨 ℚ𝕦𝕖𝕣𝕪: We get the deployed-in-production version of the 
																					  #           STG_COURSE_UNIT table from the COURSE_LEARNING_STANDARD_MAP
																					  #           table in produciton. This query derives a STG_COURSE_UNIT
																					  #           from COURSE_LEARNING_STANDARD_MAP.  This represents the
																					  #           "ground truth" in that the application displays 
																					  #           course/learning-standard measurements, estimates for
																					  #           learning standards in the COURSE_LEARNING_STANDARD_MAP table.
algmt_detail = evid_algmt_query(session_attributes, tenant_config, work_dir)    # ℕ𝕖𝕨 ℚ𝕦𝕖𝕣𝕪: We perform a complex, denormalizing query of the
																				#           associated production database to get an exhaustive
																				#           set of evidentiary measurements.
#
##################################################################################################################################
#🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛#
##################⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇ DEVELOPMENTAL STATEMENT ⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇###############
# Acquire our baseline stg_course_unit from an sft-server locaiton.  This is needed if the version derived from from 
# IBMSIH.COURSE_LEARNING_STANDARD_MAP lacks characteristics needed for development.
# pysftp.Connection(**sftp_login_params).get(remotepath = os.path.abspath(os.path.join(session_attributes.get('sftp_source'), 
# 																					session_attributes.get('stg_course_unit'))),
# 											localpath = os.path.abspath(os.path.join(session_attributes.get('work_dir'), 
# 																					'COURSE_UNIT.csv')),
# 											preserve_mtime = True)
# stg_course_unit = pd.read_csv(filepath_or_buffer = os.path.abspath(os.path.join(session_attributes.get('work_dir'), 
# 																						'COURSE_UNIT.csv')),
# 										dtype = str).drop_duplicates()
# os.remove(os.path.abspath(os.path.join(session_attributes.get('work_dir'), 
# 															'COURSE_UNIT.csv')))

# stg_course_unit = pd.merge(left = stg_course_unit.drop(labels = 'SIH_COURSEPK_ID',
# 													   axis = 1)\
# 												  .rename(columns = {'SIH_LEARNING_STANDARD_ID' : 'LEARNING_STANDARD_ID'}) ,
# 						   right = course_catalogue[['SIH_COURSEPK_ID',
# 													 'COURSE_ID']])
#
##################⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆ DEVELOPMENTAL STATEMENT ⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆##############
#🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛#
###################################################################################################################################
#
# Ⓑ Deconvolve the stg_course_unit, the course × instructional-unit × learning-standard view of the
#    intended curriculum. We employ internally-defined course_unit_deconvolution.  This procedure separates out the stg_course_unit_blueprint
#    and the course_unit_bridge tables. It 'synthesizes' a BLUEPRINT_TITLE attribute by concatenating the (subject, grade-level)-span
#    of the blueprint with an integer index. We subsequently need substitute these 'sythesized' values with the 'authoritative' versions
#    in later logic near the end of this procedure.
stg_course_unit = stg_course_unit_query(session_attributes, tenant_config, work_dir)
deconvolved_course_unit = course_unit_deconvolution(stg_course_unit = stg_course_unit)
deconvolved_course_unit.update({'STG_COURSE_UNIT_BLUEPRINT' : deconvolved_course_unit.get('STG_COURSE_UNIT_BLUEPRINT')\
																							.loc[[not bool(re.search('Assessed Not in Blueprint', unit_nm)) 
																							for unit_nm in deconvolved_course_unit.get('STG_COURSE_UNIT_BLUEPRINT')['UNIT_NM']]],
								 'prior_unprescribed_measurements' : deconvolved_course_unit.get('STG_COURSE_UNIT_BLUEPRINT')\
																							.loc[[bool(re.search('Assessed Not in Blueprint', unit_nm)) 
																							for unit_nm in deconvolved_course_unit.get('STG_COURSE_UNIT_BLUEPRINT')['UNIT_NM']]],
								 'unaligned_crs_lrn_std_meas' : algmt_detail.set_index(keys = 'LEARNING_STANDARD_AY',
																					   drop = True)\
																			.loc['CURRENT_AY_LEARNING_STD', ['CAMPUS',
																											 'COURSE_SUBJECTS',
																											 'COURSE_GRADE_LVL',
																											 'COURSE_SECTION_TITLE',
																											 'STANDARD_SUBJECT',
																											 'STANDARD_GRADE_LVL',
																											 'LEARNING_STANDARD_CD',
																											 'SIH_COURSEPK_ID',
																											 'LEARNING_STANDARD_ID',
																											 'COURSE_STANDARD_ALIGNMENT']]\
																			.rename(columns = {'STANDARD_GRADE_LVL' : 'GRADE_LEVEL'})\
																			.set_index(keys = 'COURSE_STANDARD_ALIGNMENT',
																					   drop = True)\
																			.drop(labels = 'Aligned Standard',
																				  axis = 0)\
																			.drop_duplicates()\
																			.reset_index(drop = True)\
																			.astype(str)    })
#
##################################################################################################################################
#🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛#
##################⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇ DEVELOPMENTAL STATEMENT ⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇###############
# If our overall blueprint-extension procedure has previously been run, and  no new non-prescribed measurements have been received,
# then all of the non-prescribed learning standards will already be in the stg_course_unit_blueprint data frames. Subsequent
# logic to identify and process them will not therefore be exercised. This statement "reinitialzies" the stg_course_unit_blueprint
# so that non-prescribed measurements are not identified as such. Commenting-out this statement is the normal configuration.
# Un-commenting it allows us to expose non-prescribed measurements to the logic.
#
# In terms of the output, if this statement is commented-out and all of the non-prescribed course/learning-standard measurements
# have beem previously dealt-with, then all such measurements go into the prior_unprescribed_measurements partition of the
# STG_COURSE_UNIT_BLUEPRINT element of deconvolved_course_unit.  Otherwise, the logic subsequently assigns them to a 
# new_unprescribed_measurements dictionary item. In the end, STG_COURSE_UNIT_BLUEPRINT, prior_unprescribed_measurements, 
# and new_unprescribed_measurements partitions are concatenated together to produce the final output.
# deconvolved_course_unit.update({'prior_unprescribed_measurements' : pd.DataFrame(columns = deconvolved_course_unit.get('prior_unprescribed_measurements').columns)})
# stg_course_unit = pd.merge(left = deconvolved_course_unit.get('STG_COURSE_UNIT_BLUEPRINT'),
#                          right = deconvolved_course_unit.get('COURSE_UNIT_BRIDGE')).drop(labels = 'BLUEPRINT_TITLE',
#                                                                                          axis = 1)\
#                                                                                    .sort_values(by = ['SIH_COURSEPK_ID',
#                                                                                                       'UNIT_NUMBER'])\
#                                                                                    [['COURSE_ID',
#                                                                                      'COURSE_TITLE',
#                                                                                      'UNIT_NUMBER',
#                                                                                      'UNIT_NM',
#                                                                                      'LEARNING_STANDARD_CD',
#                                                                                      'SUBJECT',
#                                                                                      'GRADE_LEVEL',
#                                                                                      'UNIT_START_DT',
#                                                                                      'UNIT_FINISH_DT',
#                                                                                      'SIH_COURSEPK_ID',
#                                                                                      'LEARNING_STANDARD_ID',
#                                                                                      'TENANT_ID']]
# deconvolved_course_unit.update({'prior_unprescribed_measurements' : pd.DataFrame(columns = deconvolved_course_unit.get('STG_COURSE_UNIT_BLUEPRINT').columns) })
#
##################⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆ DEVELOPMENTAL STATEMENT ⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆##############
#🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛#
###################################################################################################################################
#
#  Ⓒ Identify the non-prescribed course/learning-standard measurements to be added to the course blueprint.
#     We perform an alignment analysis to identify non-prescribed measurements. We then compare these
#     with the baseline course blueprints. 
#    ① Get all (sih_coursepk_id, learning_standard_id) pairs for individual-student proficiency measurements 
#       within the (subject, grade-level) span of the blueprint of a course in which the student is enrolled.
#       Project student_standardized_test onto the subspace of proficiency_span corresponding to
#       blueprint-aligned standards. Marginalize the result to get the desired tuple.
algned_evid_span_subj_grd_proj = fct.reduce(lambda x, y: pd.merge(left = x,
																  right = y),
															   [proficiency_span.set_index(keys = 'BLUEPRINT_ALGNED_LRN_STD',
																						   drop = True)\
																				 .loc['Y']\
																				 .reset_index(drop = True)\
																				 [['SIH_COURSEPK_ID',
																				   'COURSE_TITLE',
																				   'STUDENT_ID',
																				   'STANDARD_SUBJECT',
																				   'STANDARD_GRADE_LVL']]\
																				 .drop_duplicates(),
																algmt_detail[['STUDENT_ID',
																			  'STANDARD_SUBJECT',
																			  'STANDARD_GRADE_LVL',
																			  'LEARNING_STANDARD_CD',
																			  'LEARNING_STANDARD_ID']]\
																			 .drop_duplicates(),
																stud_campus_enrolmt[['STUDENT_ID']]])[['SIH_COURSEPK_ID',
																									   'COURSE_TITLE',
																									   'LEARNING_STANDARD_CD',
																									   'LEARNING_STANDARD_ID']]\
																									  .drop_duplicates()

#
#    ② Identify the (sih_coursepk_id, learning_standard_id) tuples from proficiency-span projection above that
#       are not in stg_course_unit.  Convert our dataframes to tuples  list using the pd.to_records feature.  Perform 
#       set-difference operation.
new_unprescribed_measurements = pd.DataFrame(data = list(set(algned_evid_span_subj_grd_proj[['SIH_COURSEPK_ID',
																							 'COURSE_TITLE',
																							 'LEARNING_STANDARD_CD',
																							 'LEARNING_STANDARD_ID']]\
																						   .to_records(index = False)\
																						   .tolist()) -\
														set(stg_course_unit[['SIH_COURSEPK_ID',
																			 'COURSE_TITLE',
																			 'LEARNING_STANDARD_CD',
																			 'LEARNING_STANDARD_ID']]\
																			.to_records(index = False)\
																			.tolist())),
											 columns = ['SIH_COURSEPK_ID',
														'COURSE_TITLE',
														'LEARNING_STANDARD_CD',
														'LEARNING_STANDARD_ID']).drop_duplicates()\
																				.sort_values(by = ['COURSE_TITLE',
																								   'LEARNING_STANDARD_CD'])
#
#    ③ Extend our new_unprescribed_measurements out into a stg_course_unit table with UNIT_NM as "Assessed Not in Bluepribt".
#       This is accomplishedf by a series of joins.
#       ⓐ A (course_id, course_title, sih_coursepk_id) slice from proficiency_span gives us the course-identification attributes,
#          which we need to associate via COURSE_UNIT_BRIDGE with a BLUEPRINT_TITLE;
#       ⓑ lrn_std_id_cd gives us human-recognizable learning-standard attributes;
#       ⓒ The COURSE_UNIT_BRIDGE component of deconvolved_course_unit gives us the BLUEPRINT_TITLE; and
#       ⓓ Manipulation via pandas groupby of the STG_COURSE_UNIT_BLUEPRINT dataframe from 
#          deconvovled_course_unit conditionally assigns the UNIT_NUMBER attribute.
#       We use a lambda operation to join all of these items in a single command. After the join, we drop off
#       the course-distict attributes and de-duplicate the result.
course_blueprint_extension =\
	fct.reduce(lambda x, y: pd.merge(left = x,
									right = y),
							[new_unprescribed_measurements.assign(UNIT_NM = 'Assessed Not in Blueprint',
																  UNIT_START_DT = acad_yr_start_end.loc[0,'ACADEMIC_YEAR_START'],
																  UNIT_FINISH_DT = acad_yr_start_end.loc[0,'ACADMEMIC_YEAR_END'],
																  TENANT_ID = session_attributes.get('TENANT_ID')),
							 proficiency_span[['COURSE_ID',
											   'COURSE_TITLE',
											   'SIH_COURSEPK_ID']]\
											 .drop_duplicates(),
							 lrn_std_id_cd[['LEARNING_STANDARD_ID',
											'LEARNING_STANDARD_CD',
											'SUBJECT_TITLE',
											'GRADE_LEVEL']]\
											.rename(columns = {'SUBJECT_TITLE' : 'SUBJECT',
															   'GRADE_LEVEL' : 'GRADE_LEVEL'}),
							  deconvolved_course_unit.get('COURSE_UNIT_BRIDGE'),
							  pd.DataFrame(data = [ (bp_title, ('00' +  str(int(max(unit_no))))[-3:], max(unit_no))
																if 'Assessed Not in Blueprint' in unit_nm
																else (bp_title, ('00' +  str(int(max(unit_no))+1))[-3:], max(unit_no)) 
													for (bp_title, unit_nm, unit_no) 
													in  deconvolved_course_unit.get('STG_COURSE_UNIT_BLUEPRINT')[['BLUEPRINT_TITLE',
																												  'UNIT_NM',
																												  'UNIT_NUMBER']]\
																												.groupby(by = 'BLUEPRINT_TITLE',
																														 as_index = False)\
																												.agg(set)\
																												.to_records(index = False)\
																												.tolist()],
										   columns =  ['BLUEPRINT_TITLE',
													   'UNIT_NUMBER',
													   'prior_max_unit_number'] ) ] )\
						.drop(labels = ['SIH_COURSEPK_ID',
										'COURSE_ID',
										'COURSE_TITLE'],
							  axis = 1)\
						.drop_duplicates()\
						.assign(SIH_COURSEPK_ID = '')\
						[deconvolved_course_unit.get('STG_COURSE_UNIT_BLUEPRINT').columns.tolist() + ['prior_max_unit_number'] ]
#
# Ⓓ Assemble the results in deconvolved_course_unit.  Our courrent-state STG_COURSE_UNIT_BLUEPRINT data-frame element of the 
#    dictionary contains only the prescribed course/learning-standard measurements. The dictionary also contains prior_unprescribed_measurements,
#    a partition of the original c containing all previously-identified non-prescribed course/learning-standard measurements.
#    We want to concatenate these and course_blueprint_extension into an updated STG_COURSE_UNIT_BLUEPRINT. We also  want to save
#    the baseline STG_COURSE_UNIT_BLUEPRINT as STG_COURSE_UNIT_BLUEPRINT_prescribed.
deconvolved_course_unit.update({'new_unprescribed_measurements' : course_blueprint_extension,
								'STG_COURSE_UNIT_BLUEPRINT_prescribed' : deconvolved_course_unit.get('STG_COURSE_UNIT_BLUEPRINT'),
								'STG_COURSE_UNIT_BLUEPRINT' : pd.concat(objs = [deconvolved_course_unit.get('STG_COURSE_UNIT_BLUEPRINT'),
																				deconvolved_course_unit.get('prior_unprescribed_measurements'),
																				course_blueprint_extension],
																		sort = True).sort_values(by = ['BLUEPRINT_TITLE',
																										'UNIT_NUMBER',
																										'LEARNING_STANDARD_CD'])\
																					[deconvolved_course_unit.get('STG_COURSE_UNIT_BLUEPRINT').columns]})
#
# Ⓔ Incorporate authoritative attributes not in the production database. Specifically, the production database does
#    not store the BLUEPRINT_TITLE attribute. This attribute — produced by the Watsion Education Data-Integration (WEDI) 
#    curriculum-management tool — resides in a version  of our COURSE_BLUEPRINT table on the sftp server. Additionally, 
#    the version on the staged sftp version contains courses not accounted for in the production database. We want our
#    output here to contain all of that information.  So, we're going to get this information from the sftp
#    server and incorporate it.
#    ① Get the COURSE_UNIT_BRIDGE and STG_COURSE_UNIT_BLUEPRINT version from the sftp server. We use pysftp.
#       We write them to local working space as comma-separated-variable (csv) files, read them in as dataframes,
#       and immediately delete the local-stored versions.
pysftp.Connection(**sftp_login_params).get(remotepath = os.path.abspath(os.path.join(session_attributes.get('sftp_source'), 
																					session_attributes.get('stg_course_unit_blueprint'))),
											localpath = os.path.abspath(os.path.join(session_attributes.get('work_dir'), 
																					'STG_COURSE_UNIT_BLUEPRINT.csv')),
											preserve_mtime = True)
STG_COURSE_UNIT_BLUEPRINT_sftp = pd.read_csv(filepath_or_buffer = os.path.abspath(os.path.join(session_attributes.get('work_dir'), 
																							 'STG_COURSE_UNIT_BLUEPRINT.csv')),
											dtype = str,
											encoding = "ISO-8859-1").drop_duplicates()
os.remove(os.path.abspath(os.path.join(session_attributes.get('work_dir'), 
															'STG_COURSE_UNIT_BLUEPRINT.csv')))
pysftp.Connection(**sftp_login_params).get(remotepath = os.path.abspath(os.path.join(session_attributes.get('sftp_source'), 
																					session_attributes.get('stg_course_unit_bridge'))),
											localpath = os.path.abspath(os.path.join(session_attributes.get('work_dir'), 
																					'COURSE_UNIT_BRIDGE.csv')),
											preserve_mtime = True)
COURSE_UNIT_BRIDGE_sftp = pd.read_csv(filepath_or_buffer = os.path.abspath(os.path.join(session_attributes.get('work_dir'), 
																						'COURSE_UNIT_BRIDGE.csv')),
										dtype = str).drop_duplicates()
os.remove(os.path.abspath(os.path.join(session_attributes.get('work_dir'), 
															'COURSE_UNIT_BRIDGE.csv')))

pysftp.Connection(**sftp_login_params).get(remotepath = os.path.abspath(os.path.join(session_attributes.get('sftp_source'), 
																					session_attributes.get('stg_course_unit'))),
											localpath = os.path.abspath(os.path.join(session_attributes.get('work_dir'), 
																					'COURSE_UNIT.csv')),
											preserve_mtime = True)
COURSE_UNIT_sftp = pd.read_csv(filepath_or_buffer = os.path.abspath(os.path.join(session_attributes.get('work_dir'), 
																						'COURSE_UNIT.csv')),
										dtype = str).drop_duplicates()
os.remove(os.path.abspath(os.path.join(session_attributes.get('work_dir'), 
															'COURSE_UNIT.csv')))

#
#    ② Find unrecognized courses in the COURSE_UNIT_BRIDGE_sftp.  Identify them and add to deconvolved_course_unit
#       as an unrecognized course item.  Truncate COURSE_UNIT_BRIDGE_sftp to retain only recognized courses.
deconvolved_course_unit.update({'unrecognized_courses' : 
										COURSE_UNIT_BRIDGE_sftp.set_index(keys = 'COURSE_ID')\
																.loc[set(COURSE_UNIT_BRIDGE_sftp['COURSE_ID'])- set(course_catalogue['COURSE_ID'])]\
																.reset_index(drop = False)  })
COURSE_UNIT_BRIDGE_sftp = pd.merge(left = COURSE_UNIT_BRIDGE_sftp,
									right = course_catalogue[['COURSE_ID']])
#
#    ③ Find courses in COURSE_UNIT_BRIDGE_sftp that are not accounted-for in the production-derived COURSE_BLUEPRINT_BRIDGE.
deconvolved_course_unit.update({'courses_not_deployed' : 
				COURSE_UNIT_BRIDGE_sftp.set_index(keys = 'COURSE_ID')\
										.loc[set(COURSE_UNIT_BRIDGE_sftp['COURSE_ID']) - set(deconvolved_course_unit.get('COURSE_UNIT_BRIDGE')['COURSE_ID'])]\
										.reset_index(drop = False) })
COURSE_UNIT_BRIDGE_sftp = pd.merge(left = COURSE_UNIT_BRIDGE_sftp,
									right = deconvolved_course_unit.get('COURSE_UNIT_BRIDGE')[['COURSE_ID']]).drop_duplicates()
#
#    ④ Create a blueprint-title bridge table. This maps between the "synthetic" blueprint title created by our
#       course_unit_deconvolution and the 'authoritative' value in the sftp location. This comes from joining 
#       our sftp version of COURSE_UNIT_BRIDGE onto that produced our routine.
blueprint_title_bridge =  pd.merge(left = COURSE_UNIT_BRIDGE_sftp[['COURSE_ID',
																   'BLUEPRINT_TITLE']],
									right = deconvolved_course_unit.get('COURSE_UNIT_BRIDGE')\
																	[['COURSE_ID',
																	  'BLUEPRINT_TITLE']]\
																	.rename(columns = {'BLUEPRINT_TITLE' : 'syn_blueprint_title'}),
									how = 'left')\
									.drop(labels = 'COURSE_ID',
										  axis = 1)\
									.drop_duplicates()\
									.reset_index(drop = True)
#
#    ⑤ Substitute the 'authoritative' BLUEPRINT_TITLE for the 'synthetic' value derived by our course_unit_deconvolution procedure.
#       Accomplish using a join/drop approach. Rename the BLUEPRINT_TITLE attribute to syn_blueprint_title. Then join the 
#       STG_COURSE_UNIT_BLUEPRINT and COURSE_UNIT_BRIDGE
deconvolved_course_unit.update({bp_title_obj : pd.merge(left = deconvolved_course_unit.get(bp_title_obj)\
																					  .rename(columns = {'BLUEPRINT_TITLE' : 'syn_blueprint_title'}),
														right = blueprint_title_bridge)\
												.drop(labels = 'syn_blueprint_title',
													  axis = 1)\
												.drop_duplicates()\
												[deconvolved_course_unit.get(bp_title_obj).columns]
								for bp_title_obj in {'STG_COURSE_UNIT_BLUEPRINT',
													 'COURSE_UNIT_BRIDGE',
													 'STG_COURSE_UNIT_BLUEPRINT_prescribed',
													 'prior_unprescribed_measurements',
													 'new_unprescribed_measurements'}})
#
##################################################################################################################################
#🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛#
##################⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇ MARCH 13, 2019 LOGIC FIX ⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇###############
# We fix here the construction of STG_COURSE_UNIT, produced by rejoining STG_COURSE_UNIT_BLUEPRINT with COURSE_UNIT_BRIDGE.
# Our original list of reference columns was based on the stg_course_unit produced by stg_course_unit_query SQL query.
# This query synthesizes stg_course_unit from COURSE_LEARNING_STANDARD_MAP, which lacks required attribute UNIT_NUMBER.
# Also, the production-database-required SIH_COURSEPK_ID surrogate-key attribute assigned to STG_COURSE_UNIT_BLUEPRINT is blank.  We
# drop this blank value and join in an actual value from course_catalogue.  Also get the reference column list from the 
# sftp-baseline for the stg_course_unit table. 
deconvolved_course_unit.update({'STG_COURSE_UNIT' : fct.reduce(lambda x, y: pd.merge(left = x,
																					 right = y),
																			[deconvolved_course_unit.get('STG_COURSE_UNIT_BLUEPRINT')\
																									.drop(labels = ['SIH_COURSEPK_ID'],
																										  axis = 1),
																			 deconvolved_course_unit.get('COURSE_UNIT_BRIDGE'),
																			 course_catalogue[['COURSE_ID',
																							   'SIH_COURSEPK_ID']]] )\
																							 .rename(columns = {'LEARNING_STANDARD_ID' : 'SIH_LEARNING_STANDARD_ID'})\
																							 [['COURSE_ID',
																							   'COURSE_TITLE',
																							   'UNIT_NUMBER',
																							   'UNIT_NM',
																							   'LEARNING_STANDARD_CD',
																							   'SUBJECT',
																							   'GRADE_LEVEL',
																							   'UNIT_START_DT',
																							   'UNIT_FINISH_DT',
																							   'SIH_COURSEPK_ID',
																							   'SIH_LEARNING_STANDARD_ID',
																							   'TENANT_ID']]\
																							 .sort_values(by = ['COURSE_ID',
																												'UNIT_START_DT',
																												'UNIT_NUMBER',
																												'LEARNING_STANDARD_CD'])\
																							 .drop_duplicates(),
								'blueprint_summary' : pd.merge(left = pd.merge(left = deconvolved_course_unit.get('STG_COURSE_UNIT_BLUEPRINT')[['BLUEPRINT_TITLE',
																																			  'UNIT_NUMBER',
																																			  'UNIT_NM',
																																			  'LEARNING_STANDARD_CD']]\
																											.groupby(by = ['BLUEPRINT_TITLE',
																														   'UNIT_NUMBER',
																														   'UNIT_NM'],
																													 as_index = False)\
																											.agg(lambda x : len(set(x)))\
																											.rename(columns = {'LEARNING_STANDARD_CD' : 'learning_standard_count'})\
																											.drop_duplicates(),
																			right = deconvolved_course_unit.get('COURSE_UNIT_BRIDGE').drop(labels = 'COURSE_ID',
																																		   axis = 1)\
																											.groupby(by = 'BLUEPRINT_TITLE',
																													 as_index = False)\
																											.agg(lambda x : list(set(x)))   ),
															right = deconvolved_course_unit.get('STG_COURSE_UNIT_BLUEPRINT')[['BLUEPRINT_TITLE',
																															  'UNIT_NUMBER',
																															  'UNIT_NM',
																															  'LEARNING_STANDARD_CD']]\
																							.drop_duplicates()\
																							.groupby(by = ['BLUEPRINT_TITLE',
																										   'UNIT_NUMBER',
																										   'UNIT_NM'],
																									 as_index = False)\
																							.agg(lambda x : list(set(x)))\
																							.rename(columns = {'LEARNING_STANDARD_CD' : 'included_learning_standards'}))  })
deconvolved_course_unit.update({artifact : pd.merge(left = deconvolved_course_unit.get(artifact).rename(columns = {'GRADE_LEVEL' : 'grd_lvl_nonconforming'}),
													right = pd.DataFrame(data = [(grd_lvl, str(int(grd_lvl)) + ' ' )  if grd_lvl.isdigit()
																													else (grd_lvl, grd_lvl)
																													for grd_lvl in set(deconvolved_course_unit.get(artifact)['GRADE_LEVEL'])],
																		 columns = ['grd_lvl_nonconforming',
																					'GRADE_LEVEL']) )[deconvolved_course_unit.get(artifact).columns]
								   for artifact in set(deconvolved_course_unit.keys()) - {'COURSE_UNIT_BRIDGE','courses_not_deployed','blueprint_summary','unrecognized_courses'}} )
#
##################⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆ MARCH 13, 2019 LOGIC FIX ⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆##############
#🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛#
###################################################################################################################################
#
# Ⓔ Write the results out. We temporarily write them to local-directory storage location specified by our work_dir
#    parameter. We then put it into the sftp directory location specified by the sftp_destination value in session_attributes.
#    We immediately thereafter delete the temporarily-stored from the working-directory location. We prepend our 
#    filenames with benchmark_time to report a UTC time stamp indicating the time of session initation.
for curric_item in deconvolved_course_unit.keys():
	deconvolved_course_unit.get(curric_item).astype(str)\
											.to_csv(path_or_buf= os.path.abspath(os.path.join(session_attributes.get('work_dir'), 
																					benchmark_time + session_attributes.get('TENANT_ID') + '_' +\
																					 curric_item + '.csv')),
													index=False,
													encoding='utf-8',
													line_terminator='\r\n',
													quoting=csv.QUOTE_NONNUMERIC)
	pysftp.Connection(**sftp_login_params).put(localpath = os.path.abspath(os.path.join(session_attributes.get('work_dir'), 
																					benchmark_time + session_attributes.get('TENANT_ID') + '_' +\
																					 curric_item + '.csv')),
												remotepath = os.path.abspath(os.path.join(session_attributes.get('sftp_destination'), 
																					benchmark_time + session_attributes.get('TENANT_ID') + '_' +\
																					 curric_item + '.csv')),
												preserve_mtime = True)
	os.remove(os.path.abspath(os.path.join(session_attributes.get('work_dir'), 
											benchmark_time + session_attributes.get('TENANT_ID') + '_' +\
												 curric_item + '.csv')))

#
#
##################⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆ MAIN PROGRAM ⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆##############
#🚛⚒⛏📇🔑🔭🔬🔩⚙️🗜📡🔌🚛⚒⛏📇🔑🔭🔬🔩⚙️🗜📡🔌🚛⚒⛏📇🔑🔭🔬🔩⚙️🗜📡🔌🚛⚒⛏📇🔑🔭🔬🔩⚙️🗜📡🔌🚛⚒⛏📇🔑🔭🔬🔩⚙️🗜📡#
##################################################################################################################################




## ‼️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️‼️#        
## ‼️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️‼️#        
## ‼️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️‼️#        
## ‼️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️‼️#        
#
## ‼️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️‼️#        



## ‼️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️‼️#        
## ‼️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️‼️#        
## ‼️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️‼️#        
## ‼️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️‼️#        
#
## ‼️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️‼️#        


