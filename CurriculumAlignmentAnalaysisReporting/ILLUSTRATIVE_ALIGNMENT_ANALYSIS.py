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
import names
from numpy import random
#
##################################################################################################################################
#⚠️🚸🛑🆗🔔🖼💻⚠️🚸🛑🆗🔔🖼💻⚠️🚸🛑🆗🔔🖼💻⚠️🚸🛑🆗🔔🖼💻⚠️🚸🛑🆗🔔🖼💻⚠️🚸🛑🆗🔔🖼💻⚠️🚸🛑🆗🔔🖼💻⚠️🚸🛑🆗🔔🖼💻#
##################⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇ RUN-TIME PLATFORM-CONFIGURATIONS ⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇##############
# These directory paths must te adapted to the platform on which the logic is deployed. We require specificaiton of two
# platform configurations:
# ⧐ work_dir contains the location of local working storage space.  It contains SESSION_ATTRIBUTES.csv containing client-specific
#   attributes, including sftp-server locations from which external data are retrived and outputs stored.
# ⧐ db_credential_dir contains private credentials used to authenticate for database queries and for sftp sessions.
work_dir = '/Users/nahamlet@us.ibm.com/Box Sync/IBM-Watson ED K12/Pathway-Centric CONOPS/Mastery-Readiness Offering/Illustrative_aligment_artifacts'
db_credential_dir = '/Users/nahamlet@us.ibm.com/Documents/Documents/Oportunidades actuales'
#
###################⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆ RUN-TIME PLATFORM-CONFIGURATIONS ⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆##############
#⚠️🚸🛑🆗🔔🖼💻⚠️🚸🛑🆗🔔🖼💻⚠️🚸🛑🆗🔔🖼💻⚠️🚸🛑🆗🔔🖼💻⚠️🚸🛑🆗🔔🖼💻⚠️🚸🛑🆗🔔🖼💻⚠️🚸🛑🆗🔔🖼💻⚠️🚸🛑🆗🔔🖼💻#
##################################################################################################################################
#
## PURPOSE:  PROVIDE EVIDENTIARY-COVERAGE TREND ANALYSIS.  We want to report how evidentiary coverage of the
##           Intended Curriculum evolves over time. This includes the proportion of measured versus unmeasured
##           covered versus time and at distinct points in time. We report the following:
##           ⧐ Measured versus unmeasured learning standards by (campus, teacher, course section) most-recent
##             point in time;
##           ⧐ Proportion of (student, learning standard) tuples versus time for which measurements are reported
##             for each (campus, teacher, course section);
##           
##
## APPROACH: 
## Attempts were made to move most of the anciallary functionality out of the main body of the program.  This logic is intended to
## build on previously-deployed Software Configuration Items (SWCIs). We move such functionality into local functions in order to
## minimize their footprint in the program's main body.  The main body therefore mostly contains the new functionality.
##
## The procedure follows the following sequence. 
## ⓪ Set up session. This includes reading in session_attributes, querying IBMSIHG to get the logical 
##    location of the client DB, and creating the query-connection specifications. We concentrate the
##    session-initialization logic in a session_initializaiton function.
## Ⓐ Query the production database for several tables:  
##    ⧐ student_standardized_test contains operationally-representative student/learning-standard measurements that
##      we evaluate to detect the need to extend corresponding course blueprints with non-prescribed learning standards
##      for which measurements are offered;
##    ⧐ acad_yr_start_end contains 'official' academic-year start, end dates, which we assign as default values for
##      the start and end dates for contrived instructional units into which we place non-prescribed course/learning-standard
##      measurements;
##    ⧐ stud_campus_enrolmt is employed to assist with evidentiary-alignment analysis incidental to this procedure;
##    ⧐ course_catalogue provides an exhaustive course catalogue of courses offerred during the current academic year, used
##      to identify unrecognized courses in the 'offline' version of the course blueprints;
##    ⧐ proficiency_span contains an exhaustive set of (campus, course-section, student, learning-standard) tuples 
##      of which the intended curriculum is comprised, used for evidentiary-alignment analysis; and
##    ⧐ stg_course_unit contains the production version of course blueprints, which we compare with the course/learning-standard
##      measurements to identify non-prescribed measurements.
##
## Ⓑ Deconvolve the stg_course_unit table into two components:
##    ⧐ course_blueprint contains patterns of instructional-unit, learning-standard scope common to 
##      multiple, distinct courses; and
##    ⧐ course_blueprint_bridge associates individual buleprints to the courses for which they apply.
##    We employ pre-existing logic from a distinct piece of logic to accomplish this.  This is instantiated as 
##    a function.
## Ⓒ Identify non-prescribed course/learning-standard measurements. We use student_standardized_test and proficiency_span
##    to provide an evidentiary-alignment analysis. We compare this analysis — specifically non-aligned course/learning-standard
##    measurements with stg_course_unit, our "basline" set of course blueprints, to identify non-prescribed learning standards.
## Ⓓ Incorporate newly-identified non-prescribed course/learning-standard measurements into the course bluerpints.  This
##    includes incorporating the 'authoritative' BLUEPRINT_TITLE from offline source.
## Ⓔ Write outputs to the sftp server. We produce the following outputs:
##    ⧐ STG_COURSE_UNIT_BLUEPRINT contains the updated course blueprints, an iput to the Watson-Education Curriculm-
##      Management Tool;
##    ⧐ COURSE_UNIT_BRIDGE is an updated mapping of blueprints to courses, an input to the Watson-Education
##      Curriculum-Management Tool;
##    ⧐ STG_COURSE_UNIT is an updated, load-ready table obtained by joinin STG_COURSE_UNIT_BLUEPRINT with
##      COURSE_UNIT_BRIDGE;
##    ⧐ prior_unprescribed_measurements is a partition of the start-of-procedure 'baseline' STG_COURSE_UNIT_BLUEPRINT
##      containing all learning standards already contained in the '𝘈𝘴𝘴𝘦𝘴𝘴𝘦𝘥 𝘕𝘰𝘵 𝘪𝘯 𝘉𝘭𝘶𝘦𝘱𝘳𝘪𝘯𝘵' instructional units;
##    ⧐ new_unprescribed_measurements contains newly-identified non-prescribed course/learning-standard measurements,
##      which are added by this procedure to the '𝘈𝘴𝘴𝘦𝘴𝘴𝘦𝘥 𝘕𝘰𝘵 𝘪𝘯 𝘉𝘭𝘶𝘦𝘱𝘳𝘪𝘯𝘵' instructional units;
##    ⧐ STG_COURSE_UNIT_BLUEPRINT_prescribed is an original version of the 'STG_COURSE_UNIT_BLUEPRINT', containing 
##      the prescribed instructional-unit/learning-standard structure of the course before any non-prescribed measurements
##      are appended into a '𝘈𝘴𝘴𝘦𝘴𝘴𝘦𝘥 𝘕𝘰𝘵 𝘪𝘯 𝘉𝘭𝘶𝘦𝘱𝘳𝘪𝘯𝘵' instructional unit;
##    ⧐ unrecognized_courses is a partion of the COURSE_UNIT_BRIDGE from the offline source for which COURSE_IDs are
##      not contained in the production version of the course catalogue; and 
##    ⧐ courses_not_deployed is a partition of COURSE_UNIT_BRIDGE from the offline corresponding to COURSE_IDs 
##      recognizable fron the course catalogue, but not-yet deployed into the COURSE_LEARNING_STD_MAP table in 
##      in the procuction database.
##
##################################################################################################################################
#🤓😎🤡👻👽🧐🤓😎🤡👻👽🧐🤓😎🤡👻👽🧐🤓😎🤡👻👽🧐🤓😎🤡👻👽🧐🤓😎🤡👻👽🧐🤓😎🤡👻👽🧐🤓😎🤡👻👽🧐🤓🐵🙈👽🙉🙊🐒🐍#
##################⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇ LOCAL FUNCTIONS ⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇##############
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
										'STD_PROGRESSION_ALIGNMENT',
										'MEASUREABLE_STANDARD_YN',
										'SIH_COURSEPK_ID',
										'COURSE_SECTION_SID',
										'SIH_PERSONPK_ID',
										'LEARNING_STANDARD_ID',
										'DATE_MEASUREMENT_LOADED',
										'EVID_TUPLE']).astype(str)
	return pd.merge(left = evid_algmt.rename(columns = {'STANDARD_GRADE_LVL' : 'grd_lvl_nonconforming'}),
					right = pd.DataFrame(data = [(grd_lvl, ('00' + grd_lvl.strip())[-2:])
													for grd_lvl in set(evid_algmt['STANDARD_GRADE_LVL'])],
										 columns = ['grd_lvl_nonconforming',
													'STANDARD_GRADE_LVL'])).drop(labels = 'grd_lvl_nonconforming',
																				 axis = 1)[evid_algmt.columns]

def skl_query(session_attributes, tenant_config, work_dir):
	return pd.DataFrame(data = list(create_engine(URL(**tenant_config)).execute(text(' '.join([sql_line 
																								for sql_line in open(file = os.path.abspath(os.path.join(work_dir, 
																															'ALGMT_REPORTING—STUDENT_KNOWLEDGE_LEVEL.sql')) )])),
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
									   'LRN_STD_SUBJECT',
									   'LRN_STD_GRADE_LVL',
									   'LEARNING_STANDARD_CD',
									   'KNOWLEDGE_LEVEL',
									   'KNOWLEDGE_LVL_PREVISION',
									   'KNOWLEDGE_LVL_TYPE',
									   'LEARNING_STD_IN_CURRENT_AY',
									   'KNOWLEDGE_LVL_ASOF_DATE',
									   'MEASUREABLE_STANDARD_YN',
									   'SIH_PERSONPK_ID',
									   'LEARNING_STANDARD_ID',
									   'SIH_COURSEPK_ID',
									   'COURSE_SECTION_SID',
									   'STUDENT_KNOWLEDGE_LVL_SID',
									   'DATE_MEASUREMENT_LOADED',
									   'PROFICIENCY_SPACE_TUPLE']).drop_duplicates()\
																  .astype(str)\
																  .replace(to_replace = 'None',
																		   value = '')
	


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
										'STUDENT_ID',
										'GRADE_ENROLLED',
										'STUDENT_LNAME',
										'STUDENT_FNAME',
										'STANDARD_SUBJECT',
										'STANDARD_GRADE_LVL',
										'LEARNING_STANDARD_CD',
										'BLUEPRINT_ALGNED_LRN_STD',
										'PROG_ALGNED_LRN_STD',
										'LEARNING_STANDARD_ID',
										'SIH_COURSEPK_ID',
										'COURSE_SECTION_SID',
										'PROF_TUPLE'])\
						.drop_duplicates()
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


def std_graph_query(session_attributes, tenant_config, work_dir): 
	return pd.DataFrame(data = list(create_engine(URL(**tenant_config)).execute(text(' '.join([sql_line 
																										for sql_line in open(file = os.path.abspath(os.path.join(work_dir, 
																															'LEARNING_STANDARD_HIERARCHY.sql')) )])),
																						tenant_id = session_attributes.get('TENANT_ID')      )),
									columns = ['CONSTITUENT_LEARNING_STD_ID',
											   'LEARNING_STANDARD_ID',
											   'GRAPH_TYPE'])\
							.drop_duplicates()\
							.astype(str)


def course_verts_query(session_attributes, tenant_config, work_dir): 
	return pd.DataFrame(data = list(create_engine(URL(**tenant_config)).execute(text(' '.join([sql_line 
																										for sql_line in open(file = os.path.abspath(os.path.join(work_dir, 
																															'ALGMT_ANALYSIS—COURSE_GRAPH_VERTICES.sql')) )])),
																						tenant_id = session_attributes.get('TENANT_ID')      )),
									columns = ['CAMPUS',
											   'COURSE_SUBJECTS',
											   'COURSE_GRADE_LVL',
											   'COURSE_ID',
											   'COURSE_TITLE',
											   'STANDARD_SUBJECT',
											   'STANDARD_GRADE_LVL',
											   'LEARNING_STANDARD_CD',
											   'LEARNING_STANDARD_ID',
											   'BLUEPRINT_ALGNMENT',
											   'MEASUREABLE_STANDARD_YN',
											   'SIH_COURSEPK_ID',
											   'JURISDICTION_ID',
											   'TENANT_ID'])\
							.drop_duplicates()\
							.astype(str)



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

def cpt_query(session_attributes, tenant_config, work_dir):
	return pd.DataFrame(data = list(create_engine(URL(**tenant_config)).execute(text(' '.join(
																				[sql_line 
																				for sql_line in open(file = os.path.abspath(os.path.join(work_dir, 
																														'CPT_QUERY.sql')) )])),
																				tenant_id =  session_attributes.get('TENANT_ID'))),
								columns = ['CONSTITUENT_COUNT',
										   'CPT_CELL_IDX',
										   'MEAS',
										   'IS_ROOT']).astype(str)



def session_initialization(db_credential_dir, work_dir):
	session_attributes  = pd.read_csv(filepath_or_buffer = os.path.abspath(os.path.join(work_dir, 
																				'SESSION_ATTRIBUTES.csv')),
									dtype = str)\
									.set_index(keys = 'ATTRIBUTE',
												drop = True)\
									.to_dict(orient = 'dict').get('VALUE') 
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
	
	session_attributes.update({'work_dir' : work_dir,
							   'benchmark_time' : benchmark_time})

	tenant_config = derive_tenant_config(tenant_config_dir_args, work_dir, session_attributes)
#   pysftp.Connection(**sftp_login_params).makedirs(session_attributes.get('sftp_destination'))

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
# Ⓐ Get the working data.  We query the production database for five denormalized tables.  These are:
#    ⧐ body_of_evidence contains all student/learning-standard measurements;
#    ⧐ proficiency_span is the intended curriculum (Porter2001, http://ibm.biz/PorterCurriculumAlignment)
#      specified in (course-section, student, learning-standard);
#    ⧐ lrn_std_id_cd contains learning-standards specifications;
#    ⧐ student_kinowlede_level contains estimates of the learned curriculum (Porter2001, http://ibm.biz/PorterCurriculumAlignment); and
#    ⧐ stg_course_unit contains a course/instructional-unit/learning-standard summary of the
#      intended curriculum.
#    We keep only a subset of our body_of_evidence records. We specifically want the earliest occurrence for each
#    (course, student, learning-standard) triple.
body_of_evidence = evid_algmt_query(session_attributes, tenant_config, work_dir)
body_of_evidence = body_of_evidence.groupby(by = list(set(body_of_evidence.columns)-{'DATE_MEASUREMENT_LOADED'}),
											as_index = False)\
									.agg(min)\
									[body_of_evidence.columns]
proficiency_span = prof_span_query(session_attributes, tenant_config, work_dir)
lrn_std_id_cd = lrn_std_id_cd_query(session_attributes, tenant_config, work_dir)
student_knowledge_lvl = skl_query(session_attributes, tenant_config, work_dir)
stg_course_unit = stg_course_unit_query(session_attributes, tenant_config, work_dir)
course_graph_verts = course_verts_query(session_attributes, tenant_config, work_dir)
curric_std_graph = std_graph_query(session_attributes, tenant_config, work_dir)
start_dt = datetime.strptime(acad_yr_start_end_query(session_attributes, tenant_config, work_dir).loc[0,'ACADEMIC_YEAR_START'], '%Y-%m-%d')
crs_interval = (datetime.now()-start_dt).days
#
# Ⓑ De-identify the working data. We want to break any associations with a specific client.  To accomplish this, we
#    apply substitute values for campuses; first and last names for teachers and students; student IDs; course IDs; and
#    work products. We use random substitutes names and student ids. 
#    ① Get/produce aliased attributes for names, student ids. Get the campus-name aliases from an external
#       file. The name, ID aliases are randomly-generated.
campus_alias = pd.read_csv(filepath_or_buffer = os.path.abspath(os.path.join(work_dir, 
																			'CAMPUS_NAME_ALIAS.csv')),
						   dtype = str).drop(labels = 'SIH_ORGPK_ID',
											 axis = 1)
name_alias = pd.DataFrame(data = [(fname, lname) + tuple(names.get_full_name().upper().split(' '))
															for (fname, lname) in list(set(body_of_evidence[['STUDENT_FNAME',
																											 'STUDENT_LNAME']]\
																											.to_records(index = False)\
																											.tolist())\
																						.union(body_of_evidence[['TEACHER_FNAME',
																												 'TEACHER_LNAME']]\
																												.to_records(index = False)\
																												.tolist())\
																						.union(proficiency_span[['TEACHER_FNAME',
																												 'TEACHER_LNAME']]\
																												.to_records(index = False)\
																												.tolist())\
																						.union(proficiency_span[['STUDENT_FNAME',
																												 'STUDENT_LNAME']]\
																											   .to_records(index = False)\
																											   .tolist()))],
						  columns = ['FNAME_act',
									 'LNAME_act',
									 'FNAME_alias',
									 'LNAME_alis'])
stud_id_alias = pd.concat(objs = [stud_id_alias.assign(STUDENT_ID_alias = ['S-' + ('00000' + str(int_alias))[-mt.ceil(mt.log10(3*max(stud_id_alias['id_integer']))):]
																												for int_alias in random.randint(low = max(stud_id_alias['id_integer']) + 1,
																																				high = 3*max(stud_id_alias['id_integer']),
																																				size = len(stud_id_alias))])
									for stud_id_alias in [pd.DataFrame(data = [(stud_id, int(stud_id.split('S-')[1]))
																					for stud_id in set(proficiency_span['STUDENT_ID'])\
																									.union(body_of_evidence['STUDENT_ID'])\
																									.union(student_knowledge_lvl['STUDENT_ID']) ],
																	   columns = ['STUDENT_ID',
																				  'id_integer'])]],
						   sort = True).drop(labels = 'id_integer',
											 axis = 1)
crs_id_alias = pd.concat(objs = [crs_id_alias.assign(COURSE_ID_alias = list(map(str, random.randint(low = int(max(crs_id_alias['COURSE_ID'])) + 1,
																									 high = 3*int(max(crs_id_alias['COURSE_ID'])),
																									 size = len(crs_id_alias))  ))  )
									for crs_id_alias in [pd.DataFrame(data  = list(set(proficiency_span['COURSE_ID'])\
																					.union(body_of_evidence['COURSE_ID'])\
																					.union(student_knowledge_lvl['COURSE_ID'])),
																	   columns = ['COURSE_ID'])  ]],
						   sort = True)
#
##################################################################################################################################
#🐕🐩🐈🐿🦓🐪🐫🐃🐄🐎🦌🐈🦓🐘🐕🐩🐈🐿🦓🐪🐫🐃🐄🐎🦌🐈🦓🐘🐕🐩🐈🐿🦓🐪🐫🐃🐄🐎🦌🐈🦓🐘🐕🐩🐈🐿🦓🐪🐫🐃🐄🐎🦌🐈🦓🐘#
##################⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇ RECURRING-WORKING BLOCK ⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇##############

#    ② Apply the aliases to the body_of_evidence, proficiency_span, and student_knowledge_level.  Accomplish this via join-substitute
#       operations.
boe_aliased      = fct.reduce(lambda x, y: pd.merge(left = x,
													right = y),
											[body_of_evidence,
											 campus_alias,
											 name_alias.rename(columns = {'FNAME_act' : 'STUDENT_FNAME',
																		  'LNAME_act' : 'STUDENT_LNAME',
																		  'FNAME_alias' : 'STUDENT_FNAME_alias',
																		  'LNAME_alis' :  'STUDENT_LNAME_alias'}),
											 name_alias.rename(columns = {'FNAME_act' : 'TEACHER_FNAME',
																		  'LNAME_act' : 'TEACHER_LNAME',
																		  'FNAME_alias' : 'TEACHER_FNAME_alias',
																		  'LNAME_alis' :  'TEACHER_LNAME_alias'}),
											 stud_id_alias,
											 crs_id_alias,
											 pd.DataFrame(data = [(work_prod, work_prod_aliased.replace('i-Ready Lessons YTD', 'Formative Assessment'))
																	 for (work_prod, work_prod_aliased) in [(work_prod, work_prod.replace('Jumprope', 'Unit Exams')) 
																			if bool(re.search('Jumprope YTD', work_prod))
																			else (work_prod, work_prod)
																	for work_prod in set(body_of_evidence['WORK_PRODUCT_TITLE'])]],
														  columns = ['WORK_PRODUCT_TITLE',
																	 'WORK_PRODUCT_TITLE_aliased']) 
													] ).drop(labels = ['CAMPUS',
																	   'STUDENT_FNAME',
																	   'STUDENT_LNAME',
																	   'TEACHER_FNAME',
																	   'TEACHER_LNAME',
																	   'STUDENT_ID',
																	   'WORK_PRODUCT_TITLE',
																	   'COURSE_ID'],
															 axis = 1)\
														.rename(columns = {'STUDENT_FNAME_alias' : 'STUDENT_FNAME',
																		   'STUDENT_LNAME_alias' : 'STUDENT_LNAME',
																		   'TEACHER_FNAME_alias' : 'TEACHER_FNAME',
																		   'TEACHER_LNAME_alias' : 'TEACHER_LNAME',
																		   'CAMPUS_alias' : 'CAMPUS',
																		   'STUDENT_ID_alias' : 'STUDENT_ID',
																		   'WORK_PRODUCT_TITLE_aliased' : 'WORK_PRODUCT_TITLE',
																		   'COURSE_ID_alias' : 'COURSE_ID'})\
														[body_of_evidence.columns]
prof_span_aliased = fct.reduce(lambda x, y: pd.merge(left = x,
													 right = y),
											[proficiency_span,
											 campus_alias,
											 crs_id_alias,
											 name_alias.rename(columns = {'FNAME_act' : 'STUDENT_FNAME',
																		  'LNAME_act' : 'STUDENT_LNAME',
																		  'FNAME_alias' : 'STUDENT_FNAME_alias',
																		  'LNAME_alis' :  'STUDENT_LNAME_alias'}),
											 name_alias.rename(columns = {'FNAME_act' : 'TEACHER_FNAME',
																		  'LNAME_act' : 'TEACHER_LNAME',
																		  'FNAME_alias' : 'TEACHER_FNAME_alias',
																		  'LNAME_alis' :  'TEACHER_LNAME_alias'}),
											 stud_id_alias 
													] ).drop(labels = ['CAMPUS',
																	   'STUDENT_FNAME',
																	   'STUDENT_LNAME',
																	   'TEACHER_FNAME',
																	   'TEACHER_LNAME',
																	   'STUDENT_ID',
																	   'COURSE_ID'],
															 axis = 1)\
														.rename(columns = {'STUDENT_FNAME_alias' : 'STUDENT_FNAME',
																		   'STUDENT_LNAME_alias' : 'STUDENT_LNAME',
																		   'TEACHER_FNAME_alias' : 'TEACHER_FNAME',
																		   'TEACHER_LNAME_alias' : 'TEACHER_LNAME',
																		   'CAMPUS_alias' : 'CAMPUS',
																		   'STUDENT_ID_alias' : 'STUDENT_ID',
																		   'COURSE_ID_alias' : 'COURSE_ID'})\
														[proficiency_span.columns]
skl_aliased       = fct.reduce(lambda x, y: pd.merge(left = x,
													 right = y),
											[student_knowledge_lvl,
											 campus_alias,
											 crs_id_alias,
											 name_alias.rename(columns = {'FNAME_act' : 'STUDENT_FNAME',
																		  'LNAME_act' : 'STUDENT_LNAME',
																		  'FNAME_alias' : 'STUDENT_FNAME_alias',
																		  'LNAME_alis' :  'STUDENT_LNAME_alias'}),
											 name_alias.rename(columns = {'FNAME_act' : 'TEACHER_FNAME',
																		  'LNAME_act' : 'TEACHER_LNAME',
																		  'FNAME_alias' : 'TEACHER_FNAME_alias',
																		  'LNAME_alis' :  'TEACHER_LNAME_alias'}),
											 stud_id_alias 
													] ).drop(labels = ['CAMPUS',
																	   'STUDENT_FNAME',
																	   'STUDENT_LNAME',
																	   'TEACHER_FNAME',
																	   'TEACHER_LNAME',
																	   'STUDENT_ID',
																	   'COURSE_ID'],
															 axis = 1)\
														.rename(columns = {'STUDENT_FNAME_alias' : 'STUDENT_FNAME',
																		   'STUDENT_LNAME_alias' : 'STUDENT_LNAME',
																		   'TEACHER_FNAME_alias' : 'TEACHER_FNAME',
																		   'TEACHER_LNAME_alias' : 'TEACHER_LNAME',
																		   'CAMPUS_alias' : 'CAMPUS',
																		   'STUDENT_ID_alias' : 'STUDENT_ID',
																		   'COURSE_ID_alias' : 'COURSE_ID'})\
														[student_knowledge_lvl.columns]
stg_crs_unit_alias    = pd.merge(left = stg_course_unit.assign(
												UNIT_NM = [unit_nm.replace('Washington', 'Common Core Learning') if bool(re.search('Washington', unit_nm))
																												else unit_nm
															for unit_nm in stg_course_unit['UNIT_NM']]),
								 right = crs_id_alias).drop(labels = 'COURSE_ID',
															axis = 1)\
													  .rename(columns = {'COURSE_ID_alias' : 'COURSE_ID'})\
													  [stg_course_unit.columns]
#
# Ⓒ Temporally redistribute the evidentiary accrual. We desire our illustration to show approximately
#    uniform rate of accrual for each course. We uniformly distribute the learning-standard measurements for each course 
#    beginning from the start of the academic year until now.  Use a random-uniform distribution to get the
#    date corresponding to each learning-standard measurement. For measured students, distribute their measurement dates
#    uniformly ±10 days from the "centroid" for the course.
#    ① We now calculate the "temporal centroids" for course/learning-standard measurements for each 
#       (SIH_COURSEPK_ID, LEARNING_STANDARD_CD) couple. We want to build this up into a dataframe.
crs_std_meas_dates = pd.concat(objs = [pd.DataFrame(data = list((course, meas_std, meas_dt)
																		for (meas_std, meas_dt) in 
																		list(zip(sorted(standard),
																				 sorted(random.randint(low =15,
																									   high = crs_interval-10,\
																									   size = len(standard) ) )))),
													  columns = ['SIH_COURSEPK_ID',
																 'LEARNING_STANDARD_CD',
																 'crs_std_meas_date'])
															for (course, standard) in 
																	boe_aliased[['SIH_COURSEPK_ID',
																				 'LEARNING_STANDARD_CD',
																				 'LEARNING_STANDARD_AY']]\
																			   .set_index(keys = 'LEARNING_STANDARD_AY',
																						  drop = True)\
																			   .loc['CURRENT_AY_LEARNING_STD']\
																			   .drop_duplicates()\
																			   .groupby(by = 'SIH_COURSEPK_ID',
																						as_index = False)\
																			   .agg(set)\
																			   .to_records(index = False)\
																			   .tolist()],
							   sort = False)




stud_std_meas_dates = pd.concat(objs = [pd.DataFrame(data = list((course, standard, meas_stud, stud_meas_date)
																		for  (meas_stud, stud_meas_date) in  
																			list(zip(meas_stud,
																					 random.randint(low = -10,
																									high = 10,
																									size = len(meas_stud))))  ),
													  columns = ['SIH_COURSEPK_ID',
																 'LEARNING_STANDARD_CD',
																 'STUDENT_ID',
																 'stud_std_meas_date'])
																	for (course, standard, meas_stud) in 
																			boe_aliased[['SIH_COURSEPK_ID',
																						 'LEARNING_STANDARD_CD',
																						 'STUDENT_ID',
																						 'LEARNING_STANDARD_AY']]\
																					   .set_index(keys = 'LEARNING_STANDARD_AY',
																								  drop = True)\
																					   .loc['CURRENT_AY_LEARNING_STD']\
																					   .drop_duplicates()\
																					   .groupby(by = ['SIH_COURSEPK_ID',
																									  'LEARNING_STANDARD_CD'],
																								as_index = False)\
																					   .agg(set)\
																					   .to_records(index = False)\
																					   .tolist()],
								sort = False)



assmt_date =   pd.concat(objs = [meas_chron.assign(meas_date_offset = meas_chron[['crs_std_meas_date',
																							  'stud_std_meas_date']]\
																							.sum(axis = 1),
															   ASSESSMENT_DATE = [(start_dt + timedelta(days = offset_dt)).strftime('%Y-%m-%d')
																					for offset_dt in meas_chron[['crs_std_meas_date',
																												 'stud_std_meas_date']]\
																											   .sum(axis = 1)\
																											   .tolist()])\
													   .drop(labels = ['crs_std_meas_date',
																	   'stud_std_meas_date',
																	   'start_dt',
																	   'meas_date_offset'],
															 axis = 1)
											 for meas_chron in 
											 [pd.merge(left = crs_std_meas_dates,
													   right = stud_std_meas_dates).assign(start_dt = start_dt)]],
						 sort = False)




boe_aliased = pd.concat(objs = [pd.merge(left = boe_aliased.set_index(keys = ['LEARNING_STANDARD_AY'],
																	  drop = False)\
															 .loc['CURRENT_AY_LEARNING_STD']\
															 .drop(labels = ['ASSESSMENT_DATE'],
																   axis = 1),
										 right = assmt_date)[boe_aliased.columns],
								 boe_aliased.set_index(keys = ['LEARNING_STANDARD_AY'],
													   drop = False)\
											.drop(labels = 'CURRENT_AY_LEARNING_STD',
												  axis = 0)\
											.reset_index(drop = True)],
						sort = False)
#
# Ⓑ Back out adaptive extensions to the course blueprints in stg_course_unit. In-place mechanisms incorporate
#    non-prescirbed — not a priori in blueprint — measurements into blueprint units titled 
#    "Assessed Not in Blueprint" when encountered. We want to back these out.  
#    ① Begin by partitioning stg_crs_unit_alias into prescribed and non-prescribed units. For each case the
#       tuple (SIH_COURSEPK_ID, LEARNING_STANDARD_ID) interests us principally. 
stg_crs_unit_partition = {'assessed_not_in_blueprint' : stg_crs_unit_alias.loc[[bool(re.search('Assessed Not in Blueprint', unit_nm)) 
																					for unit_nm in stg_crs_unit_alias['UNIT_NM']],
																				['SIH_COURSEPK_ID', 
																				 'LEARNING_STANDARD_ID']]
																		   .drop_duplicates()\
																		   .reset_index(drop = True)\
																		   .assign(COURSE_BLUEPRINT_ALIGNMENT = 'assessed_not_in_blueprint'),
						  'blueprint_measurement' : stg_crs_unit_alias.loc[[not bool(re.search('Assessed Not in Blueprint', unit_nm)) 
																					for unit_nm in stg_crs_unit_alias['UNIT_NM']],
																			['SIH_COURSEPK_ID', 
																			 'LEARNING_STANDARD_ID']]
																	   .drop_duplicates()\
																	   .reset_index(drop = True)\
																	   .assign(COURSE_BLUEPRINT_ALIGNMENT = 'blueprint_measurement') }
stg_crs_unit_partition.update({'assessed_not_in_blueprint' : pd.merge(left = pd.merge(left = stg_crs_unit_partition.get('assessed_not_in_blueprint')[['SIH_COURSEPK_ID',
																																					  'LEARNING_STANDARD_ID']],
																					  right = stg_crs_unit_partition.get('blueprint_measurement')[['SIH_COURSEPK_ID',
																																				   'LEARNING_STANDARD_ID']]\
																																				.assign(blueprint_measurement = 'Y'),
																					  how = 'left').fillna(value = 'N')\
																								   .set_index(keys = 'blueprint_measurement')\
																								   .loc['N']\
																								   .reset_index(drop = True),
																	  right = stg_crs_unit_partition.get('assessed_not_in_blueprint')) })
#
#    ② We now manipulate each our body of evidence, student knowledge level, and proficiency span in order to to
#       adjust the state learning standards in non-prescribed units.  
#       ⓐ Beginning with student knowledge level, we want to excise KNOWLEDGE_LVL_TYPE = "MEASURED" (SIH_COURSEPK_ID, LEARNING_STANDARD_ID)
#          tuples correspoiding to non-prescribed units. We assume here that our non-prescribed units are knowledge-graph singletons. Dropping 
#          these specific records is a bit of a tricky endeavor.  We accomplish this by setting surrogate key STUDENT_KNOWLEDGE_LVL_SID
#          as an index and then dropping selected records.
#           ⅰ. First, "Window" skl_aliased by KNOWLEDGE_LVL_TYPE to get only "MEASURED" records. Employ pandas set_index operation
#              to recover (SIH_COURSEPK_ID, LEARNING_STANDARD_ID, STUDENT_KNOWLEDGE_LVL_SID) triples corresponding to MEASURED
#              records.
#           ⅱ. Join the result from ⅰ onto the nonprescribed_units partion of stg_crs_unit_partition. Extract only the
#              (SIH_COURSEPK_ID, LEARNING_STANDARD_ID) tuple for these purposes.  This result produces a list of
#              STUDENT_KNOWLEDGE_LVL_SID surrogate-key values corresponding to the records we want to excise.
#           ⅲ. Reindex skl_aliased by STUDENT_KNOWLEDGE_LVL_SID. Then employ pandas drop to eliminate
#              the indices identified above by ⅱ. 
skl_aliased = skl_aliased.set_index(keys = 'STUDENT_KNOWLEDGE_LVL_SID',
									drop = False)\
						 .drop(labels = pd.merge(left = skl_aliased.set_index(keys = 'KNOWLEDGE_LVL_TYPE',
																			  drop = False)\
																   .loc['MEASURED', ['SIH_COURSEPK_ID',
																					 'LEARNING_STANDARD_ID',
																					 'STUDENT_KNOWLEDGE_LVL_SID']]\
																   .reset_index(drop = True),
												 right = stg_crs_unit_partition.get('assessed_not_in_blueprint')[['SIH_COURSEPK_ID',
																												  'LEARNING_STANDARD_ID']]\
																												.drop_duplicates())\
																												['STUDENT_KNOWLEDGE_LVL_SID']\
																												.tolist(),
							   axis = 0).reset_index(drop = True)
#
#       ⓑ proficiency_span requires similar treatment.  We specifically excise from proficiency_span records for which
#          BLUEPRINT_ALGNED_LRN_STD = "Y" when corresponding (SIH_COURSEPK_ID, LEARNING_STANDARD_ID) tuple appears in the
#          nonprescribed_units. 
#           ⅰ. Use pandas dataframe indexing to get the (SIH_COURSEPK_ID, LEARNING_STANDARD_ID, PROF_TUPLE) triples
#              correspoinding to the BLUEPRINT_ALGNED_LRN_STD = "Y" records.
#           ⅱ. Join the result from ⅰ onto the nonprescribed_units partion of stg_crs_unit_partition. Extract only the
#              (SIH_COURSEPK_ID, LEARNING_STANDARD_ID) tuple for these purposes.  This result produces a list of
#              PROF_TUPLE values corresponding to the records we want to excise.
#           ⅲ. Reindex prof_span_aliased by PROF_TUPLE, and use pandas data-frame drop to remove records identified
#              by ⅱ.
prof_span_aliased = prof_span_aliased.set_index(keys = 'PROF_TUPLE',
												drop = False)\
									 .drop(labels = pd.merge(left = prof_span_aliased.set_index(keys = 'BLUEPRINT_ALGNED_LRN_STD',
																								drop = False)\
																					  .loc['Y', ['SIH_COURSEPK_ID',
																								 'LEARNING_STANDARD_ID',
																								 'PROF_TUPLE',
																								 'BLUEPRINT_ALGNED_LRN_STD']]\
																					  .reset_index(drop = True),
															 right = stg_crs_unit_partition.get('assessed_not_in_blueprint')[['SIH_COURSEPK_ID',
																															  'LEARNING_STANDARD_ID']]\
																															.drop_duplicates())\
																															['PROF_TUPLE']\
																															.tolist(),
										   axis = 0).reset_index(drop = True)
#
#       ⓑ body_of_evidence involves a slightly-different operation. We must reassign alignment-indicator
#          attributes for measurements correspoinding to (SIH_COURSEPK_ID, LEARNING_STANDARD_ID) tuples in
#          nonprescribed_units. We accomplish this by partioning boe_aliased into records within the
#          nonprescribed_units span and those outside of the span. For thise within the span, we we reassign
#          COURSE_STANDARD_ALIGNMENT to "Unaligned Standard".
boe_aliased = pd.merge(left = boe_aliased.drop_duplicates(),
					   right = pd.concat(objs = stg_crs_unit_partition.values(),
										 sort = False).drop_duplicates(),
					   how = 'left').fillna(value = 'unaligned_standard')

pd.pivot_table(data = boe_aliased.assign(evidentiary_tuple = 1),
			   values = 'evidentiary_tuple',
			   index = ['CAMPUS',
						'COURSE_SUBJECTS',
						'COURSE_GRADE_LVL'],
			   columns = 'COURSE_BLUEPRINT_ALIGNMENT',
			   aggfunc = np.sum).fillna(value = 0)\
								.reset_index(drop = False)#\
								# .to_excel(excel_writer = os.path.abspath(os.path.join(session_attributes.get('target_dir'), 
								#                                                     session_attributes.get('benchmark_time') +\
								#                                                     '_BLUEPRINT_ALGMT_ANALYSIS.xlsx')),
								#         sheet_name = 'Blueprint-Algmt Summy',
								#         index = False)
#
# Ⓒ Unwind adaptive extensions of the learning-standard progressions.  "Off-axis" graph edges — edges connecting
#    measured learning standards that are hierarchically removed from the learning-standards progressions graph —
#    have been incorporated. We seek to back these out.  We want begin with the original-source graphs.  
#
#    Relating measured learning standards to their distance to learning standards in the progression 
#    represents or objective. A measured learning standard is "on-axis" with respect to the learning-standard
#    progressions graph, or "off-axis". Off-axis measurements are one step graphically removed from
#    progressions in the learning-standard progressions.  We want to label the "off-axis" measurements
#    as either "upward-extensible" if an upward extension of the progressions connects it to progression graph,
#    or "downward-extensible" of extending the graph hierarchically down connects it to the graph. We only
#    consider upward- and downward-extension for the case of learning standard in the course blueprint.
#    Propagation loss  will render neglible the influence by any off-axis prior-year measurements.
#
#    ① Read in the comma-separated variable (csv) representations of the curriculum graph. Each type of
#       graph edge is partitioned into a distinct file. We read each of these files in and concatenate the results.
#       While doing so, we join on human-recognizable learning-standard codes,  so that the results are accessible
#       for visual inspection. 
curric_graph_edges = fct.reduce(lambda x, y: pd.merge(left = x,
													 right = y,
													 how = 'left'),
													 [pd.concat(objs  = [pd.read_csv(filepath_or_buffer = os.path.abspath(os.path.join(session_attributes.get('edgate_standards_directory'), 
																													filename)),
																					  dtype = str,
																					  usecols = ['GRAPH_TYPE',
																								 'CONSTITUENT_LEARNING_STD_ID',
																								 'LEARNING_STANDARD_ID'])
																				for filename in os.listdir(session_attributes.get('edgate_standards_directory'))
																				if (bool(re.search(session_attributes.get('juiris_filename_signature'), filename))
																							and not  bool(re.search('2018', filename)))],
																sort = False),
													  lrn_std_id_cd[['SUBJECT_TITLE',
																	 'GRADE_LEVEL',
																	 'LEARNING_STANDARD_ID',
																	 'LEARNING_STANDARD_CD']],
													  lrn_std_id_cd[['LEARNING_STANDARD_ID',
																	 'LEARNING_STANDARD_CD']]\
																   .rename(columns = {'LEARNING_STANDARD_ID' : 'CONSTITUENT_LEARNING_STD_ID',
																					  'LEARNING_STANDARD_CD' : 'CONSTITUENT_LEARNING_STD_CD'})]).fillna(value = '')
#
#    ② Perform edge-occurence analysis. Some edges appear in different partitions. 

edge_analysis =\
fct.reduce(lambda x, y: pd.merge(left = x,
								 right = y,
								 how = 'left'),
								[curric_graph_edges.set_index(keys = 'GRAPH_TYPE')\
												   .loc[edge_type, ['SUBJECT_TITLE',
																	'CONSTITUENT_LEARNING_STD_CD',
																	'LEARNING_STANDARD_CD']]\
												   .assign(edge_type = 1)\
												   .rename(columns = {'edge_type' : edge_type})
									for edge_type in set(curric_graph_edges['GRAPH_TYPE'])] ).fillna(value = 0)\
																							 .assign(edge_occurrences = lambda x : x[list(set(curric_graph_edges['GRAPH_TYPE']))].sum(axis = 1))\
																							 .sort_values(by = ['edge_occurrences',
																												'SUBJECT_TITLE',
																												'CONSTITUENT_LEARNING_STD_CD'],
																										  axis = 0,
																										  ascending = [False, True, True])																							 
# edge_analysis.to_excel(excel_writer = os.path.abspath(os.path.join(session_attributes.get('target_dir'), 
#                                                                 session_attributes.get('benchmark_time') +\
#                                                                 '_CURRIC_GRAPH_EDGE_ANALYSIS.xlsx')),
#                      sheet_name = 'Graph-Edge Incidences',
#                      index = False)
#
#      ③ We now build networkx directed-graph objects for the progressions and hierarchy graphs.  We get these
#         by re-partitioning our curriculum-graph edge list curric_graph_edges and filtering the results.  We 
#         specify our edges as surrogate-key learning-standard IDs.  These are integer labels.
curric_prog_graph = nx.DiGraph()
curric_prog_graph.add_edges_from(curric_graph_edges[['GRAPH_TYPE',
													'CONSTITUENT_LEARNING_STD_ID',
													'LEARNING_STANDARD_ID']]\
												  .set_index(keys = 'GRAPH_TYPE')\
												  .loc[['PROGRESSION',
														'UNPACKED']]\
												  .drop_duplicates()\
												  .to_records(index = False)\
												  .tolist())
# (lrn_std, course) = ('642222',
#   ['6333412354941264044', '6333412354941263935', '6333412354941264123'])


# [list(it.product(nx.ego_graph(G  = curric_prog_graph.reverse(),      # ⬅️ Predecessor vertices
# 							  n = lrn_std,
# 							  radius = 2,
# 							  undirected = False).reverse()
# 												 .nodes(),
# 			   [course])) +\
#  list(it.product(nx.ego_graph(G  = curric_prog_graph,      # ⬅️ Successor vertices
# 							  n = lrn_std,
# 							  radius = 1,
# 							  undirected = False).nodes(),
# 				 [course]))]



# nx.ego_graph(G  = curric_prog_graph.reverse(),      # ⬅️ Predecessor vertices
# 			 n = lrn_std,
# 			 radius = 1,
# 			 undirected = False).reverse().edges()
# curric_graph_edges.set_index(keys = 'GRAPH_TYPE')\
# 				   .loc[['PROGRESSION', 'UNPACKED']]\
# 				   .set_index(keys = 'LEARNING_STANDARD_ID',
# 							  drop = False)\
# 				   .loc[[lrn_std], ['CONSTITUENT_LEARNING_STD_ID', 'LEARNING_STANDARD_ID']]


# nx.ego_graph(G  = curric_prog_graph,      # ⬅️ Successor vertices
# 			 n = lrn_std,
# 			 radius = 1,
# 			 undirected = False).edges()
# curric_graph_edges.set_index(keys = 'GRAPH_TYPE')\
# 				   .loc[['PROGRESSION', 'UNPACKED']]\
# 				   .set_index(keys = 'CONSTITUENT_LEARNING_STD_ID',
# 							  drop = False)\
# 				   .loc[[lrn_std], ['CONSTITUENT_LEARNING_STD_ID', 'LEARNING_STANDARD_ID']]

# fct.reduce(lambda x, y: pd.merge(left = x,
# 								 right = y),
# 								 [pd.DataFrame(data = list(it.product(nx.ego_graph(G  = curric_prog_graph.reverse(),      # ⬅️ Predecessor vertices
# 																				   n = lrn_std,
# 																				   radius = 2,
# 																				   undirected = False).reverse()
# 																									  .nodes(),
# 																	  course)),
# 											   columns = ['LEARNING_STANDARD_ID',
# 														  'SIH_COURSEPK_ID']),

# 								  lrn_std_id_cd[['SUBJECT_TITLE',
# 								  				 'GRADE_LEVEL',
# 								  				 'LEARNING_STANDARD_ID']],
# 								  .get('blueprint_measurement')]

# pd.concat(objs = [pd.DataFrame(data = list(it.product(nx.ego_graph(G  = curric_prog_graph.reverse(),      # ⬅️ Predecessor vertices
# 																   n = lrn_std,
# 																   radius = 2,
# 																   undirected = False).reverse()
# 																					  .nodes(),
# 													  course)),
# 							   columns = ['LEARNING_STANDARD_ID',
# 										  'SIH_COURSEPK_ID'])
# 					for (lrn_std, course ) in pd.merge(left = pd.concat(objs = stg_crs_unit_partition.values())\
# 																					 .drop(labels = 'COURSE_BLUEPRINT_ALIGNMENT',
# 																						   axis = 1)\
# 																					 .drop_duplicates()\
# 																					 .groupby(by = 'LEARNING_STANDARD_ID',
# 																							  as_index= False)\
# 																					 .agg(list),
# 														right = pd.DataFrame(data = {'LEARNING_STANDARD_ID' : list(curric_prog_graph)})).to_records(index = False)\
# 																																		.tolist()])



crs_std_est_range = pd.DataFrame(data = list(it.chain(*[list(it.product(nx.ego_graph(G  = curric_prog_graph.reverse(),      # ⬅️ Predecessor vertices
																					  n = lrn_std,
																					  radius = 2,
																					  undirected = False).reverse()
																										 .nodes(),
																	   course)) +\
														 list(it.product(nx.ego_graph(G  = curric_prog_graph,      # ⬅️ Successor vertices
																					  n = lrn_std,
																					  radius = 1,
																					  undirected = False).nodes(),
																		 course))
														  for (lrn_std, course ) in pd.merge(left = pd.concat(objs = stg_crs_unit_partition.values())\
																														 .drop(labels = 'COURSE_BLUEPRINT_ALIGNMENT',
																															   axis = 1)\
																														 .drop_duplicates()\
																														 .groupby(by = 'LEARNING_STANDARD_ID',
																																  as_index= False)\
																														 .agg(list),
																							right = pd.DataFrame(data = {'LEARNING_STANDARD_ID' : list(curric_prog_graph)})).to_records(index = False)\
																																											.tolist() ]  )),
								 columns = ['LEARNING_STANDARD_ID',
											'SIH_COURSEPK_ID']).drop_duplicates()\
															   .reset_index(drop = True)





crs_std_est_range = pd.DataFrame(data = list(it.chain(*[list(it.product(nx.ego_graph(G  = curric_prog_graph.reverse(),      # ⬅️ Predecessor vertices
																					  n = lrn_std,
																					  radius = 2,
																					  undirected = False).reverse()
																										 .nodes(),
																	   course)) +\
														 list(it.product(nx.ego_graph(G  = curric_prog_graph,      # ⬅️ Successor vertices
																					  n = lrn_std,
																					  radius = 1,
																					  undirected = False).nodes(),
																		 course))
														  for (lrn_std, course ) in pd.merge(left = pd.concat(objs = stg_crs_unit_partition.values())\
																														 .drop(labels = 'COURSE_BLUEPRINT_ALIGNMENT',
																															   axis = 1)\
																														 .drop_duplicates()\
																														 .groupby(by = 'LEARNING_STANDARD_ID',
																																  as_index= False)\
																														 .agg(list),
																							right = pd.DataFrame(data = {'LEARNING_STANDARD_ID' : list(curric_prog_graph)})).to_records(index = False)\
																																											.tolist() ]  )),
								 columns = ['LEARNING_STANDARD_ID',
											'SIH_COURSEPK_ID']).drop_duplicates()\
															   .reset_index(drop = True)


curric_hierarch_graph = nx.DiGraph()
curric_hierarch_graph.add_edges_from(curric_graph_edges[['GRAPH_TYPE',
														'CONSTITUENT_LEARNING_STD_ID',
														'LEARNING_STANDARD_ID']]\
													  .set_index(keys = 'GRAPH_TYPE')\
													  .loc['Hierarchy']\
													  .drop_duplicates()\
													  .to_records(index = False)\
													  .tolist())


boe_aliased = pd.merge(left = boe_aliased.drop(labels = 'STD_PROGRESSION_ALIGNMENT',
											   axis = 1),
					   right = pd.DataFrame(data = {'LEARNING_STANDARD_ID' : list(curric_prog_graph.nodes())}).assign(STD_PROGRESSION_ALIGNMENT = 'Aligned Standard'),
					   how = 'left').fillna(value = 'Unaligned Standard')


meas_alignment = pd.concat(objs = [fct.reduce(lambda x, y: pd.merge(left = x,
																  right = y),
																  [boe_aliased.set_index(keys = 'STD_PROGRESSION_ALIGNMENT')\
																			  .loc['Unaligned Standard']\
																			  .reset_index(drop = True)\
																			  .set_index(keys = 'COURSE_BLUEPRINT_ALIGNMENT')\
																			  .loc[['assessed_not_in_blueprint',
																					'blueprint_measurement'], ['SIH_COURSEPK_ID',
																											   'LEARNING_STANDARD_ID',
																											   'LEARNING_STANDARD_CD']]\
																			  .drop_duplicates()\
																			  .reset_index(drop = True)\
																			  .rename(columns = {'LEARNING_STANDARD_ID' : 'measured_std'}),
																   curric_graph_edges.set_index(keys = 'GRAPH_TYPE')\
																					 .loc[['Hierarchy'], ['CONSTITUENT_LEARNING_STD_ID',
																										  'LEARNING_STANDARD_ID']]\
																					 .reset_index(drop = True)\
																					 .rename(columns = {'CONSTITUENT_LEARNING_STD_ID' : 'progression_std',
																										'LEARNING_STANDARD_ID' : 'measured_std'})\
																					 .assign(STD_PROGRESSION_ALIGNMENT = 'extensible_upward'),
																   pd.DataFrame(data = {'progression_std' : list(curric_prog_graph.nodes())}) ] ).drop(labels = 'progression_std',
																																					   axis = 1)\
																																				 .rename(columns = {'measured_std' : 'LEARNING_STANDARD_ID'}),
								 fct.reduce(lambda x, y: pd.merge(left = x,
																  right = y),
																  [boe_aliased.set_index(keys = 'STD_PROGRESSION_ALIGNMENT')\
																			  .loc['Unaligned Standard']\
																			  .reset_index(drop = True)\
																			  .set_index(keys = 'COURSE_BLUEPRINT_ALIGNMENT')\
																			  .loc[['assessed_not_in_blueprint',
																					'blueprint_measurement'], ['SIH_COURSEPK_ID',
																											   'LEARNING_STANDARD_ID',
																											   'LEARNING_STANDARD_CD']]\
																			  .drop_duplicates()\
																			  .reset_index(drop = True)\
																			  .rename(columns = {'LEARNING_STANDARD_ID' : 'measured_std'}),
																   curric_graph_edges.set_index(keys = 'GRAPH_TYPE')\
																					 .loc[['Hierarchy'], ['CONSTITUENT_LEARNING_STD_ID',
																										  'LEARNING_STANDARD_ID']]\
																					 .reset_index(drop = True)\
																					 .rename(columns = {'CONSTITUENT_LEARNING_STD_ID' : 'measured_std',
																										'LEARNING_STANDARD_ID' : 'progression_std'})\
																					 .assign(STD_PROGRESSION_ALIGNMENT = 'extensible_downward'),
																   pd.DataFrame(data = {'progression_std' : list(curric_prog_graph.nodes())}) ] ).drop(labels = 'progression_std',
																																					   axis = 1)\
																																				 .rename(columns = {'measured_std' : 'LEARNING_STANDARD_ID'}),
								 boe_aliased.set_index(keys = 'STD_PROGRESSION_ALIGNMENT',
													   drop = False)\
											.loc['Aligned Standard', ['SIH_COURSEPK_ID',
																	  'LEARNING_STANDARD_ID',
																	  'LEARNING_STANDARD_CD',
																	  'STD_PROGRESSION_ALIGNMENT']]\
											.reset_index(drop = True)\
											.drop_duplicates()],
						   sort = False)

boe_aliased = pd.merge(left = boe_aliased.drop(labels = 'STD_PROGRESSION_ALIGNMENT',
											   axis = 1),
					   right = meas_alignment,
					   how = 'left').fillna(value = 'Unaligned Standard')


pd.pivot_table(data = boe_aliased.assign(evidentiary_tuple = 1),
			   values = 'evidentiary_tuple',
			   index = ['COURSE_BLUEPRINT_ALIGNMENT'],
			   columns = 'STD_PROGRESSION_ALIGNMENT',
			   aggfunc = np.sum).fillna(value = 0)\
								.reset_index(drop = False)

pd.pivot_table(data = boe_aliased.assign(evidentiary_tuple = 1),
			   values = 'evidentiary_tuple',
			   index = ['CAMPUS',
						'COURSE_SUBJECTS',
						'COURSE_GRADE_LVL'],
			   columns = 'STD_PROGRESSION_ALIGNMENT',
			   aggfunc = np.sum).fillna(value = 0)\
								.reset_index(drop = False)#\
								# .to_excel(excel_writer = os.path.abspath(os.path.join(session_attributes.get('target_dir'), 
								#                                                     session_attributes.get('benchmark_time') +\
								#                                                     '_PROGRESSION_ALGMT_ANALYSIS.xlsx')),
								#         sheet_name = 'Prog-Algmt Summy',
								#         index = False)


set(boe_aliased['STD_PROGRESSION_ALIGNMENT'])

algmt_state = 'Aligned Standard'


fct.reduce(lambda x, y: pd.merge(left = x,
								 right = y,
								 how = 'outer'),
								[boe_aliased[['CAMPUS',
											 'COURSE_SUBJECTS',
											 'COURSE_GRADE_LVL',
											 'COURSE_ID',
											 'COURSE_TITLE',
											 'STD_PROGRESSION_ALIGNMENT',
											 'LEARNING_STANDARD_CD']]\
										   .drop_duplicates()\
										   .groupby(by = ['CAMPUS',
														  'COURSE_SUBJECTS',
														  'COURSE_GRADE_LVL',
														  'COURSE_ID',
														  'COURSE_TITLE',
														  'STD_PROGRESSION_ALIGNMENT'],
												   as_index = False)\
										   .agg(set)\
										   .set_index(keys = 'STD_PROGRESSION_ALIGNMENT')\
										   .loc[[algmt_state]]\
										   .reset_index(drop = True)\
										   .rename(columns = {'LEARNING_STANDARD_CD' : algmt_state})
								 for algmt_state in set(boe_aliased['STD_PROGRESSION_ALIGNMENT'])] ).fillna(value = '')\
																									.reset_index(drop = True)#\
																										# .to_excel(excel_writer = os.path.abspath(os.path.join(session_attributes.get('target_dir'), 
																										#                                                     session_attributes.get('benchmark_time') +\
																										#                                                     '_PROGRESSION_ALGMT_SUMMY.xlsx')),
																										#         sheet_name = 'Prog-Algmt Summy',
																										#         index = False)



#
# Ⓒ Evaluate the evidentiary coverage of the proficiency span. We want the projection of body_of_evidence onto
#    proficiency span. We are projecting in the (course, student, standards) dimensions.  The percentage of 
#    course-blueprint learning standards for which the average course-enrolled student has at least one
#    learning standard represents our statistic of interest. We further want to summarized by SIH_COURSEPK_ID
#    and measurement date.  proficiency_span gives us the addressible measuremnt space. We "window" proficiency
#    span so as to only include learning standard in the course blueprints for each course.
#    ① We first need three essential statistics for each (course, measurment-date) couple:
#       ⧐ enrollees is the count of actively-enrolled students for each ;
#       ⧐ standards is the count of learning standards in the course blueprint;
#       ⧐ measurements_recorded is the count of student/learning-standards measurements
#         are recorded.
#    We first project body_of_evidence onto proficiency_span, via a left-join. We get our statistics
#    by applying pandas groupby/count-aggregation to subsets of our projection.  We join all of these results
#    together.
crs_evidentiary_accrual = pd.concat(
					objs = [fct.reduce(lambda x, y: pd.merge(left = x,
															 right = y),
													[evid_projection.assign(measurements_recorded = [0 if pd.isna(raw_score) 
																									  else 1 
																									for raw_score in evid_projection['ASSESSMENT_DATE']])\
																	.groupby(by = ['SIH_COURSEPK_ID',
																				   'ASSESSMENT_DATE'],
																			 as_index = False)\
																	.sum(),
													 evid_projection.assign(enrollees = 1)\
																	.drop_duplicates(subset = ['SIH_COURSEPK_ID',
																							   'STUDENT_ID'],
																					 keep = 'first')\
																	.groupby(by = 'SIH_COURSEPK_ID',
																			 as_index = False)\
																	.sum(),
													 evid_projection.assign(standards = 1)\
																	.drop_duplicates(subset = ['SIH_COURSEPK_ID',
																							   'LEARNING_STANDARD_ID'],
																					 keep = 'first')\
																	.groupby(by = 'SIH_COURSEPK_ID',
																			 as_index = False)\
																	.sum()])
														for evid_projection in 
															[pd.merge(left = prof_span_aliased.set_index(keys = 'BLUEPRINT_ALGNED_LRN_STD',
																										 drop = False)\
																							  .loc['Y']\
																							  .reset_index(drop = True)\
																							  [['SIH_COURSEPK_ID',
																							   'STUDENT_ID',
																							   'LEARNING_STANDARD_ID']],
																	  right = boe_aliased[['SIH_COURSEPK_ID',
																						   'STUDENT_ID',
																						   'LEARNING_STANDARD_ID',\
																						   'ASSESSMENT_DATE']],
																	  how = 'left').drop_duplicates()]],
					sort = True ).sort_values(by = ['SIH_COURSEPK_ID',
													'ASSESSMENT_DATE'])
#
#    ② Now, calculate a "coverage factor" for each (course, measurement-date) tuple. This coverage factor
#      is the proportion of the total measurable space — enrollees × standards — for which measurements are
#      recorded for each tuple.  Our coverage factor is measurements_recorded ÷ (enrollees × standards).
crs_evidentiary_accrual = crs_evidentiary_accrual.assign(covg_factor = [meas/(stud*std)
																		for (meas, stud, std) in crs_evidentiary_accrual[['measurements_recorded',
																														  'enrollees',
																														  'standards']]\
																														.to_records(index = False)\
																														.tolist()])
#
#    ③ We finally want a cumulative coverage factor, accounting for how over time the evidentiary coverage
#       increases. This cumulative-coverage factor cum_covgᵀ = ∑ᵢᵀ covg_factorᵢ. Getting this is a bit tricky.  
#       We list-aggregate — groupby, agg(list) — our (covg_factor, measurement date, course) triples
#       by course.  What resiults is a dataframe with lists by course of measurement dates and
#       corresponding coverage factors. We pairwise combine the measurement date with the
#       numpy.cumsum of the coverage factor list, and expand the results out into a dataframe
#       of (course, date, cum_covg) tuples. 
#    ④ Expand the result from ③ by joining in course-identifying attributes from proficiency_span, body_of_evidence.
crs_evidentiary_accrual = fct.reduce(lambda x, y: pd.merge(left = x,
														   right = y),
													[pd.concat(objs =  [pd.DataFrame(data = cum_covg_tuple,
																					 columns = ['SIH_COURSEPK_ID',
																								'ASSESSMENT_DATE',
																								'cum_covg'])
																		for cum_covg_tuple in 
																			[list(map(lambda x : [sec] + list(x), list(zip(meas_date, np.cumsum(covg_factor)))))
																				for (sec, meas_date, covg_factor) in 
																					crs_evidentiary_accrual.drop(labels = ['measurements_recorded',
																														   'enrollees',
																														   'standards'],
																												 axis = 1)\
																											.groupby(by = ['SIH_COURSEPK_ID'],
																													 as_index = False)\
																											.agg(list)\
																											.to_records(index = False)\
																											.tolist() ] ],
																sort = True),
													 crs_evidentiary_accrual.assign(possible_measurements = [stud*std
																					for (meas, stud, std) in crs_evidentiary_accrual[['measurements_recorded',
																																	  'enrollees',
																																	  'standards']]\
																																	.to_records(index = False)\
																																	.tolist()]),
													 boe_aliased[['SIH_COURSEPK_ID',
																  'ASSESSMENT_DATE',
																  'LEARNING_STANDARD_CD']].drop_duplicates()\
																.groupby(by = ['SIH_COURSEPK_ID',
																			   'ASSESSMENT_DATE'],
																		 as_index = False)\
																.agg(set)\
																.rename(columns = {'LEARNING_STANDARD_CD' : 'learning_standards_measured'}),
													 prof_span_aliased[['CAMPUS',
																		'COURSE_SUBJECTS',
																		'COURSE_GRADE_LVL',
																		'COURSE_ID',
																		'COURSE_TITLE',
																		'TEACHER_LNAME',
																		'TEACHER_FNAME',
																		'SIH_COURSEPK_ID']]\
																	  .drop_duplicates() ] )\
																	  .sort_values(by = ['CAMPUS',
																						 'COURSE_SUBJECTS',
																						 'COURSE_GRADE_LVL',
																						 'COURSE_ID',
																						 'COURSE_TITLE',
																						 'TEACHER_LNAME',
																						 'ASSESSMENT_DATE'])\
																	  [['CAMPUS',
																		'COURSE_SUBJECTS',
																		'COURSE_GRADE_LVL',
																		'COURSE_ID',
																		'COURSE_TITLE',
																		'TEACHER_LNAME',
																		'TEACHER_FNAME',
																		'enrollees',
																		'standards',
																		'possible_measurements',
																		'ASSESSMENT_DATE',
																		'learning_standards_measured',
																		'measurements_recorded',
																		'covg_factor',
																		'cum_covg',
																		'SIH_COURSEPK_ID']]
#
# Ⓒ Produce an aggregate evidentiary-coverage analysis. Most of the work here was already done in constructing 
#    our body_of_evidence query. We merely perform here some summary analysis.  Our body_of_evidence contains
#    three essential alignment-related attributes: 
#    ⧐ COURSE_STANDARD_ALIGNMENT indicates whether the measured learning standard appears either in the 
#      course blueprint or in graphical predecessors thereof in the learning-standard progressions;
#    ⧐ LEARNING_STANDARD_AY indicates whether the measured learning standard appears in the course blu
agg_algmt_blueprint = pd.merge(left = boe_aliased[['SIH_COURSEPK_ID',
													 'COURSE_BLUEPRINT_ALIGNMENT',
													 'LEARNING_STANDARD_AY',
													 'AY_OF_MEASUREMENT']]\
													.assign(measurement_count = 1)\
													.groupby(by = ['SIH_COURSEPK_ID',
																   'COURSE_BLUEPRINT_ALIGNMENT',  #⬅️👹👺🐞🕷🦗🐜
																   'LEARNING_STANDARD_AY',
																   'AY_OF_MEASUREMENT'],
															 as_index = False)\
													.agg(sum),
								 right = boe_aliased[['SIH_COURSEPK_ID',
													  'CAMPUS',
													  'COURSE_SUBJECTS',
													  'COURSE_GRADE_LVL',
													  'COURSE_ID',
													  'COURSE_TITLE',
													  'TEACHER_LNAME']]\
													.drop_duplicates())[['CAMPUS',
																		 'COURSE_SUBJECTS',
																		 'COURSE_GRADE_LVL',
																		 'COURSE_ID',
																		 'COURSE_TITLE',
																		 'TEACHER_LNAME',
																		 'COURSE_BLUEPRINT_ALIGNMENT',
																		 'LEARNING_STANDARD_AY',
																		 'AY_OF_MEASUREMENT',
																		 'measurement_count',
																		 'SIH_COURSEPK_ID']]\
																	   .sort_values(by = ['CAMPUS',
																						  'COURSE_SUBJECTS',
																						  'COURSE_GRADE_LVL',
																						  'COURSE_ID',
																						  'COURSE_TITLE'])
agg_algmt_progression = pd.merge(left = boe_aliased[['SIH_COURSEPK_ID',
													 'STD_PROGRESSION_ALIGNMENT',
													 'LEARNING_STANDARD_AY',
													 'AY_OF_MEASUREMENT']]\
													.assign(measurement_count = 1)\
													.groupby(by = ['SIH_COURSEPK_ID',
																   'STD_PROGRESSION_ALIGNMENT',  #⬅️👹👺🐞🕷🦗🐜
																   'LEARNING_STANDARD_AY',
																   'AY_OF_MEASUREMENT'],
															 as_index = False)\
													.agg(sum),
								 right = boe_aliased[['SIH_COURSEPK_ID',
													  'CAMPUS',
													  'COURSE_SUBJECTS',
													  'COURSE_GRADE_LVL',
													  'COURSE_ID',
													  'COURSE_TITLE',
													  'TEACHER_LNAME']]\
													.drop_duplicates())[['CAMPUS',
																		 'COURSE_SUBJECTS',
																		 'COURSE_GRADE_LVL',
																		 'COURSE_ID',
																		 'COURSE_TITLE',
																		 'TEACHER_LNAME',
																		 'STD_PROGRESSION_ALIGNMENT',
																		 'LEARNING_STANDARD_AY',
																		 'AY_OF_MEASUREMENT',
																		 'measurement_count',
																		 'SIH_COURSEPK_ID']]\
																	   .sort_values(by = ['CAMPUS',
																						  'COURSE_SUBJECTS',
																						  'COURSE_GRADE_LVL',
																						  'COURSE_ID',
																						  'COURSE_TITLE'])

#
# Ⓓ Perform for student_knowledge_level analaysis similar to that for body_of_evidence in Ⓒ.
#    Our student_knowledge_level contains measurements of learning standards not in the progressions.
#    Many of these  are — in a hierarchical-graph sense — one step away from standards in the hierarchy. 
#    We want to identify these and introduce them as KNOWLEDGE_LVL_TYPE = "ESTIMATED" in our 
#    student_knowledge_level table.  
unmeas_tuples_in_est_range = pd.merge(left = skl_aliased.set_index(keys = 'KNOWLEDGE_LVL_TYPE',
																   drop = False)\
													    .loc['UNMEASURED'],
									  right = crs_std_est_range)['PROFICIENCY_SPACE_TUPLE']


skl_aliased = pd.concat(objs = [skl_aliased.set_index(keys = 'PROFICIENCY_SPACE_TUPLE',
													drop = False)\
										 .loc[unmeas_tuples_in_est_range]\
										 .assign(KNOWLEDGE_LVL_TYPE = 'ESTIMATED'),
							   skl_aliased.set_index(keys = 'PROFICIENCY_SPACE_TUPLE',
													 drop = False)\
										   .drop(labels = unmeas_tuples_in_est_range,
												 axis = 0) ],
					  sort = False).sort_values(by = ['CAMPUS',
													  'COURSE_SUBJECTS',
													  'COURSE_GRADE_LVL',
													  'COURSE_ID',
													  'TEACHER_LNAME',
													  'STUDENT_LNAME',
													  'STUDENT_FNAME',
													  'LEARNING_STANDARD_CD'])\
								   .reset_index(drop = True)\
								   [skl_aliased.columns]

pd.pivot_table(data = skl_aliased.assign(proficiency_tuple = 1),
			   values = 'proficiency_tuple',
			   index = ['CAMPUS',
						'COURSE_SUBJECTS',
						'COURSE_GRADE_LVL'],
			   columns = 'KNOWLEDGE_LVL_TYPE',
			   aggfunc = np.sum).fillna(value = 0)\
								.reset_index(drop = False)


#    We think of this however as an coverage analysis analysis. Now, student_knowledge_level
#    contains exactly one record for each (course, student, learning-standard) triple in the 
#    proficiency-space span. Our relevant category attributes are 
#    ⧐ KNOWLEDGE_LVL_TYPE indicates whether the (course, student, learning-standard) triple is
#      MEASURED, ESTIMATED, or UNMEASURED;
#    ⧐ LEARNING_STD_IN_CURRENT_AY indicates whether the (course, learning-standard) couple is
#      in the course blueprint for the corresponding course.
#    We want counts by (course, KNOWLEDGE_LVL_TYPE, LEARNING_STD_IN_CURRENT_AY).  Get this by a 
#    groupby/count-aggregation operation. Then join in course-identifying attributes.
evid_est_covg = pd.merge(left = skl_aliased[['SIH_COURSEPK_ID',
											 'KNOWLEDGE_LVL_TYPE',
											 'LEARNING_STD_IN_CURRENT_AY']]\
											.assign(meas_est_covg = 1)\
											.groupby(by = ['SIH_COURSEPK_ID',
														   'KNOWLEDGE_LVL_TYPE',
														   'LEARNING_STD_IN_CURRENT_AY'],
													 as_index = False)\
											.agg(sum),
						 right = skl_aliased[['SIH_COURSEPK_ID',
											  'CAMPUS',
											  'COURSE_SUBJECTS',
											  'COURSE_GRADE_LVL',
											  'COURSE_ID',
											  'COURSE_TITLE',
											  'TEACHER_LNAME']]\
											.drop_duplicates())[['CAMPUS',
																 'COURSE_SUBJECTS',
																 'COURSE_GRADE_LVL',
																 'COURSE_ID',
																 'COURSE_TITLE',
																 'TEACHER_LNAME',
																 'KNOWLEDGE_LVL_TYPE',
																 'LEARNING_STD_IN_CURRENT_AY',
																 'meas_est_covg',
																 'SIH_COURSEPK_ID']]\
															   .sort_values(by = ['CAMPUS',
																				  'COURSE_SUBJECTS',
																				  'COURSE_GRADE_LVL',
																				  'COURSE_ID',
																				  'COURSE_TITLE',
																				  'KNOWLEDGE_LVL_TYPE',
																				  'LEARNING_STD_IN_CURRENT_AY',
																				  'SIH_COURSEPK_ID'])
#
# Ⓔ Write the results out to tabs in an xlsx file.
writer  = pd.ExcelWriter(os.path.abspath(os.path.join(session_attributes.get('target_dir'), 
													  session_attributes.get('benchmark_time') +\
													  '_EXEMPLARY_ALIGNMENT_INDICATORS.xlsx')),
						engine = 'xlsxwriter')

crs_evidentiary_accrual.assign(SIH_COURSEPK_ID = list(map(str, crs_evidentiary_accrual['SIH_COURSEPK_ID'])))\
						.to_excel(excel_writer = writer,
								  sheet_name = 'Course Evid Accrual',
								  index = False)
agg_algmt_progression.assign(SIH_COURSEPK_ID = list(map(str, agg_algmt_progression['SIH_COURSEPK_ID'])))\
					 .to_excel(excel_writer = writer,
							   sheet_name = 'Evid Algmt — Progressions',
							   index = False)
agg_algmt_blueprint.assign(SIH_COURSEPK_ID = list(map(str, agg_algmt_blueprint['SIH_COURSEPK_ID'])))\
					 .to_excel(excel_writer = writer,
							   sheet_name = 'Evid Algmt — Blueprints',
							   index = False)
evid_est_covg.assign(SIH_COURSEPK_ID = list(map(str, evid_est_covg['SIH_COURSEPK_ID'])))\
			 .to_excel(excel_writer = writer,
					   sheet_name = 'Meas-Est Coverage',
					   index = False)

writer.save()
writer.close()


#
##################⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆ MAIN PROGRAM ⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆##############
#🚛⚒⛏📇🔑🔭🔬🔩⚙️🗜📡🔌🚛⚒⛏📇🔑🔭🔬🔩⚙️🗜📡🔌🚛⚒⛏📇🔑🔭🔬🔩⚙️🗜📡🔌🚛⚒⛏📇🔑🔭🔬🔩⚙️🗜📡🔌🚛⚒⛏📇🔑🔭🔬🔩⚙️🗜📡#
##################################################################################################################################






## ‼️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️‼️#        
## ‼️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️‼️#        
## ‼️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️‼️#        
## ‼️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️‼️#        



sec_evidentiary_accrual = pd.concat(
					objs = [fct.reduce(lambda x, y: pd.merge(left = x,
															 right = y),
													[evid_projection.assign(measurements_recorded = [0 if pd.isna(raw_score) 
																							  else 1 
																							for raw_score in evid_projection['DATE_MEASUREMENT_LOADED']])\
																	.groupby(by = ['COURSE_SECTION_SID',
																				   #'STUDENT_ID',
																				   #'LEARNING_STANDARD_ID',
																				   'DATE_MEASUREMENT_LOADED'],
																			 as_index = False)\
																	.sum(),
													 evid_projection.assign(enrollees = 1)\
																	.drop_duplicates(subset = ['COURSE_SECTION_SID',
																							   'STUDENT_ID'],
																					 keep = 'first')\
																	.groupby(by = 'COURSE_SECTION_SID',
																			 as_index = False)\
																	.sum(),
													 evid_projection.assign(standards = 1)\
																	.drop_duplicates(subset = ['COURSE_SECTION_SID',
																							   'LEARNING_STANDARD_ID'],
																					 keep = 'first')\
																	.groupby(by = 'COURSE_SECTION_SID',
																			 as_index = False)\
																	.sum()

													  ])
													 for evid_projection in 
															[pd.merge(left = proficiency_span[['COURSE_SECTION_SID',
																							   'STUDENT_ID',
																							   'LEARNING_STANDARD_ID']],
																	  right = body_of_evidence[['COURSE_SECTION_SID',
																								'STUDENT_ID',
																								'LEARNING_STANDARD_ID',
																								'DATE_MEASUREMENT_LOADED']],
																	  how = 'left').drop_duplicates()]] )
sec_evidentiary_accrual = sec_evidentiary_accrual.assign(covg_factor = [meas/(stud*std)
																					for (meas, stud, std) in sec_evidentiary_accrual[['measurements_recorded',
																																		   'enrollees',
																																		   'standards']]\
																																		 .to_records(index = False)\
																																		 .tolist()])

sec_evidentiary_accrual = fct.reduce(lambda x, y: pd.merge(left = x,
															 right = y),
												  [pd.concat(objs =  [pd.DataFrame(data = cum_covg_tuple,
																				   columns = ['COURSE_SECTION_SID',
																							  'DATE_MEASUREMENT_LOADED',
																							  'cum_covg'])
																	  for cum_covg_tuple in 
																		  [list(map(lambda x : [sec] + list(x), list(zip(meas_date, np.cumsum(covg_factor)))))
																					for (sec, meas_date, covg_factor) in 
																						sec_evidentiary_accrual.drop(labels = ['measurements_recorded',
																															   'enrollees',
																															   'standards'],
																													 axis = 1)\
																												.groupby(by = ['COURSE_SECTION_SID'],
																														as_index = False)\
																												.agg(list)\
																												.to_records(index = False)\
																												.tolist() ] ],
															 sort = True),
												   sec_evidentiary_accrual.assign(possible_measurements = [stud*std
																					for (meas, stud, std) in sec_evidentiary_accrual[['measurements_recorded',
																																	  'enrollees',
																																	  'standards']]\
																																	.to_records(index = False)\
																																	.tolist()]),
												   body_of_evidence[['COURSE_SECTION_SID',
																	   'DATE_MEASUREMENT_LOADED',
																	   'LEARNING_STANDARD_CD']].drop_duplicates()\
																	  .groupby(by = ['COURSE_SECTION_SID',
																					 'DATE_MEASUREMENT_LOADED'],
																			   as_index = False)\
																	  .agg(set)\
																	  .rename(columns = {'LEARNING_STANDARD_CD' : 'learning_standards_measured'}),
												   body_of_evidence[['COURSE_SECTION_SID',
																	 'DATE_MEASUREMENT_LOADED',
																	 'LEARNING_STANDARD_CD']].drop_duplicates()\
																	.groupby(by = ['COURSE_SECTION_SID',
																				   'DATE_MEASUREMENT_LOADED'],
																			 as_index = False)\
																	.agg(set),
												   proficiency_span[['CAMPUS',
																	  'COURSE_SUBJECTS',
																	  'COURSE_GRADE_LVL',
																	  'COURSE_ID',
																	  'COURSE_TITLE',
																	  'COURSE_SECTION_TITLE',
																	  'TEACHER_LNAME',
																	  'TEACHER_FNAME',
																	  'COURSE_SECTION_SID']]\
																	.drop_duplicates() ] )\
																	.sort_values(by = ['CAMPUS',
																					   'COURSE_SUBJECTS',
																					   'COURSE_GRADE_LVL',
																					   'COURSE_ID',
																					   'COURSE_TITLE',
																					   'COURSE_SECTION_TITLE',
																					   'TEACHER_LNAME',
																					   'DATE_MEASUREMENT_LOADED'])\
																	 [['CAMPUS',
																	   'COURSE_SUBJECTS',
																	   'COURSE_GRADE_LVL',
																	   'COURSE_ID',
																	   'COURSE_TITLE',
																	   'COURSE_SECTION_TITLE',
																	   'TEACHER_LNAME',
																	   'TEACHER_FNAME',
																	   'enrollees',
																	   'standards',
																	   'DATE_MEASUREMENT_LOADED',
																	   'learning_standards_measured',
																	   'measurements_recorded',
																	   'covg_factor',
																	   'cum_covg',
																	   'COURSE_SECTION_SID']]





## ‼️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️‼️#        
## ‼️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️‼️#        
## ‼️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️‼️#        
## ‼️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️‼️#        
#
## ‼️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️📛🛑⛔️‼️#        
#                                                                                           })
# #
# ##################################################################################################################################
# #🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛#
# ##################⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇ DEVELOPMENTAL STATEMENT ⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇###############
# #
# ##################⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆ DEVELOPMENTAL STATEMENT ⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆##############
# #🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛🖲📐⏲🗜🎚🎛#
# ###################################################################################################################################




