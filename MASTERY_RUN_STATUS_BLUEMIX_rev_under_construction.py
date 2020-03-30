
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
import functools as fct
import itertools as it

run_status_dir = '/Users/nahamlet@us.ibm.com/Box Sync/IBM Watson Education Delivery/Data/Partner/Mastery Run Status/DB_QUERY_IV&V'
db_cfg_dir = '/Users/nahamlet@us.ibm.com/Box Sync/IBM Watson Education Delivery/Data/Partner/Mastery Run Status/DB_QUERY_IV&V/DB_CONFIG_QUERY'
fx_dir = '/Users/nahamlet@us.ibm.com/Box Sync/IBM Watson Education Delivery/Data/Partner/Mastery Run Status/DB_QUERY_IV&V/FX'
db_credential_dir = '/Users/nahamlet@us.ibm.com/Documents/Documents/Oportunidades actuales'

benchmark_time = datetime.utcnow()
os.system('say "Mastery Run-Status Dashboard"')
run_start_time = time.time()
print('Mastery Run-Status Dashboard-Population Start Time:  ' + str(benchmark_time.strftime('%Y-%m-%d, %H:%M:%SZ')))
subprocess.call(['afplay', os.path.abspath(os.path.join(fx_dir,'oogachakaoogaooga.mp3'))])



tenant_config_dir_args = pd.read_csv(filepath_or_buffer = os.path.abspath(os.path.join(db_credential_dir, 
																				'TENANT_CONFIG_QUERY_SPECS.csv')),
									dtype = str)\
									.set_index(keys = 'ATTRIBUTE',
												drop = True)\
									.to_dict(orient = 'dict').get('VALUE')




tenant_config = pd.DataFrame(data = list(create_engine(URL(**tenant_config_dir_args)).execute(text(' '.join([sql_line 
																				for sql_line in open(file = os.path.abspath(os.path.join(db_cfg_dir, 
																									'TENANT_CONFIG.sql')) )]))) ),
							columns = ['host',
										'port',
										'database',
										'username',
										'password'])\
						.assign(drivername = 'db2+ibm_db')\
						.set_index(keys = 'database',
									drop = False)\
						.to_dict(orient = 'index')



tenant_config = {db_key : {key : val
							for dict_comp in [{'port' : 50000},
												{attrib_key : attrib_val
												for (attrib_key, attrib_val) in db_val.items()
												if attrib_key != 'port'}]
							for (key, val) in dict_comp.items()}
					for (db_key, db_val) in tenant_config.items()}

query_start_time = time.time()

database = list(tenant_config.keys())[0]
{database : color_list 
	for (database, color_list) in [(database ,list(create_engine(URL(**tenant_config.get(database))).execute(text(' '.join([sql_line 
																					for sql_line in open(file = os.path.abspath(os.path.join(db_cfg_dir, 
																										'MASTERY_COLOR_LIST.sql')) )]))) ))
						for database in list(tenant_config.keys()) ] }




EVID_EST_CVG_BY_COURSE = pd.concat([pd.DataFrame(data = list(create_engine(URL(**tenant_config.get(database))).execute(text(' '.join([sql_line 
																				for sql_line in open(file = os.path.abspath(os.path.join(db_cfg_dir, 
																									'OP_STATE—EST_COVG_DEFICIT.sql')) )]))) ),
												columns = ['TENANT_NAME',
															'COURSE_SUBJECTS',
															'COURSE_GRADE_LVL',
															'COURSE_ID',
															'COURSE_TITLE',
															'ENROLLEE_COUNT',
															'ENROLLEES_ASSESSED',
															'PCT_STUD_MEAS',
															'STD_COUNT_COURSE',
															'STANDARDS_ASSESSED',
															'PCT_STD_MEAS',
															'TUPLE_COUNT',
															'ESTIMATED',
															'MEASURED',
															'UNMEASURED',
															'PCT_TUPLE_MEAS',
															'PCT_TUPLE_EST',
															'PCT_TUPLE_UNMEAS',
															'EST_COVG_BACKLOG',
															'MOST_RECENT_MEAS',
															'MOST_REC_EST',
															'EARLIEST_REC_EST',
															'SIH_COURSEPK_ID'])
									for database in list(tenant_config.keys())])
print('First Query Done! ' + str(datetime.utcnow().strftime('%Y-%m-%d, %H:%M:%SZ')) + ', ' + str(round(time.time() - query_start_time, 2)) + ' seconds')
subprocess.call(['afplay', os.path.abspath(os.path.join(fx_dir,'106157_SOUNDDOGS__sh.mp3'))])
os.system('say " First Query Done!" ')





query_start_time = time.time()



preproc_config = pd.DataFrame(data = {'database' : [prod_key.replace('PR', 'MS') for prod_key in list(tenant_config.keys())],
									'drivername' : 'db2+ibm_db',
									'host' : '10.184.82.150',
									'username' : 'appuser',
									'port' : '50000',
									'password' : 'App12@23'})\
						.set_index(keys = 'database',
									drop = False)\
						.to_dict(orient = 'index')


EOL_SUB_CRS_MAP = pd.concat([pd.DataFrame(data = list(create_engine(URL(**preproc_config.get(database))).execute(text(' '.join([sql_line 
																				for sql_line in open(file = os.path.abspath(os.path.join(db_cfg_dir, 
																									'STATUS_FROM_EOL_MEAS_SUB_CRS_MAP.sql')) )])))),
											columns = ['SIH_COURSEPK_ID',
														'SUB_CRS_MAP_PK_ID',
														'ALGORITHM_RUN_STATUS',
														'MOST_REC_RUN'])
									for database in list(preproc_config.keys())])\
						.drop_duplicates(subset = ['SIH_COURSEPK_ID'],
										keep = 'first')

Counter(Counter(EOL_SUB_CRS_MAP['SIH_COURSEPK_ID']).values())


print('Second Query Done! ' + str(datetime.utcnow().strftime('%Y-%m-%d, %H:%M:%SZ')) + ', ' + str(round(time.time() - query_start_time, 2)) + ' seconds')
subprocess.call(['afplay', os.path.abspath(os.path.join(fx_dir,'106157_SOUNDDOGS__sh.mp3'))])
os.system('say "Second Query Done!"')
								
query_start_time = time.time()





CLSM_CVG_OF_COURSES = pd.concat([pd.DataFrame(data = list(create_engine(URL(**tenant_config.get(database))).execute(text(' '.join([sql_line 
																				for sql_line in open(file = os.path.abspath(os.path.join(db_cfg_dir, 
																									'CLSM_CVG_OF_COURSES.sql')) )]))) ),
												columns = ['SIH_COURSEPK_ID',
															'BLUEPRINT_LOADED'])
										for database in list(tenant_config.keys())])
#CLSM_CVG_OF_COURSES = CLSM_CVG_OF_COURSES.assign(SIH_COURSEPK_ID = list(map(str, CLSM_CVG_OF_COURSES['SIH_COURSEPK_ID'])))







print('Third Query Done! ' + str(datetime.utcnow().strftime('%Y-%m-%d, %H:%M:%SZ')) + ', ' + str(round(time.time() - query_start_time, 2)) + ' seconds')
subprocess.call(['afplay', os.path.abspath(os.path.join(fx_dir,'106157_SOUNDDOGS__sh.mp3'))])
os.system('say "Third Query Done!"')
#
EVID_EST_STATUS = pd.merge(left = pd.merge(left = EVID_EST_CVG_BY_COURSE,
											right = EOL_SUB_CRS_MAP,
											how = 'left'),
							right = CLSM_CVG_OF_COURSES,
							how = 'left')\
						.drop_duplicates()\
						.set_index(keys = 'SIH_COURSEPK_ID',
									drop = False)




EVID_EST_STATUS = EVID_EST_STATUS.assign(NEW_EVID_SINCE_LAST_RUN = [True if ((rec_run is not None) and (rec_meas > rec_run))
																		else False
																		for (rec_meas, rec_run) in EVID_EST_STATUS[['MOST_RECENT_MEAS', 
																													'MOST_REC_RUN']]\
																												.to_records(index = False)\
																												.tolist()] )\
								.assign(MOST_REC_EST = EVID_EST_STATUS['MOST_REC_EST'].tolist() )\
								.assign(EARLIEST_REC_EST = EVID_EST_STATUS['EARLIEST_REC_EST'].tolist() )


EVID_EST_STATUS = EVID_EST_STATUS.assign(ALGORITHM_RUN_STATUS = ['Insufficient Data' if ((meas_tuple <= 0.01)
																						  or np.isnan(meas_tuple))
																else 'Data Loaded, Algorithm Run Not Attempted' if (((meas_tuple > 0.01) 
																															and ((est_tuple is None) 
																																or np.isnan(est_tuple) 
																																or  (est_tuple <= 0.01)) )
																														or (rec_est is None) )
																else 'Estimates Present, Algorithm Run Unknown' if  (((str(run_stat) == 'nan')
																														or run_stat is None
																														or rec_run is None)
																													and ((est_tuple > 0.01) 
																															or np.istinite(est_tuple) 
																															or (est_tuple is not None)) ) 
																else run_stat
																for (run_stat, meas_tuple, est_tuple, rec_est, rec_run) in EVID_EST_STATUS[['ALGORITHM_RUN_STATUS',
																																	'PCT_TUPLE_MEAS',
																																	'PCT_TUPLE_EST',
																																	'MOST_REC_EST',
																																	'MOST_REC_RUN']]\
																													.to_records(index = False)\
																													.tolist()])\
									.assign(NEW_EVID_SINCE_LAST_EST = [True if ((rec_est is not None) and (rec_meas > rec_est))
																			else False
																			for (rec_meas, rec_est) in EVID_EST_STATUS[['MOST_RECENT_MEAS', 
																													'MOST_REC_EST']]\
																												.to_records(index = False)\
																												.tolist()] )
EVID_EST_STATUS.set_index(keys = 'SIH_COURSEPK_ID',
						drop = False,
						inplace = True)





writer  = pd.ExcelWriter(os.path.abspath(os.path.join(run_status_dir, 
									benchmark_time.strftime('%y%m%d_%H%MZ') +\
										'_MASTERY_RUN_ANALYSIS.xlsx')),
						engine = 'xlsxwriter')



pd.DataFrame(data = dict()).to_excel(excel_writer = writer,
									sheet_name = '⓵ PivotTableAnalysis',
									index = False)



EVID_EST_STATUS[['TENANT_NAME',
																'COURSE_SUBJECTS',
																'COURSE_GRADE_LVL',
																'COURSE_ID',
																'COURSE_TITLE',
																'SIH_COURSEPK_ID',
																'NEW_EVID_SINCE_LAST_EST',
																'ALGORITHM_RUN_STATUS',
																'EST_COVG_BACKLOG']]


pd.DataFrame(data = [ (tenant, subj, grade, crs_id,
						crs_title, crspk_id,
						new_evid, run_stat)
					for (tenant, subj, grade, crs_id,
						crs_title, crspk_id,
						new_evid, run_stat)	in 	EVID_EST_STATUS[['TENANT_NAME',
																'COURSE_SUBJECTS',
																'COURSE_GRADE_LVL',
																'COURSE_ID',
																'COURSE_TITLE',
																'SIH_COURSEPK_ID',
																'NEW_EVID_SINCE_LAST_EST',
																'ALGORITHM_RUN_STATUS']].to_records(index = False).tolist()
					if (new_evid or ('Not Attempted') in run_stat)],
			columns = ['TENANT_NAME',
						'COURSE_SUBJECTS',
						'COURSE_GRADE_LVL',
						'COURSE_ID',
						'COURSE_TITLE',
						'SIH_COURSEPK_ID',
						'NEW_EVID_SINCE_LAST_EST',
						'ALGORITHM_RUN_STATUS'])\
		.astype(str)\
		.to_excel(excel_writer = writer,
				sheet_name = '⓶ Algorithm-Ready Courses',
				index = False)






EVID_EST_STATUS[['TENANT_NAME',
					'COURSE_SUBJECTS',
					'COURSE_GRADE_LVL',
					'COURSE_ID',
					'COURSE_TITLE',
					'ALGORITHM_RUN_STATUS',
					'MOST_REC_RUN',
					'NEW_EVID_SINCE_LAST_EST',
					'NEW_EVID_SINCE_LAST_RUN',
					'MOST_RECENT_MEAS',
					'MOST_REC_EST',
					'EARLIEST_REC_EST',
					'ENROLLEE_COUNT',
					'STD_COUNT_COURSE',
					'TUPLE_COUNT_COURSE',
					'ESTIMATED',
					'MEASURED',
					'UNMEASURED',
					'MEAS_TUPLE_COUNT_COURSE',
					'EST_TUPLE_COUNT_COURSE',
					'PCT_STD_MEAS',
					'PCT_STUD_MEAS',
					'PCT_TUPLE_MEAS',
					'PCT_TUPLE_EST',
					'PCT_TUPLE_UNMEAS',,
					'TENANT_ID',
					'SIH_COURSEPK_ID']]\
		.assign(MASTERY_RUN_STATUS_AS_OF = str(benchmark_time.strftime('%Y-%m-%d, %H:%M:%SZ')))\
		.sort_values(by = 'MOST_REC_RUN',
					ascending = False)\
		.astype(str)\
		.drop_duplicates()\
		.to_excel(excel_writer = writer,
				sheet_name = '⓷ MasteryRunSessionTable',
				index = False)






pd.pivot_table(data = EVID_EST_STATUS.assign(group_count = 1),
				values = 'group_count',
				index = ['TENANT_NAME', 'COURSE_SUBJECTS', 'COURSE_GRADE_LVL'],
				columns = ['NEW_EVID_SINCE_LAST_EST','ALGORITHM_RUN_STATUS'],
				aggfunc = np.sum)\
		.fillna(value = 0)\
		.to_excel(excel_writer = writer,
				sheet_name = '⓸ PandasPivot',
				index = True)




writer.save()
writer.close()

Counter(Counter(EVID_EST_STATUS['SIH_COURSEPK_ID'].tolist()).values())
#
#
print('Thats all folks! ' + str(datetime.utcnow().strftime('%Y-%m-%d, %H:%M:%SZ')) + ', ' + str(round(time.time() - run_start_time, 2)) + ' seconds')
subprocess.call(['afplay', os.path.abspath(os.path.join(fx_dir,'Thats All Folks.mp3'))])


#
