SELECT sst.STUDENT_ID, sst.SUBJECT, sst.LEARNING_STANDARD_CD, sst.TEST_NAME,
		sst.MEASUREMENT_APPROACH, sst.MEAS_EVIDENCE, sst.TEST_DATE, 
		sst.DIST_QTR_NAME, sst.GRADE, sst."LANGUAGE", SST.TEST_VERSION,
		sst.BRAILLE, sst.TENANT_ID, sst.LEARNING_STANDARD_ID, sst.MAX_RAW_SCORE,
		sst.RAW_SCORE, sst.WORK_PRODUCT_TITLE
FROM IBMSIH.STUDENT_STANDARDIZED_TEST sst
WHERE sst.LAST_UPDATE_DT > '2019-01-01'
		AND sst.TENANT_ID = :tenant_id
ORDER BY sst.LAST_UPDATE_DT DESC