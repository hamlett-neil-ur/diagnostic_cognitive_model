SELECT DISTINCT course.COURSE_ID, course.COURSE_TITLE, tch_unit.UNIT_NM,
				std.LEARNING_STANDARD_CD, cont.SUBJECT_TITLE AS SUBJECT,
				cont.GRADE_LEVEL, tch_unit.UNIT_START_DT, tch_unit.UNIT_FINISH_DT,
				course.SIH_COURSEPK_ID, std.LEARNING_STANDARD_ID, clsm.TENANT_ID
FROM IBMSIH.COURSE_LEARNING_STD_MAP clsm
JOIN IBMSIH.SIHCOURSE course ON course.SIH_COURSEPK_ID = clsm.SIH_COURSEPK_ID
JOIN (SELECT sec.SIH_COURSEPK_ID, sec.COURSE_SECTION_SID
	  FROM IBMSIH.COURSE_SECTION sec
	  JOIN IBMSIH.ACADEMIC_YEAR_LOOKUP acad_yr ON acad_yr.ACADEMIC_YEAR = sec.ACADEMIC_YEAR_DELIVERED
	  WHERE acad_yr.ACADEMIC_YEAR_START < CURRENT_TIMESTAMP
	      AND acad_yr.ACADMEMIC_YEAR_END > CURRENT_TIMESTAMP
		 AND sec.TENANT_ID = :tenant_id
	  ORDER BY sec.SIH_COURSEPK_ID, sec.COURSE_SECTION_SID ) 
								AS sec ON sec.SIH_COURSEPK_ID = course.SIH_COURSEPK_ID
JOIN IBMSIH.TEACHER_UNIT tch_unit ON tch_unit.COURSE_SECTION_SID = sec.COURSE_SECTION_SID
JOIN IBMSIH.CRS_SCTN_TCHR_UNT_LRGN_STD cstuls ON cstuls.TEACHER_UNIT_SID = tch_unit.TEACHER_UNIT_SID
JOIN IBMSIH.SIHLEARNING_STANDARD std ON std.LEARNING_STANDARD_ID = cstuls.LEARNING_STANDARD_ID
JOIN IBMSIH.SIHSTANDARD_CONTENT cont ON cont.STANDARD_CONTENT_ID = std.STANDARD_CONTENT_ID
WHERE clsm.TENANT_ID = :tenant_id
ORDER BY cont.SUBJECT_TITLE, cont.GRADE_LEVEL, course.COURSE_ID, tch_unit.UNIT_START_DT,
		tch_unit.UNIT_NM, std.LEARNING_STANDARD_CD
