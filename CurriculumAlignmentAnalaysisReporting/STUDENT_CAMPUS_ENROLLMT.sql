/*
PURPOSE:  ASSOCIATE A STUDENT_ID WITH A CAMPUS ENROLLMENT. During evidentiary-alignment analysis we
          encounter instances for which a student × learning-standard measurement cannot be associated
          with a fully-enacted course. Enacted courses are identifiable by 
          ⓵ Enrolled students in an active course-section;
          ⓶ Course scope articulated via a course blueprint; and
          ⓷ Learning-standard progressions.
          We produce here a simple relationship between the campus and the STUDENT_ID.
APPROACH: It turns out that our SIHORGPERSONROLE contains the essential information:  Unique, synthetic
		  keys for both the student and the campus of enrollment.  We acquire our required cross-walk
		  table by the following procedure.
		  ⓵ Begin with the SIHORGPERSONROLE table including records for the specified TENANT_ID.
		  ⓶ Join with SIHORG, to introduce the CAMPUS as SIHORG.NAME.
		  ⓷ Join with COURSE_SECTION_STUDENT in order to specify only students 
		     who are currently enrolled.
		  ⓸ Join with a time-windowed COURSE_SECTION. We want to limit our query response
		     to course-section enrollments where sections are active at the time of the query. 
*/
SELECT DISTINCT org.NAME AS CAMPUS, opr_stud.PERSON_ID AS STUDENT_ID
FROM IBMSIH.SIHORGPERSONROLE opr_stud
JOIN IBMSIH.SIHORG org ON opr_stud.SIH_ORGPK_ID = org.SIH_ORGPK_ID
JOIN IBMSIH.COURSE_SECTION_STUDENT sec_stud ON sec_stud.SIH_ORG_PERSON_ROLEPK_ID = opr_stud.SIH_ORG_PERSON_ROLEPK_ID
JOIN (SELECT sec.SIH_COURSEPK_ID, sec.COURSE_SECTION_SID, sec.COURSE_SECTION_TITLE,
			sec_day.TERM_START_DATE, sec_day.TERM_END_DATE, sec.SIH_ORGPK_ID
		FROM IBMSIH.COURSE_SECTION sec
		JOIN (SELECT sec_day.COURSE_SECTION_SID, 
					MIN(sec_day.COURSE_SECTION_DAY_DATE) AS TERM_START_DATE,
					MAX(sec_day.COURSE_SECTION_DAY_DATE) AS TERM_END_DATE
				FROM IBMSIH.COURSE_SECTION_DAY sec_day
				GROUP BY sec_day.COURSE_SECTION_SID
				ORDER BY sec_day.COURSE_SECTION_SID) 
						AS sec_day ON sec_day.COURSE_SECTION_SID = sec.COURSE_SECTION_SID
		WHERE sec.IS_ACTIVE_YN = 1
			AND sec_day.TERM_START_DATE < CURRENT_TIMESTAMP
			AND sec_day.TERM_END_DATE > CURRENT_TIMESTAMP
			ORDER BY sec.SIH_ORGPK_ID, sec.SIH_COURSEPK_ID, sec.COURSE_SECTION_SID )
				AS sec ON sec.COURSE_SECTION_SID = sec_stud.COURSE_SECTION_SID
WHERE opr_stud.TENANT_ID = :tenant_id
	AND sec_stud.IS_ACTIVE_YN = 1
ORDER BY opr_stud.PERSON_ID


