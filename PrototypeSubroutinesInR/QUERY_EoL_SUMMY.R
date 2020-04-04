## PURPOSE:  APPLY A BAYESIAN NETWORK TO ESTIMATE LEARNER KNOWLEGE STATE GIVEN EVIDENCE OF LEARNING
## MEASUREMENT.  The scope of the estimation is bounded by the learning map associated with a course unit and the 
## course section.  An external file USE_CASE_QUERY_ATTRIBUTES specifies the scope of the query.
##
## MAJOR STEPS IN THE ALGORITHM LOGIC.
## 1︎⃣ Set workspace parameters and read in working files.  We specifically require the following:
##      ⪧ USE_CASE_QUERY_ATTRIBUTES guides the case study on which we focus.
##      ⪧ COURSE_ENROLL contains the enrollment and responsible educator.
##      ⪧ EoL_MEAS contains the learners' evidence of learning (EoL) measurements.
##      ⪧ KNOW_STATE_SPEC contains relationships between learners' measured learning evidence and their implied knowledge states.
##      ⪧ GRAPH_CLUST_N_UNIT_MAP_JDF contains the joint distrubition functions (JDF) for Nᵗʰ cluster of connected vertices
##         within UNIT_MAP_EDGE_LIST. We employ this to get the in-scope vertices.
## 2︎⃣ Window the EoL_MEAS learning-measurement table. Retain only records corresponding to subjects (students) for whom 
##      STUDENT_ID exists in EoL_MEAS. Also, limit the LEARNING_STANDARD_ID to the variables specfied within the columns of
##      GRAPH_CLUST_N_UNIT_MAP_JDF.  Also, sort the EoL_MEAS by DATE_OF_MEAS and retain only the most-recent in cases
##      of multiple measurements of LEARNING_STANDARD_IDs for distinct subjects.
## 3︎⃣ Apply KNOW_STATE_SPEC to impute hard-decision knowledge-state estimates for each EoL_MEAS.
## 4︎⃣ Identify the evidence states in EoL_MEAS.  We introduce here three aspects of our framework.
##      ⓐ KNOWLEDGE STATE represents the estimated extent of mastery for an individual learner with respect to all LEARNING_STANDARD_ID
##           attributes from the proficiency model.
##      ⓑ EVIDENTIARY PROFILE contains all of the observed variables from which that estimate is derived.
##      ⓒ EVIDENTIARY STATE specifies the actual state for each evidentiary-profile variable for a specific learner.
##      We extract during this stage the evidentiary profile and evidentiary state for each subject (learner, student) from EoL_MEAS.
##       Categorize learners according to evidentiary profile and evidentiary state. Also identify by cluster for each unit-submap cluster
##       of connected vertices:
##       ⓐ Observed variables from the evidentiary profile on which we condition the submap-cluster's JDF; and
##       ⓑ The target variables for which we obtain marginal CDFs conditioned on evidentiary states in the evidentiary profile.
## 6︎⃣ Translate each EVIDENTIARY STATE into an estimated KNOWLEDGE STATE.  Condition GRAPH_CLUST_N_UNIT_MAP_JDF
##      on each observed evidentiary state.  Marginalize the resulting conditional distribution with respect to each target variable to obtain
##      a distribution of knowledge-state probabilities for each observed evidentiary state.
## 7︎⃣ Associate the LEARNING_STANDARD_ID-marginalized CDFs for each learner with the measured knowledge state to get a complete 
##      probability distribution for each variable.  Append to LEARNER_KNOW_STATE.  Reshape to wide-table format so that LEARNER_KNOW_STATE
##      contains for each STUDENT_ID × LEARNING_STANDARD_ID pair a row of conditional probability distributions regarding the LEARNER's state.
#
# Initialize environment.
	options(stringsAsFactors = FALSE)
	options(java.parameters = "-Xmx16g")
	library(stringr)
	library(reshape2)
	library(abind)
#
# 1︎⃣ DATA INGESTION.   Read in USE_CASE_ATTRIBUTES to get the distinguishing case-study variable states.
	proto.dir <- "/Users/nahamlet/Box Sync/IBM-Watson ED K12/Pathway-Centric CONOPS/Learning-Map Prototype"
	USE_CASE_ATTRIBUTES <- read.csv(file = paste(proto.dir, "USE_CASE_QUERY_ATTRIBUTES.csv", sep = "/"),
																colClasses = "character")
	rownames(USE_CASE_ATTRIBUTES) <- USE_CASE_ATTRIBUTES[,"QUERY_ATTRIBUTE"]
	Case.dir <- USE_CASE_ATTRIBUTES["Case.dir","VALUE"]
	PROF_TASK.dir <- paste(Case.dir, "PROF_TASK_MODEL", sep = "/")
	setwd(PROF_TASK.dir)
#
#     Read in other files listed above.
	KNOW_STATE_SPEC <- read.csv(file = paste(PROF_TASK.dir, "KNOW_STATE_SPEC.csv", sep = "/"),
														colClasses = "character")
	COURSE_ENROLL <- read.csv(file = paste(PROF_TASK.dir, "COURSE_ENROLL.csv", sep = "/"),
													colClasses = "character")
	EoL_MEAS <- read.csv(file = paste(PROF_TASK.dir, "EoL_MEAS.csv", sep = "/"),
										colClasses = "character")[c("STUDENT_ID","LEARNING_STANDARD_ID","LEARN_ACTY",
																					"MEAS_EVIDENCE","DATE_OF_MEAS")]
	LEARNING_STANDARD <- read.csv(file = paste(PROF_TASK.dir, "SIHLEARNING_STANDARD.csv", sep = "/"),
															colClasses = "character")[c("LEARNING_STANDARD_ID","LEARNING_STANDARD_CD")]
	UNIT_MAP_EDGES <- read.csv(file = paste(PROF_TASK.dir, "UNIT_MAP_EDGE_LIST.csv", sep = "/"),
														colClasses = "character")
	UNIT_MAP_EDGES <- data.frame(LEARNING_STANDARD_ID = unique(unlist(UNIT_MAP_EDGES[c("LEARNING_STANDARD_ID_FROM",
																																									"LEARNING_STANDARD_ID_TO")])))
#
# 2︎⃣ Window the EoL_MEAS learning-measurement table. First window by subjects with STUDENT_IDs in COURSE_ENROLL for the
#       COURSE_ID, CLASS_ID  specified by the corresponding values of USE_CASE_ATTRIBUTES. Create a "windowed" version of
#       COURSE_ENROLL.  Then merge the result with EoL_MEAS.
	SECT_ENROLL <- data.frame(rbind(USE_CASE_ATTRIBUTES[c("COURSE_ID","CLASS_ID"),"VALUE"]))
	colnames(SECT_ENROLL) <- c("COURSE_ID","CLASS_ID")
	SECT_ENROLL <- merge(x = SECT_ENROLL,
											y = COURSE_ENROLL)
	EoL_MEAS <- merge(x = SECT_ENROLL,
										y = EoL_MEAS)
	EoL_MEAS <- merge(x = EoL_MEAS,
										y = UNIT_MAP_EDGES)
#
	EoL_MEAS <- merge(x = EoL_MEAS,
										y = LEARNING_STANDARD)
	EoL_MEAS[,"MEAS_EVIDENCE"] <- as.numeric(EoL_MEAS[,"MEAS_EVIDENCE"])
	EoL_MEAS[,"IMPLIED_KNOW_STATE"] <- cut(x = EoL_MEAS[,"MEAS_EVIDENCE"],
																				breaks = unique(unlist(KNOW_STATE_SPEC[c("LOW_BOUND","UP_BOUND")])),
																				labels = KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"],
																				include.lowest = TRUE,
																				ordered.result = TRUE)
	EoL_MEAS[,"IMPLIED_KNOW_STATE"] <- as.character(EoL_MEAS[,"IMPLIED_KNOW_STATE"])
	EoL_MEAS <- EoL_MEAS[c("STUDENT_ID","STUDENT_NAME","LEARN_ACTY","LEARNING_STANDARD_CD",
												"MEAS_EVIDENCE","LEARNING_STANDARD_ID","IMPLIED_KNOW_STATE","DATE_OF_MEAS")]
	if(USE_CASE_ATTRIBUTES["STUDENT_ID","VALUE"] != "ALL") EoL_MEAS <- EoL_MEAS[EoL_MEAS[,"STUDENT_ID"] == USE_CASE_ATTRIBUTES["STUDENT_ID","VALUE"],]
	EoL_MEAS <- EoL_MEAS[order(EoL_MEAS$STUDENT_ID, 
														EoL_MEAS$LEARNING_STANDARD_CD, 
														EoL_MEAS$DATE_OF_MEAS, 
														decreasing = TRUE),]
#
	for (col_idx in colnames(EoL_MEAS)) EoL_MEAS[,col_idx] <- enc2utf8(as.character(EoL_MEAS[,col_idx]))
#
	write.csv(x = EoL_MEAS,
					file = paste(PROF_TASK.dir, "QUERY_EoL_SUMMY.csv", sep = "/"),
					row.names = FALSE,
					eol = "\r\n",
					fileEncoding = "UTF-8",
					quote = TRUE)
#