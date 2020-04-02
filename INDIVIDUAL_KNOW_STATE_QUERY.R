## PURPOSE: PRODUCE KNOWLEDGE-STATE PROBABILITY DISTRIBUTIONS FOR GRAPH PLOTS.
## We set up here a table of the knowledge-state probability distributions for each vertex in a
## subgraph under analysis. The output consists of a table used as the vertex.pie argument
## by the igraph iplot function.  The output VERTEX_STATE_DIST is produced via the following steps.
## 1︎⃣ Set run-time environment variables and read in input tables. These include:
##      ⓐ USE_CASE_QUERY_ATTRIBUTES specifies the scope parameters for the query and analysis.
##      ⓑ COURSE_ENROLL contains subject (learner, student) enrollment by course number and section.
##      ⓒ UNIT_MAP_EDGE_LIST contains vertex variables of which the subgraph is comprised.
##      ⓓ LEARNER_KNOW_STATE contains conditional probability distributions of individual learner 
##           knowledge states produced by the Bayesian network.
##      ⓔ KNOW_STATE_SPEC contains knowledge-state categories specified by the client jurisdiction.
## 2︎⃣ Conditionally query LEARNER_KNOW_STATE for the records to be plotted.
##      ⓐ Retrieve knowledge-state estimates for an individual subject if a distinct STUDENT_ID is specified.
##      ⓑ Retrieve knowledge-state estimates for all subjects in the course section of "ALL" is specified for STUDENT_ID
##           value in USE_CASE_QUERY_ATTRIBUTES.
## 3︎⃣ Reshape the query results from long-table to wide-table format. Use formula STUDENT_ID ~ IMPLIED_KNOW_STATEs
##      and MEAS as the value.var.  
##      ⓐ If "ALL" is specified as the STUDENT_ID attribute in USE_CASE_QUERY_ATTRIBUTES, then mean-aggregate 
##           the value variables.
## 4︎⃣ Prepare the table for output and write out result as csv file. We want as our columns the LEARNING_STANDARD_ID
##      and the IMPLIED_KNOW_STATE categories.
#
# Initialize environment.
	options(stringsAsFactors = FALSE)
	options(java.parameters = "-Xmx16g")
	library(reshape2)
#
# 1︎⃣ DATA INGESTION.   Read in USE_CASE_ATTRIBUTES first.
	proto.dir <- "/Users/nahamlet/Box Sync/IBM-Watson ED K12/Pathway-Centric CONOPS/Learning-Map Prototype"
	USE_CASE_ATTRIBUTES <- read.csv(file = paste(proto.dir, "USE_CASE_QUERY_ATTRIBUTES.csv", sep = "/"),
																colClasses = "character")
	rownames(USE_CASE_ATTRIBUTES) <- USE_CASE_ATTRIBUTES[,"QUERY_ATTRIBUTE"]
	Case.dir <- USE_CASE_ATTRIBUTES["Case.dir","VALUE"]
	PROF_TASK.dir <- paste(Case.dir, "PROF_TASK_MODEL", sep = "/")
	setwd(PROF_TASK.dir)
#
#     Now read in the files used for analysis.
	KNOW_STATE_SPEC <- read.csv(file = paste(PROF_TASK.dir, "KNOW_STATE_SPEC.csv", sep = "/"),
														colClasses = "character")
	COURSE_ENROLL <- read.csv(file = paste(PROF_TASK.dir, "COURSE_ENROLL.csv", sep = "/"),
													colClasses = "character")
	UNIT_MAP_EDGE_LIST <- read.csv(file = paste(PROF_TASK.dir, "UNIT_MAP_EDGE_LIST.csv", sep = "/"),
															colClasses = "character")
	LEARNER_KNOW_STATE <- read.csv(file = paste(PROF_TASK.dir, "LEARNER_KNOW_STATE.csv", sep = "/"),
																colClasses = "character")
#
# 2︎⃣ Conditionally query LEARNER_KNOW_STATE for the records to be plotted.  Do this in four stages.
#      ⓐ Merge LEARNER_KNOW_STATE with STUDENT_ID instances from COURSE_ENROLL for subjects in the specified COURSE_ID.
#      ⓑ Merge LEARNER_KNOW_STATE with STUDENT_ID instances from COURSE_ENROLL for subjects in the specified CLASS_ID.
#      ⓒ Detect which of the learning standards have been measured for any subjects.  This occurs for MEAS values have been assigned as unity.
#      ⓓ If STUDENT_ID in USE_CASE_ATTRIBUTES is not "ALL", winnow LEARNER_KNOW_STATE down to the specified STUDENT_ID.
	LEARNER_KNOW_STATE <- LEARNER_KNOW_STATE[LEARNER_KNOW_STATE[,"STUDENT_ID"] %in% 
																							COURSE_ENROLL[COURSE_ENROLL[,"COURSE_ID"] == USE_CASE_ATTRIBUTES["COURSE_ID","VALUE"],
																									"STUDENT_ID"],]
	LEARNER_KNOW_STATE <- LEARNER_KNOW_STATE[LEARNER_KNOW_STATE[,"STUDENT_ID"] %in% 
																							COURSE_ENROLL[COURSE_ENROLL[,"CLASS_ID"] == USE_CASE_ATTRIBUTES["CLASS_ID","VALUE"],
																									"STUDENT_ID"],]
	MEASURED <- unique(LEARNER_KNOW_STATE[LEARNER_KNOW_STATE[,"MEAS"] == "1","LEARNING_STANDARD_ID"])
	if(USE_CASE_ATTRIBUTES["STUDENT_ID","VALUE"] != "ALL") LEARNER_KNOW_STATE <- 
																LEARNER_KNOW_STATE[LEARNER_KNOW_STATE[,"STUDENT_ID"] == USE_CASE_ATTRIBUTES["STUDENT_ID","VALUE"],]
#
# 3︎⃣ Reshape the query results from long-table to wide-table format. Use dcast. Assign the result to our output variable VERTEX_STATE_DIST.
#      Also, coerce the knowledge-state category variables to numeric.
	VERTEX_STATE_DIST <- dcast(data = LEARNER_KNOW_STATE,
													formula = STUDENT_ID + LEARNING_STANDARD_ID  ~ IMPLIED_KNOW_STATE,
													value.var = "MEAS")
	for(col_idx in KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"]) VERTEX_STATE_DIST[,col_idx] <- as.numeric(VERTEX_STATE_DIST[,col_idx])
#
#      Conditionally aggregate.  If STUDENT_ID in USE_CASE_ATTRIBUTES is "ALL", the mean-aggregate. Otherwise, return only
#      records for the specified STUDENT_ID.
	if(USE_CASE_ATTRIBUTES["STUDENT_ID","VALUE"] == "ALL"){
	# We need to mean-aggregate by each IMPLIED_KNOW_STATE.  Do this one at a time and join onto a data frame of LEARNING_STANDARD_IDs.
		SECT_STATE_AGG <- unique(VERTEX_STATE_DIST["LEARNING_STANDARD_ID"])
		for (state_idx in KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"]){				## state_idx <- KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"][1]
			SECT_STATE_AGG <- merge(x = SECT_STATE_AGG,
															y = aggregate(formula = as.formula(paste(state_idx,"LEARNING_STANDARD_ID",sep = " ~ ")),
																					data = VERTEX_STATE_DIST[c("STUDENT_ID","LEARNING_STANDARD_ID",state_idx)],
																					FUN = mean))
		#	
		}
	#	Overwrite VERTEX_STATE_DIST with its mean-aggregates.
		VERTEX_STATE_DIST <- SECT_STATE_AGG
	#	
	} else{
	# Retain only records in VERT_STATE_DIST pertaining to the STUDENT_ID specified in USE_CASE_ATTRIBUTES.
		VERTEX_STATE_DIST <- VERTEX_STATE_DIST[VERTEX_STATE_DIST[,"STUDENT_ID"] == USE_CASE_ATTRIBUTES["STUDENT_ID","VALUE"],]
	#	
	}
#
#      Finally, add a logical flag indicating which of the vertices have been measured. We previously captured this in a MEASURED list of
#      LEARNING_STANDARD_ID vertices to which any evidence had been applied for any subject (learner, student).  Assign these as TRUE,
#      and the remainder as false.
	VERTEX_STATE_DIST["MEASURED"] <- ifelse(test = VERTEX_STATE_DIST[,"LEARNING_STANDARD_ID"] %in% MEASURED,
																				yes = TRUE,
																				no = FALSE)
#
# 4︎⃣ Prepare the table for output and write out result as csv file. 
	for (col_idx in colnames(LEARNER_KNOW_STATE)) LEARNER_KNOW_STATE[,col_idx] <- enc2utf8(as.character(LEARNER_KNOW_STATE[,col_idx]))
	write.csv(x = VERTEX_STATE_DIST,
					file = paste(PROF_TASK.dir, "VERTEX_STATE_DIST.csv", sep = "/"),
					row.names = FALSE,
					eol = "\r\n",
					fileEncoding = "UTF-8",
					quote = TRUE)
#