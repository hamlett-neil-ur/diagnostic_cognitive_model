## PURPOSE: Implement conditional use-case choreography logic for the learning-map prototype.  This choreography logic invokes
## other R scripts conditioned on values in QUERY_STATE_UPDATE table.  These scripts are invoked using the source command.
#
# The USE_CASE_ATTRIBUTES csv file contains the variables and values on which the choreograpy steps are based.
# DATA INGESTION.   Read in USE_CASE_ATTRIBUTES first. Set the working directories as that containing the subroutines.
	proto.dir <- "/Users/nahamlet/Box Sync/IBM-Watson ED K12/Pathway-Centric CONOPS/Learning-Map Prototype"
	QUERY_STATE_UPDATE <- read.csv(file = paste(proto.dir, "USE_CASE_QUERY_ATTRIBUTES.csv", sep = "/"),
																colClasses = "character",
																fileEncoding = "UTF-8")
	rownames(QUERY_STATE_UPDATE) <- QUERY_STATE_UPDATE[,"QUERY_ATTRIBUTE"]
	Case.dir <- QUERY_STATE_UPDATE["Case.dir","VALUE"]
	PROF_TASK.dir <- paste(Case.dir, "PROF_TASK_MODEL", sep = "/")	
	subroutine.dir <- paste(proto.dir, "Subroutines", sep = "/")
	setwd(subroutine.dir)
#
# Which subroutines are invoked is determined by QUERY_STATE attribute-value differences between QUERY_STATE_UPDATE and
# a QUERY_STATE_BASELINE file.  The baseline contains the attribute-state vaules of the previous query.  Conditionally incorporate the
# basline by the following.
# ⓐ Ascertaining whether a previous QUERY_STATE_BASELINE file exists in the PROF_TASK.dir directory, containing the
#      input files and output files from the previous query.  
#        ⅰ If a previous exists, read in, change VALUE column name to VALUE_BASLINE, and merge with QUERY_STATE_UPDATE.
#      ⅱ  If a previous QUERY_STATE_BASELINE does not exist, then assign NA to all of the VALUE_BASELINE columns in QUERY_STATE_UPDATE.
# ⓑ Overwrite the previous QUERY_STATE_BASELINE with the QUERY_ATTRIBUTE and VALUE columns from QUERY_STATE_UPDATE.
	if (file.exists(paste(PROF_TASK.dir, "QUERY_STATE_BASELINE.csv", sep = "/"))   ){
		QUERY_STATE_BASELINE  <- read.csv(file = paste(PROF_TASK.dir, "QUERY_STATE_BASELINE.csv", sep = "/"),
																		colClasses = "character",
																		fileEncoding = "UTF-8")
		colnames(QUERY_STATE_BASELINE) <- c("QUERY_ATTRIBUTE", "VALUE_BASELINE")
		QUERY_STATE_UPDATE <- merge(x = QUERY_STATE_UPDATE,
																y = QUERY_STATE_BASELINE)
		rownames(QUERY_STATE_UPDATE) <- QUERY_STATE_UPDATE[,"QUERY_ATTRIBUTE"]
	} else {
		QUERY_STATE_UPDATE["VALUE_BASELINE"] <- NA
	}
#
	write.csv(x = QUERY_STATE_UPDATE[c("QUERY_ATTRIBUTE","VALUE")],
					file = paste(PROF_TASK.dir, "QUERY_STATE_BASELINE.csv", sep = "/"),
					row.names = FALSE,
					eol = "\r\n",
					fileEncoding = "UTF-8",
					quote = TRUE)
#
# We now conditionally invoke subroutines to regenerate the Bayesian Network if specified VALUE attributes differ between the BASELINE and UPDATE. The
# Bayesian Network is updated by the following subroutines:
# ⓐ UNIT_SUBGRAPH.R
# ⓑ UNIT_SUBGRAPH_JDF.R
# ⓒ VERTEX_LAYOUT.R
# ⓓ LEARNER_KNOW_STATE.R
# If only DATE_LATEST_MEAS, then LEARNER_KNOW_STATE is simply invoked to update the LEARNER_KNOW_STATE observations and estimates
# according to changes to evidentiary profiles, eates.
	UPDATE_BAYESNET <- c("SCHOOL_DISTRICT","SUBJECT_TITLE","COURSE_TITLE","COURSE_ID","UNIT_ID")
	if(!identical(QUERY_STATE_UPDATE[UPDATE_BAYESNET,"VALUE"],
						QUERY_STATE_UPDATE[UPDATE_BAYESNET,"VALUE_BASELINE"])  ){
		source(file = paste(subroutine.dir, "UNIT_SUBGRAPH.R", sep = "/"),
					local = TRUE)
		source(file = paste(subroutine.dir, "VERTEX_LAYOUT.R", sep = "/"),
					local = TRUE)
		source(file = paste(subroutine.dir, "PLOT_UNIT_SUBGRAPH_FRAME.R", sep = "/"),
					local = TRUE)
		source(file = paste(subroutine.dir, "LEARNER_KNOW_STATE.R", sep = "/"),
					local = TRUE)
	} else if(!identical(QUERY_STATE_UPDATE["DATE_LATEST_MEAS","VALUE"],
						QUERY_STATE_UPDATE["DATE_LATEST_MEAS","VALUE_BASELINE"]) ){
		source(file = paste(subroutine.dir, "LEARNER_KNOW_STATE.R", sep = "/"),
					local = TRUE)
	}
# In all cases update the VERTEX_STATE_DIST.csv table of course-unit subgraph-vertex states by invoking INDIVIDUAL_KNOW_STATE_QUERY.R and
# then plot the result through PLOT_UNIT_SUBGRAPH.R.
	source(file = paste(subroutine.dir, "INDIVIDUAL_KNOW_STATE_QUERY.R", sep = "/"),
				local = TRUE)
	source(file = paste(subroutine.dir, "PLOT_UNIT_SUBGRAPH.R", sep = "/"),
				local = TRUE)
	source(file = paste(subroutine.dir, "QUERY_EoL_SUMMY.R", sep = "/"),
				local = TRUE)
#
