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
	library(gRain)
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
	KNOW_STATE_SPEC <- read.csv(file = paste(PROF_TASK.dir, "KNOW_STATE_SPEC.csv", sep = "/"),
														colClasses = "character")
	COURSE_ENROLL <- read.csv(file = paste(PROF_TASK.dir, "COURSE_ENROLL.csv", sep = "/"),
													colClasses = "character")
	EoL_MEAS <- read.csv(file = paste(PROF_TASK.dir, "EoL_MEAS.csv", sep = "/"),
										colClasses = "character")[c("STUDENT_ID","LEARNING_STANDARD_ID",
																					"MEAS_EVIDENCE","DATE_OF_MEAS")]
	LEARNING_STANDARD <- read.csv(file = paste(PROF_TASK.dir, "SIHLEARNING_STANDARD.csv", sep = "/"),
															colClasses = "character")[c("LEARNING_STANDARD_ID",
																									"LEARNING_STANDARD_CD",
																									"STANDARD_CONTENT_ID")]
	STANDARD_CONTENT <- read.csv(file = paste(PROF_TASK.dir, "SIHSTANDARD_CONTENT.csv", sep = "/"),
															colClasses = "character")[c("STANDARD_CONTENT_ID",
																									"SUBJECT_TITLE",
																								"STANDARD_CONTENT_TITLE")]
	UNIT_MAP_EDGE <- read.csv(file = paste(PROF_TASK.dir, "UNIT_MAP_EDGE_LIST.csv", sep = "/"),
										colClasses = "character")
	for (col_idx in c("IN_UNIT_FROM","IN_UNIT_TO")) UNIT_MAP_EDGE[,col_idx] <- as.logical(UNIT_MAP_EDGE[,col_idx])
	for(col_idx in c("LEARNING_STANDARD_ID_FROM","LEARNING_STANDARD_ID_TO")) UNIT_MAP_EDGE[,col_idx] <- paste("X",
																																																			UNIT_MAP_EDGE[,col_idx],
																																																			sep = "")
	UNIT_MAP_VERT <- data.frame(LEARNING_STANDARD_ID = unique(unlist(UNIT_MAP_EDGE[c("LEARNING_STANDARD_ID_TO",
																																									"LEARNING_STANDARD_ID_FROM")])))
#
	LEARNING_STANDARD <- merge(x = LEARNING_STANDARD,
															y = STANDARD_CONTENT)
	EoL_MEAS <- merge(x = EoL_MEAS,
										y = LEARNING_STANDARD[c("LEARNING_STANDARD_ID","LEARNING_STANDARD_CD",
																							"SUBJECT_TITLE","STANDARD_CONTENT_TITLE")])
#
#     Extract from UNIT_MAP_EDGE a list of in-scope vertices, and assign to IN_SCOPE_VERT.  Do this in two steps.  First, concatenate the
#     LEARNING_STANDARD_ID and IN_UNIT columns for the parent and child vertices, retaining only unique instances.  Use rbind to concatenate
#     and setNames to reconcile the names. Then select only records for which IN_UNIT is true, retaining only the LEARNING_STANDARD_ID attribute.
	IN_SCOPE_VERTEX <- unique(rbind(setNames(object = UNIT_MAP_EDGE[c("LEARNING_STANDARD_ID_FROM","IN_UNIT_FROM")],
																				nm = c("LEARNING_STANDARD_ID","IN_UNIT")),
															setNames(object = UNIT_MAP_EDGE[c("LEARNING_STANDARD_ID_TO","IN_UNIT_TO")],
																			nm = c("LEARNING_STANDARD_ID","IN_UNIT"))))
	IN_SCOPE_VERTEX <- IN_SCOPE_VERTEX[IN_SCOPE_VERTEX[,"IN_UNIT"],]["LEARNING_STANDARD_ID"]
#
# 2︎⃣ Window the EoL_MEAS learning-measurement table. First window by subjects with STUDENT_IDs in COURSE_ENROLL for the
#       COURSE_ID, CLASS_ID  specified by the corresponding values of USE_CASE_ATTRIBUTES. Create a "windowed" version of
#       COURSE_ENROLL.  Then merge the result with EoL_MEAS.
	SECT_ENROLL <- data.frame(rbind(USE_CASE_ATTRIBUTES[c("COURSE_ID","CLASS_ID"),"VALUE"]))
	colnames(SECT_ENROLL) <- c("COURSE_ID","CLASS_ID")
	SECT_ENROLL <- merge(x = SECT_ENROLL,
											y = COURSE_ENROLL)
	EoL_MEAS <- merge(x = COURSE_ENROLL["STUDENT_ID"],
										y = EoL_MEAS,
										all.x = TRUE)
#	EoL_MEAS <- EoL_MEAS[EoL_MEAS[,"STUDENT_ID"] %in% COURSE_ENROLL[,"STUDENT_ID"],]
	EoL_MEAS[!is.na(EoL_MEAS[,"LEARNING_STANDARD_ID"]),"LEARNING_STANDARD_ID"] <- 
									paste("X",
												EoL_MEAS[!is.na(EoL_MEAS[,"LEARNING_STANDARD_ID"]),"LEARNING_STANDARD_ID"],
												sep = "")
#
#      Now filter EoL_MEAS according to the LEARNING_STANDARD_IDs included in the graph.  These are obtained by calculating the intersection
#      between the column names of UNIT_SUBMAP_JDF and the instances of LEARNING_STANDARD_ID in EoL_MEAS.  The procedure here
#      is slightly more-difficult, given that we want the LEARNING_STANDARD_ID instances in IN_SCOPE_VERTEX, derived from UNIT_SUBMAP_JDF,
#      as well as STUDENT_IDs for which no measurements are available.
	EoL_MEAS <- rbind(EoL_MEAS[EoL_MEAS[,"LEARNING_STANDARD_ID"] %in% UNIT_MAP_VERT[,"LEARNING_STANDARD_ID"],],
									EoL_MEAS[apply (X = is.na(EoL_MEAS), MARGIN = 1, FUN = any), ])
#
#      Time-window EoL measurements. Apply the following procedure.
#       ⓐ First, sort the measurements in decreasing order of DATE_OF_MEAS.  
#       ⓑ Then truncate EoL_MEAS to include only measurements up to DATE_LATEST_MEAS from the USE_CASE_ATTRIBUTES table.
#            Special handling is required due to the presence of subjects (learners, students) for which no evidence of learning is present. 
#            The logic of time-windowing looses those records.  So they must be  reintroduced.
#       ⓒ  Retain only the most-recent measurement in instances for which a given LEARNING_STANDARD_ID variable has been
#             measured multiple times for a subject (student or learner).  Accomplish this with the duplicated logic. Since we sorted 
#             in order of decreasing DATE_OF_MEAS, all STUDENT_ID × LEARNING_STANDARD_ID pairs after the first occurrence
#             of each are identified as duplicated.
#
	EoL_MEAS <- EoL_MEAS[order(x = as.Date(EoL_MEAS[,"DATE_OF_MEAS"],"%Y-%m-%d"),
													decreasing = TRUE),]
	EoL_MEAS <- rbind(EoL_MEAS[which(as.Date(EoL_MEAS[,"DATE_OF_MEAS"],"%Y-%m-%d") <=
													as.Date(USE_CASE_ATTRIBUTES["DATE_LATEST_MEAS","VALUE"],"%Y-%m-%d")    
														), ],
									EoL_MEAS[apply (X = is.na(EoL_MEAS), MARGIN = 1, FUN = any), ])
	EoL_MEAS <- EoL_MEAS[!duplicated(EoL_MEAS[c("STUDENT_ID", "LEARNING_STANDARD_ID")]),]
	EoL_MEAS <- EoL_MEAS[order(EoL_MEAS[,"STUDENT_ID"],
														EoL_MEAS[,"LEARNING_STANDARD_ID"]),]
#
# 3︎⃣ Impute IMPLIED_KNOW_STATE to MEAS_EVIDENCE in EoL_MEAS.  Assign IMPLIED_KNOW_STATE based on threshold intervals
#      in KNOW_STATE_SPEC.  We first need to coerce MEAS_EVIDENCE to numeric.
	EoL_MEAS[,"MEAS_EVIDENCE"] <- as.numeric(EoL_MEAS[,"MEAS_EVIDENCE"])
	EoL_MEAS[,"IMPLIED_KNOW_STATE"] <- cut(x = EoL_MEAS[,"MEAS_EVIDENCE"],
																				breaks = unique(unlist(KNOW_STATE_SPEC[c("LOW_BOUND","UP_BOUND")])),
																				labels = KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"],
																				include.lowest = TRUE,
																				ordered.result = TRUE)
	EoL_MEAS[,"IMPLIED_KNOW_STATE"] <- as.character(EoL_MEAS[,"IMPLIED_KNOW_STATE"])
#
# 4︎⃣ Identify the evidence states in EoL_MEAS.    We need to relate subjects (learners, students) to evidentiary profiles and
#      evidentiary states. We use these to marginalize, reduce, condition the Joint Distribution Functions (JDFs) for each disconnected subgraph cluster
#      of connected vertices. We also need the knowledge-state estimation profiles, the variables for which the Bayesian network produces estimates.
#      These estimates result from marginalization of the conditioned JDF with each variable in the estimation profile one at a time.
#      
#      We get at these by the following procedure.
#       ⓐ Reshape EoL_MEAS into wide-table format. We records for each subject indicating the IMPLED_KNOW_STATE of each 
#            measured variable. Evidentiary profiles and states vary between subjects (learners, students). Keeping track of these 
#            distinctions and applying each to the Bayesian Network represents the greatest source of complexity in this approach.
#       ⓑ Associate each learner with an evidentiary profile and an evidentiary state. Calculate signatures based on concatenation
#            of the variable names — column names of the wide table — and variable values indicating evidentiary state.
#       ⓒ Create EVID_PROF_STATE, a data frame containing unique rows in the wide-table EoL_MEAS table.
#       ⓓ Ascertain the knowledge-state estimated profile, the variables not included in the  evidentiary profile.
#      To summarize, we must manage two dimensions of combinatorial variability:  Subject evidentiary profiles and states, and their
#       coverage of disconnected clusters of connected subgraph vertices.  This requires two levels of categorization of evidentiary
#       profiles, states.
#
#       ⓐ Reshape EoL_MEAS into wide-table format. Assign the STUDENT_ID subject-unique attributes as the rownames for the
#            resulting data frame. Get rid of all remaining columns not pertaining to the possibly measured variables in IN_SCOPE_VERTEX.
	EoL_WIDE <- dcast(data = EoL_MEAS,
												formula = STUDENT_ID ~ LEARNING_STANDARD_ID,
												value.var = "IMPLIED_KNOW_STATE")
	rownames(EoL_WIDE) <- EoL_WIDE[,"STUDENT_ID"]
	for (col_idx in setdiff(colnames(EoL_WIDE),unlist(UNIT_MAP_VERT))) EoL_WIDE[col_idx] <- NULL
#
#	Write out a csv file containing the LEARNER_EVID_STATE.  It is derived from EoL_WIDE but has LEARNING_STANDARD_CD for its
# column names.
	LEARNER_EVID_STATE <- EoL_WIDE
	LEARNER_EVID_STATE.cols <- data.frame(LEARNING_STANDARD_ID = colnames(LEARNER_EVID_STATE))
	LEARNER_EVID_STATE.cols[,"LEARNING_STANDARD_ID"] <- gsub(x = LEARNER_EVID_STATE.cols[,"LEARNING_STANDARD_ID"],
																													pattern = "X",
																													replacement = "")
	LEARNER_EVID_STATE.cols <- merge(x = LEARNER_EVID_STATE.cols,
																y = LEARNING_STANDARD)
	colnames(LEARNER_EVID_STATE) <- LEARNER_EVID_STATE.cols[,"LEARNING_STANDARD_CD"]
	LEARNER_EVID_STATE["STUDENT_ID"] <- rownames(EoL_WIDE)
	LEARNER_EVID_STATE <- merge(x = LEARNER_EVID_STATE,
															y = COURSE_ENROLL[c("STUDENT_ID","STUDENT_NAME","CLASS_ID")])
	for (col_idx in colnames(LEARNER_EVID_STATE)) LEARNER_EVID_STATE[,col_idx] <- enc2utf8(as.character(LEARNER_EVID_STATE[,col_idx]))
	write.csv(x = LEARNER_EVID_STATE[c("STUDENT_ID","STUDENT_NAME","CLASS_ID",LEARNER_EVID_STATE.cols[,"LEARNING_STANDARD_CD"])],
					file = paste(PROF_TASK.dir, "LEARNER_EVID_STATE.csv", sep = "/"),
					row.names = FALSE,
					eol = "\r\n",
					fileEncoding = "UTF-8",
					quote = TRUE)
# 
#       ⓑ Associate each learner with an evidentiary profile and an evidentiary state. We specifically seek the above-described
#            evidentiary-profile and -state signatures. Getting the evidentiary-profile signature requires three steps.
#              ⅰ. LIst the column names for the non-NA evidentiary state for each learner;
#             ⅱ. Concatenate the non-NA evidentiary-state variables into a signature; and
#            ⅲ. Write the results as an colum to EoL_WIDE.
#            We encounter subjects (learners, students) and clusters for which no measurements are avaiable.  Our syntax logic 
#             returns a blank for these instances. Replace the blank with "UNMEASURED".
	EoL_WIDE["EVID_PROF_SIG"] <- unlist(lapply(X = lapply(X = apply(X = !is.na(EoL_WIDE[intersect(colnames(EoL_WIDE),
																																								unlist(UNIT_MAP_VERT))]),
																												MARGIN = 1,
																												FUN = which),
																								FUN = names),
																				FUN = paste,
																				collapse = "_"))
	EoL_WIDE[apply(X = is.na(EoL_WIDE[intersect(colnames(EoL_WIDE),
																				unlist(UNIT_MAP_VERT))]),
								MARGIN = 1,
								FUN = all),"EVID_PROF_SIG"] <- "UNMEASURED"
#
#            The evidentiary-state signatures are simpler to obtain. Simply row-concatenate all of the evidentiary-state variables.  As with 
#            the EVID_PROF_SIG, we want evidentiary-state signatures for which no evidence is instantiated to be "UNMEASURED".
#            Excise all "NAs".
	EoL_WIDE["EVID_STATE_SIG"] <- apply(X = EoL_WIDE[intersect(colnames(EoL_WIDE),
																											unlist(UNIT_MAP_VERT))],
																	MARGIN = 1,
																	FUN = paste,
																	collapse = "_")
	EoL_WIDE[,"EVID_STATE_SIG"] <- gsub(x = EoL_WIDE[,"EVID_STATE_SIG"],
																	pattern = "NA_",
																	replacement = "")
	EoL_WIDE[,"EVID_STATE_SIG"] <- gsub(x = EoL_WIDE[,"EVID_STATE_SIG"],
																	pattern = "_NA",
																	replacement = "")
	EoL_WIDE[apply(X = is.na(EoL_WIDE[intersect(colnames(EoL_WIDE),
																				unlist(UNIT_MAP_VERT))]),
								MARGIN = 1,
								FUN = all),"EVID_STATE_SIG"] <- "UNMEASURED"
#
#       ⓒ Create EVID_PROF_STATE, a data frame containing unique evidentiary-profile signatures. Then, add a column containing the unique
#            evidentiary states for each evidentiary profile.
	EVID_PROF_STATE <- unique(EoL_WIDE["EVID_PROF_SIG"])
	rownames(EVID_PROF_STATE) <- EVID_PROF_STATE[, "EVID_PROF_SIG"]
	EVID_STATE <- list()
	for (prof_idx in rownames(EVID_PROF_STATE)){							## prof_idx <- rownames(EVID_PROF_STATE)[3]
		EVID_STATE[[prof_idx]] <- EoL_WIDE[EoL_WIDE[,"EVID_PROF_SIG"] == prof_idx,intersect(colnames(EoL_WIDE),
																																							unlist(UNIT_MAP_VERT))]
		EVID_STATE[[prof_idx]] <- unique(EVID_STATE[[prof_idx]][apply(X = !is.na(EVID_STATE[[prof_idx]]),
																											MARGIN = 2,
																											FUN = all)])
		rownames(EVID_STATE[[prof_idx]]) <- apply(X = EVID_STATE[[prof_idx]],
																			MARGIN = 1,
																			FUN = paste,
																			collapse = "_")
	}
	EVID_PROF_STATE[["EVID_STATE"]] <- EVID_STATE
#
#       ⓓ Ascertain the knowledge-state estimated profile.
	EVID_PROF_STATE[["EST_PROF"]] <- lapply(X = lapply(X = EVID_PROF_STATE[["EVID_STATE"]],
																							FUN = colnames),
																			FUN = setdiff,
																			x = unlist(UNIT_MAP_VERT) )
#
#            Finally, the EVID_PROF_SIG column — encapsulated in the row names of EVID_PROF_STATE — is redundant and can be removed
	EVID_PROF_STATE["EVID_PROF_SIG"] <- NULL
#
# 🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉
# 6︎⃣ Translate each EVIDENTIARY STATE into an estimated KNOWLEDGE STATE.  Our complex data frame EVID_PROF_STATE now contains
#      all of the structure we need to estimate knowledge states. We accomplish this via a Bayesian-network instantiation. Use the gRain package to
#      calcualte.  The major steps follow.
#      ⓐ Construct the graphical-independence network (Bayesian network) using tools from the gRain package.  The steps include
#          ⅰ Construct a list of conditional-probability relationships. 
#        ⅱAssociate the conditional-probability relationships with conditional-probability values from CPD_LONG.
#       ⅲ Declare the conditional-probability tables using the gRain function cptable.
#        ⅳ Compile the graphical independence network.
#      ⓑ Query the Bayesian Network by evidentiary profile and evidentiary state.
#
#      ⓐ Construct the graphical-independence network 
#          ⅰ Construct a list of conditional-probability relationships. 
#             List-aggregate LEARNING_STANDARD_ID_FROM records in UNIT_MAP_EDGE. First create a "working" copy
#             of UNIT_MAP_EDGES to which we prepend the character "X" to the LEARNING_STANDARD_ID attributes.  
#             List-aggreage the parent-vertex LEARNING_STANDARD_ID_FROM attributes in terms of the child-vertex LEARNING_STANDARD_ID_TOs.
#             This turns the parent-vertex column LEARNING_STANDARD_FROM into a list of lists. Add the root-edge parent vertices, LEARNING_STANDARD_ID_FROM 
#             instances that do not appear in LEARNING_STANDARD_ID_TO.   Resulting parent-vertex LEARNING_STANDARD_ID_FROM values are NULL.
#             Coerce these instances into empty lists.
	COND_PROB_RELATIONS <- aggregate(formula = LEARNING_STANDARD_ID_FROM ~ LEARNING_STANDARD_ID_TO,
																		data = UNIT_MAP_EDGE[c("LEARNING_STANDARD_ID_TO","LEARNING_STANDARD_ID_FROM")],
																		FUN = list)
	rownames(COND_PROB_RELATIONS) <- COND_PROB_RELATIONS[,"LEARNING_STANDARD_ID_TO"]
	COND_PROB_RELATIONS[setdiff(UNIT_MAP_EDGE[,"LEARNING_STANDARD_ID_FROM"],
															UNIT_MAP_EDGE[,"LEARNING_STANDARD_ID_TO"]),"LEARNING_STANDARD_ID_TO"] <-
																			setdiff(UNIT_MAP_EDGE[,"LEARNING_STANDARD_ID_FROM"],
																						UNIT_MAP_EDGE[,"LEARNING_STANDARD_ID_TO"])
	names(COND_PROB_RELATIONS[["LEARNING_STANDARD_ID_FROM"]]) <- COND_PROB_RELATIONS[,"LEARNING_STANDARD_ID_TO"]
	null.list <- names(which(unlist(lapply(X = COND_PROB_RELATIONS[["LEARNING_STANDARD_ID_FROM"]],
																FUN = is.null))))
	for (tgt_var_idx in null.list) COND_PROB_RELATIONS[["LEARNING_STANDARD_ID_FROM"]][[tgt_var_idx]] <- list()
#
#        ⅱ  Associate the conditional-probability relationships with conditional-probability values from CPD_LONG.
#            Begin by list-aggregating CPD long in order to get a list of values.  First coerce the COND_PROB atributes in CPD_LONG to numeric.
#            Assign the TARG_VERT_CFG-attribute values — the number of parent vertices for the corresponding to the target vertex — as the rownames
#            for the resulting data frame.
#
#            Begin by reading in CPD_LONG.  We deferrred it because its length renders it time-intensive. The size of the individual CPD tables increases
#            exponentially with the number of parent vertices. Limiting the number of rows limits the processing delay associated with reading the table
#            in. The number of needed rows results form a geometric series whose base is the number of IMPLIED_KNOW_STATE levels and whose exponents are 
#            the numbers of parent vertices.  The maximum number of parent vertices in UNIT_MAP_EDGE gives the limit to the geometric series. This
#            limit is the length of the largest LEARNING_STANDARD_ID_FROM in COND_PROB_RELATIONS.
	CPD_LONG.series_lim <- max(unlist(lapply(X = COND_PROB_RELATIONS[["LEARNING_STANDARD_ID_FROM"]],
																		FUN = length)))
	CPD_LONG.series_base <- length(KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"])
	CPD_LONG.nrows <- CPD_LONG.series_base*((CPD_LONG.series_base^(CPD_LONG.series_lim+1))-1)/(CPD_LONG.series_base-1)
	CPD_LONG <- read.csv(file = paste(PROF_TASK.dir, "CPD_LONG.csv", sep = "/"),
										colClasses = "character",
										nrows = CPD_LONG.nrows)
#
	CPD_LONG[,"COND_PROB"] <- as.numeric(CPD_LONG[,"COND_PROB"])
	COND_PROB_MEAS <- aggregate(formula =  COND_PROB ~ TARG_VERT_CFG,
															data = CPD_LONG[c("COND_PROB","TARG_VERT_CFG")],
															FUN = list)
	rownames(COND_PROB_MEAS) <- COND_PROB_MEAS[,"TARG_VERT_CFG"]
	names(COND_PROB_MEAS[["COND_PROB"]]) <- COND_PROB_MEAS[,"TARG_VERT_CFG"]
#
#            Reassign the values for the root/parent edge vertices. Assign the prior distribution as the proportion of measures in each IMPLIED_KNOW_STATE
#            category.  Assign zero to knowledge-state categories within which no subjects (learners, students) are observed.
	COND_PROB_MEAS[["COND_PROB"]][["0"]] <- as.vector(table(EoL_MEAS["IMPLIED_KNOW_STATE"])[KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"]]/
																						sum(table(EoL_MEAS["IMPLIED_KNOW_STATE"])))
	COND_PROB_MEAS[["COND_PROB"]][["0"]][is.na(COND_PROB_MEAS[["COND_PROB"]][["0"]])] <- 0
#
#            Finally assign as a COND_PROB column in COND_PROB_RELATIONS the list in COND_PROB column of COND_PROB_MEAS according to the
#            length of the parent vertices. We first must specify this length as a TARGET_VERT_CFG attribute.
	COND_PROB_RELATIONS["TARG_VERT_CFG"] <- as.character(unlist(lapply(X = COND_PROB_RELATIONS[["LEARNING_STANDARD_ID_FROM"]],
																																FUN = length)))
	COND_PROB_RELATIONS[["COND_PROB"]] <- COND_PROB_MEAS[["COND_PROB"]][COND_PROB_RELATIONS[,"TARG_VERT_CFG"]]
	names(COND_PROB_RELATIONS[["COND_PROB"]]) <- COND_PROB_RELATIONS[,"LEARNING_STANDARD_ID_TO"]
#
#       ⅲ Declare the conditional-probability tables.  Use the most-explicit notation.  First construct the conditional relationship in three steps of
#            string-paste operations. First, paste-collapse the members of each LEARNING_STANDARD_FROM list.  Then prepend the
#            LEARNING_STANDARD_ID_FROM value using " | " as a separator.  Finally, prepend with the tilda. Conditionally handle specially
#            the root-edge vertices, for which no parent vertices are specified. 
	COND_PROB_RELATIONS["COND_EQN"] <- unlist(lapply(X = COND_PROB_RELATIONS[["LEARNING_STANDARD_ID_FROM"]],
																									FUN = paste,
																									collapse = " + "))
	COND_PROB_RELATIONS[COND_PROB_RELATIONS[,"TARG_VERT_CFG"] == "0","COND_EQN"] <- NA
	COND_PROB_RELATIONS[!is.na(COND_PROB_RELATIONS["COND_EQN"]),"COND_EQN"] <- 
																		apply(X = COND_PROB_RELATIONS[!is.na(COND_PROB_RELATIONS["COND_EQN"]),
																																	c("LEARNING_STANDARD_ID_TO","COND_EQN")],
																						MARGIN = 1,
																						FUN = paste,
																						collapse = " | ")
	COND_PROB_RELATIONS[is.na(COND_PROB_RELATIONS["COND_EQN"]),"COND_EQN"] <- 
																			COND_PROB_RELATIONS[is.na(COND_PROB_RELATIONS["COND_EQN"]),"LEARNING_STANDARD_ID_TO"]
	COND_PROB_RELATIONS["COND_EQN"] <- paste("~", COND_PROB_RELATIONS[,"COND_EQN"])
#
#            Now, loop through the rows of COND_PROB_RELATIONS, declaring the conditional-probabilty table for each.
	CP_TABLE <- list()
	for (row_idx in rownames(COND_PROB_RELATIONS)) CP_TABLE[[row_idx]] <- cptable(vpar = as.formula(COND_PROB_RELATIONS[row_idx,"COND_EQN"]),
																																				values = COND_PROB_RELATIONS[["COND_PROB"]][[row_idx]],
																																				levels = KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"])
	COND_PROB_RELATIONS[["CP_TABLE"]] <- CP_TABLE
#
#        ⅳ Compile the graphical independence network.
	UNIT_MAP_GRAIN.un_comp <- compileCPT(COND_PROB_RELATIONS[["CP_TABLE"]])
	UNIT_MAP_GRAIN <- compile(grain(compileCPT(COND_PROB_RELATIONS[["CP_TABLE"]])))
#
#      ⓑ Query the Bayesian Network by evidentiary profile and evidentiary state.   
	LEARNER_KNOW_STATE <- list()
	for (prof_idx in rownames(EVID_PROF_STATE)){										## prof_idx <- rownames(EVID_PROF_STATE)[1]
	# Extract from EVID_PROF_STATE the data frame containing evidentiary states associated with the prof_idxᵗʰ evidentiary profile.  Also 
	# extract the associated EST_PROF, the list of uninstantiated vertices for which knowledge-states are sought.
		EVID_STATE.prof_idx <- EVID_PROF_STATE[["EVID_STATE"]][[prof_idx]]
		EST_PROF.prof_idx <- EVID_PROF_STATE[["EST_PROF"]][[prof_idx]]
	#
	# Use the querygrain command to apply each evidentiary state. The command requires us to query EST_STATE for one EVID_STATE at 
	# a time.  Store the results for each EVID_PROF in a three-dimensional array.  Each third-dimensional slice of the array contains the complete
	# knowledge-state — both the observed evidentiary state EVID_STATE and the estimated state EST_STATE — for all vertices in the graph.
	# We need threfore to construct the array in two steps.  (The order doesn't necessarily matter.) We first query the Bayesian network to get
	# the EST_PROF for state_idxᵗʰ evidentiary state. We then represent the evidentiary state itself as a CPD table and write into the remaining 
	# rows of the corresponding array slice.  
	#
	# Begin by initializing the array. Our 
	# Now, loop through the evidentiary states of the prof_idxᵗʰ evidentiary profile. 
		if (length(EVID_STATE.prof_idx) > 0){
		LEARNER_KNOW_STATE.prof_idx <- array(dim = list(length(unique(unlist(UNIT_MAP_EDGE[c("LEARNING_STANDARD_ID_FROM",
																																								"LEARNING_STANDARD_ID_TO")]))),
																						length(KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"]),
																						nrow(EVID_STATE.prof_idx) ),
																				dimnames = list(unique(unlist(UNIT_MAP_EDGE[c("LEARNING_STANDARD_ID_FROM",
																																									"LEARNING_STANDARD_ID_TO")])),
																										KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"],
																										rownames(EVID_STATE.prof_idx )) )
	#
			for (state_idx in dimnames(LEARNER_KNOW_STATE.prof_idx)[[3]]){					## state_idx <- dimnames(LEARNER_KNOW_STATE.prof_idx)[[3]][1]
			# The EST_STATE for the state_idxᵗʰ results from querying the graphical independence network UNIT_MAP_GRAIN for the 
			# estimated uninstantiated-vertex CPDs resulting from instantiation of the corresponding evidentiary state.  We get this
			# by using querygrain with the nodes specified by the prof_idxᵗʰ estimation profile and the evidence by the state_idxᵗʰ evidentary state.
			# The querygrain function returns a list comprised of one marginal CPD for each vertex in the estimation profile. Concatenate the 
			# list into a data frame, specify the order of its rows so as to be consistent with EST_PROF.prof_idx, and then write the result
			# to the EST_PROF.prof_idx-coresponding rows of the state_idxᵗʰ slice of the three-dimensional array. Be completely 
			# explicit in terms of how the cells resulting from query grain are mapped to those of LEARNER_KNOW_STATE.
			# Handle conditionally.  The syntax is slightly different if only one vertex is measured.  Also, if all vertices are instantiated,
			# do not query the Bayesian network.
				if(length(EST_PROF.prof_idx) > 0){
					if (length(dimnames(LEARNER_KNOW_STATE.prof_idx)[[3]]) > 1){
						LEARNER_KNOW_STATE.prof_idx[EST_PROF.prof_idx,KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"],state_idx] <- 
																as.matrix(do.call(args = querygrain(object = UNIT_MAP_GRAIN,
																															nodes = EST_PROF.prof_idx,
																														evidence = EVID_STATE.prof_idx[state_idx,]),
																							what = rbind)[EST_PROF.prof_idx,KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"]])
					} else {
						LEARNER_KNOW_STATE.prof_idx[EST_PROF.prof_idx,KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"],state_idx] <- 
																as.matrix(do.call(args = querygrain(object = UNIT_MAP_GRAIN,
																														nodes = EST_PROF.prof_idx,
																														evidence = EVID_STATE.prof_idx),
																							what = rbind)[EST_PROF.prof_idx,KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"]])
					}
				}
			#
			# We now convert the evidentiary state into a simliar CPD, with values of unity for the IMPLIED_KNOW_STATE corresponding to the 
			# observed evidentiary state and zero otherwise.  Begin this step by initializing a matrix of zeros for which the rownames are the vertices
			# from the evidentiary profile and the columns the IMPLIED_KNOW_STATE_LEVELS. Conditionally assign to unity the matrix elements
			# for which a vertex from the evidentiary profile appears in the observed knowledge state.  Then apply the resulting values to the 
			# as-yet-unassigned elements of the state_idxᵗʰ slice of the LEARNER_KNOW_STATE.prof_idx array.  Again, evaluate conditionally when
			# only one evidentiary state is observed.
				EVID_STATE.state_idx <- matrix(data = 0,
																	dimnames = list(colnames(EVID_STATE.prof_idx),
																								KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"]),
																	nrow = ncol(EVID_STATE.prof_idx),
																	ncol = length(KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"]))
				if (nrow(EVID_STATE.state_idx) > 1){			
					EVID_STATE.state_idx[matrix(data = c(names(EVID_STATE.prof_idx[state_idx,]),
																				unlist(EVID_STATE.prof_idx[state_idx,])),
																	ncol = 2)] <- 1
				} else {
					EVID_STATE.state_idx[prof_idx,state_idx] <- 1				
				}
			LEARNER_KNOW_STATE.prof_idx[colnames(EVID_STATE.prof_idx),KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"],state_idx] <- EVID_STATE.state_idx
			}				## Close for (state_idx in dimnames(LEARNER_KNOW_STATE.prof_idx)[[3]])
		#
			} else {
				LEARNER_KNOW_STATE.prof_idx <- array(dim = list(length(unique(unlist(UNIT_MAP_EDGE[c("LEARNING_STANDARD_ID_FROM",
																																								"LEARNING_STANDARD_ID_TO")]))),
																							length(KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"]),
																							1 ),
																					dimnames = list(unique(unlist(UNIT_MAP_EDGE[c("LEARNING_STANDARD_ID_FROM",
																																										"LEARNING_STANDARD_ID_TO")])),
																											KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"],
																											"UNMEASURED") )
				EST_PROF_UNMEAS.prof_idx <- do.call(what = rbind, 
																					args = querygrain(object = UNIT_MAP_GRAIN,
																												nodes = EST_PROF.prof_idx,
																												evidence = EVID_STATE.prof_idx))
				LEARNER_KNOW_STATE.prof_idx[rownames(EST_PROF_UNMEAS.prof_idx),
																		colnames(EST_PROF_UNMEAS.prof_idx),
																		"UNMEASURED"] <- EST_PROF_UNMEAS.prof_idx
		# 
		}				## close if (length(EVID_STATE.prof_idx) > 0)
	#
	# Write the resulting LEARNER_KNOW_STATE.prof_idx array as the prof_idxᵗʰ element of list LEARNER_KNOW_STATE
		LEARNER_KNOW_STATE[[prof_idx]] <- LEARNER_KNOW_STATE.prof_idx
	#	
	}					## close for (prof_idx in rownames(EVID_PROF_STATE))
# 
# Add LEARNER_KNOW_STATE as a column to EVID_PROF_STATE.
	EVID_PROF_STATE[["LEARNER_KNOW_STATE"]] <- LEARNER_KNOW_STATE
#
# 7︎⃣ Associate the LEARNING_STANDARD_ID-marginalized CDFs.  EVID_PROF_STATE and EoL_WIDE contain the evidentiary framework by which
#      we assemble LEARNER_KNOW_STATE, our intended output. We again manage two dimensions of variabilty:  Evidentiary profiles and states,
#      and coverage thereof of the submap clusters.  Our approach follows.
#      ⓐ Expand the LEARNER_KNOW_STATE attributes into data frames. LEARNER_KNOW_STATE is provided for each evidentiary-profile ×
#           subraph configuration.  Each such LEARNER_KNOW_STATE pair contains an array whose dimensions are target-variable estimation profile ×
#           IMPLIED_KNOW_STATE × evidentiary state.  Reshape this into a two-dmensional table such that the evidentiary-state signature is 
#           a distinguishing attribute for the corresponding table of conditional probabilities of target-variable states.  We concatenate these
#           tables "vertically".  Then join them onto the corresponding CLUST_EVID_STATE tables.  The evidentiary-profile is also added as an attribute
#           to this data frame.  This gives us a data frame that can be joined onto EoL_WIDE, in order to associate the estimated knowledge-states
#           with individual subjects (learners, students).
#      ⓑ Merge the resulting LEARNER_KNOW_STATE tables with the EoL_WIDE.  We then have a wide-table with distinct columns for the
#           probability that the subject's knowledge is in a given state for each variable.
#      ⓒ Reshape the wide-table LEARNER_KNOW_STATE tables into long tables.  We want columns for the IMPLIED_KNOW_STATE as well
#           as a MEAS column with the probability that the subject is in the corresponding state for a given variable.
#      ⓓ Prepare the LEARNER_KNOW_STATE long table and write it out as a csv file.
#
#      ⓐ Expand the LEARNER_KNOW_STATE attributes into data frames. We seek a data frame whose rows are EVID_STATE_SIG × 
#           LEARNING_STANDARD_ID pairs and columns are IMPLIED_KNOW_STATE levels.  Begin with the LEARNER_KNOW_STATE
#           attribute of EVID_PROF_STATE.   Each such instance consists of three-dimensional arrays, whose dimensions are 
#           the unit-map vertices, IMPLIED_KNOW_STATE, and 
	KNOW_STATE <- list()
	for (prof_idx in rownames(EVID_PROF_STATE)){														## prof_idx <- rownames(EVID_PROF_STATE)[2]
	# First extract the EVID_STATE and KNOW_STATE attributes from EVID_PROF_STATE.  Also get EoL_WIDE records corresponding to the
	# prof_idxᵗʰ evidentiary profile.
		LEARNER_KNOW_STATE.prof_idx <- EVID_PROF_STATE[["LEARNER_KNOW_STATE"]][[prof_idx]]
		EVID_STATE.prof_idx <- EVID_PROF_STATE[["EVID_STATE"]][[prof_idx]]
		EoL_WIDE.prof_idx <- EoL_WIDE[EoL_WIDE[,"EVID_PROF_SIG"] == prof_idx,]
	#
	# Prepare EoL_WIDE.prof_idx to subsequently be merged with the wide-format KNOW_STATE table. We merge EoL_WIDE.prof_idx
	# with the data frame resulting from reshaping array KNOW_STATE.prof_idx into a data rame.  We will join by the EVID_STATE_SIG attribute. 
		EoL_WIDE.prof_idx["STUDENT_ID"] <-  rownames(EoL_WIDE.prof_idx) 
		EoL_WIDE.prof_idx <- EoL_WIDE.prof_idx[c("STUDENT_ID","EVID_STATE_SIG")]
	#
	# Brute-force loop through KNOW_STATE.prof_idx by the evidentiary-state dimension.  Begin by defining a data frame.  Concatenate the
	# evidentiary-state knowledge-state slices as we go.
		KNOW_STATE.prof_idx <- data.frame(EVID_STATE_SIG = character(),
																	LEARNING_STANDARD_ID = character())
		for(lvl_idx in KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"]) KNOW_STATE.prof_idx[,lvl_idx] <- numeric()
	#
	# Now cycle through the EVID_STATE attributes.
		for (state_idx in dimnames(LEARNER_KNOW_STATE.prof_idx)[[3]]){					## state_idx <- dimnames(LEARNER_KNOW_STATE.prof_idx)[[3]][1]
		# Build out a data frame for the state_idxᵗʰ slice of LEARNER_KNOW_STATE.prof_idx.  Get LEARNING_STANDARD_ID from the dimnames of the
		# first dimension of LEARNER_KNOW_STATE.prof_idx.  Then assign EVID_STATE as state_idx.
			KNOW_STATE.state_idx <- as.data.frame(x = LEARNER_KNOW_STATE.prof_idx[,,state_idx])
			KNOW_STATE.state_idx["LEARNING_STANDARD_ID"] <- dimnames(LEARNER_KNOW_STATE.prof_idx)[[1]]
			KNOW_STATE.state_idx["EVID_STATE_SIG"] <- state_idx
		#
		# Finally, concatenate KNOW_STATE.state_idx onto KNOW_STATE.prof_idx.
			KNOW_STATE.state_idx <- KNOW_STATE.state_idx[colnames(KNOW_STATE.prof_idx)]
			KNOW_STATE.prof_idx <- rbind(KNOW_STATE.prof_idx, KNOW_STATE.state_idx)
			rownames(KNOW_STATE.prof_idx) <- NULL
		#
		}
	#  ⓑ Merge the resulting LEARNER_KNOW_STATE tables with the EoL_WIDE.  
	# Merge KNOW_STATE.prof_idx with EoL_WIDE.prof_idx.  The dimensions coincide with the number of distinct STUDENT_IDs times
	# the number of submap vertices. Afterward, drop the EVID_STATE_SIG attribute, which is no longer needed. Then write 
	# KNOW_STATE.prof_idx as the prof_idxᵗʰ element of list KNOW_STATE.
		KNOW_STATE.prof_idx <- merge(x = KNOW_STATE.prof_idx,
																y = EoL_WIDE.prof_idx)
		KNOW_STATE.prof_idx["EVID_STATE_SIG"] <- NULL
		KNOW_STATE.prof_idx <- KNOW_STATE.prof_idx[c("STUDENT_ID","LEARNING_STANDARD_ID",
																							KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"])]
		KNOW_STATE[[prof_idx]] <- KNOW_STATE.prof_idx
	#
	}
#
#           Concatenate the elements of KNOW_STATE into a single data frame.
	KNOW_STATE <- do.call(what = rbind,
											args = KNOW_STATE)
	rownames(KNOW_STATE) <- NULL
#
#      ⓒ Reshape the wide-table LEARNER_KNOW_STATE tables into long tables.  
	KNOW_STATE_LONG <- melt(data = KNOW_STATE[c("STUDENT_ID","LEARNING_STANDARD_ID",
																							KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"])],
													id.vars = c("STUDENT_ID","LEARNING_STANDARD_ID"),
													meas.vars = KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"],
													variable.name = "IMPLIED_KNOW_STATE",
													value.name = "MEAS")
#       
#       Clean up the LEARNING_STANDARD_ID attribute. Truncate off the leading character "X".
	KNOW_STATE_LONG[,"LEARNING_STANDARD_ID"] <- gsub(x = KNOW_STATE_LONG[,"LEARNING_STANDARD_ID"],
																										pattern = "X",
																										replacement = "")
#
#          Now apply two date-stamp attributes to KNOW_STATE.  EVID_STATE_AS_OF is the date of the most-recent evidentiary state
#          measurement. KNOW_STATE_AS_OF is the date of the estimate, conditioned on the evidentiary state in the former.  We get
#          EVID_STATE_AS_OF by max-aggregation of EoL_MEAS and then merging the result by STUDENT_ID with KNOW_STATE.
#          KNOW_STATE_AS_OF is simply the system date of the calculation.
	EoL_MEAS[,"DATE_OF_MEAS"] <- as.Date(x = EoL_MEAS[,"DATE_OF_MEAS"], "%Y-%m-%d")
	EVID_STATE_AS_OF <- aggregate(formula = DATE_OF_MEAS ~ STUDENT_ID + LEARNING_STANDARD_ID,
															data = EoL_MEAS,
															FUN = max)
	colnames(EVID_STATE_AS_OF) <- c("STUDENT_ID","LEARNING_STANDARD_ID","EVID_STATE_AS_OF")
	EVID_STATE_AS_OF[,"LEARNING_STANDARD_ID"] <- gsub(x = EVID_STATE_AS_OF[,"LEARNING_STANDARD_ID"],
																										pattern = "X",
																										replacement = "")
	KNOW_STATE_LONG <- merge(x = KNOW_STATE_LONG,
															y = EVID_STATE_AS_OF,
															all.x = TRUE)
	KNOW_STATE_LONG["KNOW_STATE_AS_OF"] <- Sys.Date()
	KNOW_STATE_LONG <- merge(x = KNOW_STATE_LONG,
														y = LEARNING_STANDARD)
#
#          Finally, reorder the records according to STUDENT_ID, LEARNING_STANDARD_ID, IMPLIED_KNOW_STATE.  Then
#         coerce attributs to UTF-8 character and write out as a csv table.  
	KNOW_STATE_LONG[,"IMPLIED_KNOW_STATE"] <- factor(x = KNOW_STATE_LONG[,"IMPLIED_KNOW_STATE"],
																									levels = KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"])
	KNOW_STATE_LONG <- KNOW_STATE_LONG[order(KNOW_STATE_LONG[,"STUDENT_ID"],
																	KNOW_STATE_LONG[,"LEARNING_STANDARD_ID"],
																	KNOW_STATE_LONG[,"IMPLIED_KNOW_STATE"]),
															c("STUDENT_ID","LEARNING_STANDARD_ID","LEARNING_STANDARD_CD",
																"IMPLIED_KNOW_STATE","MEAS","EVID_STATE_AS_OF",
																"KNOW_STATE_AS_OF")]
	for (col_idx in c("EVID_STATE_AS_OF","KNOW_STATE_AS_OF","IMPLIED_KNOW_STATE")) KNOW_STATE_LONG[,col_idx] <- as.character(KNOW_STATE_LONG[,col_idx])
	for (col_idx in colnames(KNOW_STATE_LONG)) {
		KNOW_STATE_LONG[,col_idx] <- enc2utf8(as.character(KNOW_STATE_LONG[,col_idx]))
		KNOW_STATE_LONG[is.na(KNOW_STATE_LONG[,col_idx]),col_idx] <- ""
	}
#
	write.csv(x = KNOW_STATE_LONG,
					file = paste(PROF_TASK.dir, "LEARNER_KNOW_STATE.csv", sep = "/"),
					row.names = FALSE,
					eol = "\r\n",
					fileEncoding = "UTF-8",
					quote = TRUE)
#
# ‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️
# INTELLECTUAL PROPERTY NOTICE:  This subroutine uses the gRain package for compiling and querying Bayesian networks.
# Available at the normal CRAN-repository site, gRain involves a dependency upon a dependency on RGBL.  RGBL is not distributed
# via the CRAN-repository. It alternatively is distributed by Bioconductor at
#  http://www.bioconductor.org/packages/release/bioc/html/RBGL.html.
# ‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️
#