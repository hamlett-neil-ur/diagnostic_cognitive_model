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
#   ‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️
#     We need one JDF for each course-unit vertex cluster. Also, remove a leading substring "X" from the column names of the
#     UNIT_SUBMAP_JDF data frames. These were introduced becuase the colClasses were not constrained to be "character."
	jdf_files <- list.files(PROF_TASK.dir)[grep(x = list.files(PROF_TASK.dir),
																	pattern = "UNIT_MAP_JDF.csv")]
	UNIT_SUBMAP_JDF <- list()
	for (jdf_idx in jdf_files) {											## jdf_idx <- jdf_files[1]
		print(paste("Reading in SUBMAP_JDF", 
							jdf_idx, 
							"Starting at",
							format(Sys.time(),"%H:%M:%S")))
		UNIT_SUBMAP_JDF[[jdf_idx]] <- read.csv(file = paste(PROF_TASK.dir, jdf_idx, sep = "/"))
		print(paste("Reading in SUBMAP_JDF", 
							jdf_idx, 
							"Ending at",
							format(Sys.time(),"%H:%M:%S")))
	}
#
#   ‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️‼️
#     Read in other files listed above.
	KNOW_STATE_SPEC <- read.csv(file = paste(PROF_TASK.dir, "KNOW_STATE_SPEC.csv", sep = "/"),
														colClasses = "character")
	COURSE_ENROLL <- read.csv(file = paste(PROF_TASK.dir, "COURSE_ENROLL.csv", sep = "/"),
													colClasses = "character")
	EoL_MEAS <- read.csv(file = paste(PROF_TASK.dir, "EoL_MEAS.csv", sep = "/"),
										colClasses = "character")[c("STUDENT_ID","LEARNING_STANDARD_ID",
																					"MEAS_EVIDENCE","DATE_OF_MEAS")]
	LEARNING_STANDARD <- read.csv(file = paste(PROF_TASK.dir, "SIHLEARNING_STANDARD.csv", sep = "/"),
										colClasses = "character")[c("LEARNING_STANDARD_ID",
																					"LEARNING_STANDARD_CD")]
#
# 2︎⃣ Window the EoL_MEAS learning-measurement table. First window by subjects with STUDENT_IDs in COURSE_ENROLL for the
#       COURSE_ID, SECTION_ID  specified by the corresponding values of USE_CASE_ATTRIBUTES. Create a "windowed" version of
#       COURSE_ENROLL.  Then merge the result with EoL_MEAS.
	SECT_ENROLL <- data.frame(rbind(USE_CASE_ATTRIBUTES[c("COURSE_ID","SECTION_ID"),"VALUE"]))
	colnames(SECT_ENROLL) <- c("COURSE_ID","SECTION_ID")
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
#      is slightly more-difficult, given that we want the LEARNING_STANDARD_ID instances in SUBMAP_VERTEX, derived from UNIT_SUBMAP_JDF,
#      as well as STUDENT_IDs for which no measurements are available.
	SUBMAP_VERTEX <- data.frame(LEARNING_STANDARD_ID = intersect(unique(EoL_MEAS[,"LEARNING_STANDARD_ID"]),
																														unlist(lapply(X = UNIT_SUBMAP_JDF,
																																			FUN = colnames))  
																													)
														)
	EoL_MEAS <- rbind(EoL_MEAS[EoL_MEAS[,"LEARNING_STANDARD_ID"] %in% SUBMAP_VERTEX[,"LEARNING_STANDARD_ID"],],
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
#       ⓓ Allocate in EVID_PROF_STATE the variables in the evidentiary profile to each disconnected cluster of connected subgraph
#            vertices. Collect this information for each cluster a distinct data frame of unique evidentiary-state configurations.  The 
#            column names represent the variables in the cluster-allocated evidentiary profile. The rownames contain the cluster-allocated
#            evidentiary states.
#       ⓔ For each subgraph cluster, ascertain the knowledge-state estimated profile, the variables in the cluster not included in the
#            cluster-allocated evidentiary profile.
#      To summarize, we must manage two dimensions of combinatorial variability:  Subject evidentiary profiles and states, and their
#       coverage of disconnected clusters of connected subgraph vertices.  This requires two levels of categorization of evidentiary
#       profiles, states.
#
#       ⓐ Reshape EoL_MEAS into wide-table format. Assign the STUDENT_ID subject-unique attributes as the rownames for the
#            resulting data frame. Get rid of all remaining columns not pertaining to the possibly measured variables in SUBMAP_VERTEX.
	EoL_WIDE <- dcast(data = EoL_MEAS,
												formula = STUDENT_ID ~ LEARNING_STANDARD_ID,
												value.var = "IMPLIED_KNOW_STATE")
	rownames(EoL_WIDE) <- EoL_WIDE[,"STUDENT_ID"]
	for (col_idx in setdiff(colnames(EoL_WIDE),unlist(SUBMAP_VERTEX))) EoL_WIDE[col_idx] <- NULL
#
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
	EoL_WIDE["EVID_PROF_SIG"] <- unlist(lapply(X = lapply(X = apply(X = !is.na(EoL_WIDE[,unlist(SUBMAP_VERTEX)]),
																												MARGIN = 1,
																												FUN = which),
																								FUN = names),
																				FUN = paste,
																				collapse = "_"))
	EoL_WIDE[nchar(EoL_WIDE[,"EVID_PROF_SIG"]) == 0,"EVID_PROF_SIG"] <- "UNMEASURED"
#
#            The evidentiary-state signatures are simpler to obtain. Simply row-concatenate all of the evidentiary-state variables.  As with 
#            the EVID_PROF_SIG, we want evidentiary-state signatures for which no evidence is instantiated to be "UNMEASURED".
#            Replace all such instances — assigned "NA_ ...." by our syntax logic with "UNMEASURED".
	EoL_WIDE["EVID_STATE_SIG"] <- apply(X = EoL_WIDE[unlist(SUBMAP_VERTEX)],
																	MARGIN = 1,
																	FUN = paste,
																	collapse = "_")
	EoL_WIDE[grep(x = EoL_WIDE[,"EVID_STATE_SIG"],
								pattern = "NA_"),"EVID_STATE_SIG"] <- "UNMEASURED"
#
#       ⓒ Create EVID_PROF_STATE, a data frame containing unique evidentiary-profile signatures. Then, add a column containing the unique
#            evidentiary states for each evidentiary profile.
	EVID_PROF_STATE <- unique(EoL_WIDE["EVID_PROF_SIG"])
	rownames(EVID_PROF_STATE) <- EVID_PROF_STATE[, "EVID_PROF_SIG"]
	EVID_STATE <- list()
	for (prof_idx in rownames(EVID_PROF_STATE)){							## prof_idx <- rownames(EVID_PROF_STATE)[3]
		EVID_STATE[[prof_idx]] <- EoL_WIDE[EoL_WIDE[,"EVID_PROF_SIG"] == prof_idx,unlist(SUBMAP_VERTEX)]
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
#       ⓓ Allocate in EVID_PROF_STATE the variables in the evidentiary profile to each cluster.  Vertex membership in 
#            clusters is contained in the column names of UNIT_SUBMAP_JDF.  We want to build up external to EVID_PROF_STATE
#            a list of data frames and then write it back into EVID_PROF_STATE as CLUST_EVID_STATE.
	CLUST_EVID_STATE <- list()
	CLUST_EVID_PROF <- lapply(X = lapply(X = UNIT_SUBMAP_JDF,
																	FUN = colnames),
													FUN = intersect,
													colnames(EoL_WIDE))
#
#            CLUST_EVID_PROF now contains the all of the  the evidentiary profiles in EoL_WIDE. We now need to subset
#            according to scope of the subtraph clusters in EVID_PROF_STATE.
	for (prof_idx in unique(EVID_PROF_STATE[,"EVID_PROF_SIG"])){					## prof_idx <- unique(EVID_PROF_STATE[,"EVID_PROF_SIG"])[3]
		EVID_PROF_STATE.prof_idx <- EVID_PROF_STATE[["EVID_STATE"]][[prof_idx]]
		CLUST_EVID_STATE.prof_idx <- list()
		for (clust_idx in names(CLUST_EVID_PROF)){ 												## clust_idx <- names(CLUST_EVID_PROF)[1]
			CLUST_EVID_STATE.prof_idx[[clust_idx]] <- EVID_PROF_STATE.prof_idx[intersect(colnames(EVID_PROF_STATE.prof_idx),
																																				CLUST_EVID_PROF[[clust_idx]])]
			CLUST_EVID_STATE.prof_idx[[clust_idx]] <- unique(CLUST_EVID_STATE.prof_idx[[clust_idx]][apply(X = !is.na(CLUST_EVID_STATE.prof_idx[[clust_idx]]),
																																										MARGIN = 2,
																																										FUN = all)])
			rownames(CLUST_EVID_STATE.prof_idx[[clust_idx]]) <- apply(X = CLUST_EVID_STATE.prof_idx[[clust_idx]],
																												MARG = 1,
																												FUN = paste,
																												collapse = "_")
		} 
		CLUST_EVID_STATE[[prof_idx]] <- CLUST_EVID_STATE.prof_idx
	}
	EVID_PROF_STATE[["CLUST_EVID_STATE"]] <- CLUST_EVID_STATE
#
#       ⓔ Provide the knowledge-state estimation profile by cluster for each evidentiary profile.
	TARG_EST_PROF <- list()
	for (prof_idx in rownames(EVID_PROF_STATE)) TARG_EST_PROF[[prof_idx]] <- lapply(X = lapply(X = UNIT_SUBMAP_JDF,
																																					FUN = colnames),
																																FUN = setdiff,
																																c(unlist(str_split(string = prof_idx, pattern = "_")),"MEAS"))
	EVID_PROF_STATE[["TARG_EST_PROF"]] <- TARG_EST_PROF
#
#      The EVID_PROF_SIG attribute is, finally, redundant and can be elimintated from EVID_PROF_STATE.
	EVID_PROF_STATE[["EVID_PROF_SIG"]] <- NULL
#
# 🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉🦉
# 6︎⃣ Translate each EVIDENTIARY STATE into an estimated KNOWLEDGE STATE.  Our complex data frame EVID_PROF_STATE now contains
#      all of the structure we need to estimate knowledge states. We accomplish this via a Bayesian-network instantiation. This includes the
#      following key steps.
#      ⓐ Marginalize UNIT_MAP_JDF according to the evidentiary profile. The evidentiary profile again conatins all of the attributes for which
#           evidence is asserted. The colum names of the data frame CLUST_EVID_STATE in EVID_PROF_STATE for each profile, each cluster
#           contain the evidentiary profile used for marginalization.
#      ⓑ Reduce the marginalized JDF according to the observed evidentiary state specified by the CLUST_EVID_STATE data frame in 
#           EVID_PROF_STATE.
#      ⓒ Condition the marginalized JDF on each of the observed evidentiary states.  This conditioned, marginalized distribution
#           becomes a three-dimensional array in which the first two dimensions represent CDFs. The third dimension represents the
#           evidentiary states with respect to each CDF is conditioned.
#      ⓓ Marginalize the resulting conditional, marginal distribution function with respect to each of the variables not included 
#           evidentiary profile. The resulting marginal distribution represents our estimate of the knowledge state conditioned on the
#           evidentiary state.
#     The procedure must be applied cluster-by-cluster. We accumulate all of the results into a three-dimensional array resembling
#      constructed for ⓑ above.  We work through according to the structure of EVID_PROF_STATE.  Loop through first according
#      to the evidentiary profile, EVID_PROF_SIG in EVID_PROF_STATE, then by unit-map connected-vertex cluster.
#
#     We seek to collect for each row in EVID_PROF_SIG an array of LEARNER_KNOW_STATE templates corresponding to each 
#     observed-variable evidentiary state. Each template should contain knowledge-state CPDs for each subnet vertex.  We 
#     subsequently use this to 
#
#     Begin by declaring an a list LEARNER_KNOW_STATE_ARR into which the learning-map states conditioned on each evidentiary
#     state are stored.
	LEARNER_KNOW_STATE <- list()
#
	for (prof_idx in rownames(EVID_PROF_STATE)){												## prof_idx <- rownames(EVID_PROF_STATE)[1]
		LEARNER_KNOW_STATE.prof_idx <- list()	
		for (clust_idx in names(UNIT_SUBMAP_JDF)){											## clust_idx  <- names(UNIT_SUBMAP_JDF)[1]
		# Extract the evidentiary profile for the cluster from EVID_PROF_CLUST.prof_idx and its JDF from UNIT_SUBMAP_JDF.
			CLUST_EVID_STATE.clust_idx <- EVID_PROF_STATE[["CLUST_EVID_STATE"]][[prof_idx]][[clust_idx]]
			EVID_PROF_CLUST.clust_idx <- colnames(CLUST_EVID_STATE.clust_idx)
			EVID_STATE_CLUST_SIG.clust_idx <- rownames(CLUST_EVID_STATE.clust_idx)
			if (length(EVID_STATE_CLUST_SIG.clust_idx) <1) EVID_STATE_CLUST_SIG.clust_idx <- "UNMEASURED"
			TARG_PROF_CLUS.clust_idx <- EVID_PROF_STATE[["TARG_EST_PROF"]][[prof_idx]][[clust_idx]]
			JDF.clust_idx <- UNIT_SUBMAP_JDF[[clust_idx]]
		#
		# Proceed conditionally.  If length EVID_PROF.clust_idx is greater than zero, then we condition JDF.clust_idx on its elements.  Otherwise,
		# convert JDF.clust_idx to a three-dimensional array with only one increment in the third dimension.  
			if(length(EVID_PROF_CLUST.clust_idx) > 0){
			# ⓐ Marginalize UNIT_MAP_JDF according to the evidentiary profile.  Marginalization
			#      results from sum-aggregation.  First construct the formula.
				marg_formula.clust_idx <- as.formula(paste("MEAS",
																						paste(EVID_PROF_CLUST.clust_idx,
																								collapse = " + "),
																			sep = " ~ "))
				print(paste("Marginalizing wrt observed profile:",
										prof_idx,
										", ", 
										clust_idx,
										"Starting at",
										format(Sys.time(),"%H:%M:%S")
							))
				MARG_JDF.cust_idx <- aggregate(formula = marg_formula.clust_idx,
																		data = JDF.clust_idx,
																		FUN = sum)
				print(paste("Marginalizing wrt observed profile:",
										prof_idx,
										", ", 
										clust_idx,
										"Finishing at",
										format(Sys.time(),"%H:%M:%S")
							))
			#
			# ⓑ Reduce the marginalized JDF according to the observed evidentiary state.  Merge MARG_JDF.cust_idx with the evidentiary-state
			#      data frame CLUST_EVID_STATE.clust_idx.
				print(paste("Reducing marginal with respect to observed evidentiary state:",
										prof_idx,
										", ", 
										clust_idx,
										"Starting at",
										format(Sys.time(),"%H:%M:%S")
							))
				MARG_JDF.cust_idx <- merge(x = MARG_JDF.cust_idx,
																y = CLUST_EVID_STATE.clust_idx)
			#
			# ⓒ Condition the marginalized JDF on each of the observed evidentiary states.  Conditioning is accomplished by Bayes rule.  
			#      Invert the measure attribute MEAS in MARG_JDF.cust_idx.  Merge the result back onto JDF.clust_idx.  Then multiply the inverted
			#      MEAS of the former by the MEAS of the latter.  
				print(paste("Conditioning unobserved wrt observed:",
										prof_idx,
										", ", 
										clust_idx,
										"Starting at",
										format(Sys.time(),"%H:%M:%S")
							))
				MARG_JDF.cust_idx["invMEAS"] <- 1/MARG_JDF.cust_idx["MEAS"]
				MARG_JDF.cust_idx["MEAS"] <- NULL
				COND_JDF.clust_idx <- merge(x = JDF.clust_idx,
																	y = MARG_JDF.cust_idx)
				COND_JDF.clust_idx["MEAS"] <- apply(X = COND_JDF.clust_idx[c("MEAS","invMEAS")],
																				MARGIN = 1,
																				FUN = prod)
				COND_JDF.clust_idx["invMEAS"] <- NULL
			#
			# ⓓ Marginalize the resulting conditional.
			#      Reshape COND_JDF.clust_idx into an array.  We need a variable EVID_STATE_SIG.clust_idx to fill this out.  The EVID_STATE_SIG
			#       variable is the evidentiary-state signature of all evidentiary states in EVID_PROF_CLUST.clust_idx.
				COND_JDF_ARR.clust_idx <- array(dim = list(length(KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"])^length(TARG_PROF_CLUS.clust_idx),
																						length(TARG_PROF_CLUS.clust_idx)+1,
																						length(EVID_STATE_CLUST_SIG.clust_idx)),
																				dimnames = list(1:(length(KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"])^length(TARG_PROF_CLUS.clust_idx)),
																										c(TARG_PROF_CLUS.clust_idx,"MEAS"),
																										EVID_STATE_CLUST_SIG.clust_idx))
				for (state_idx in rownames(CLUST_EVID_STATE.clust_idx)){		 			## rownames <- rownames(CLUST_EVID_STATE.clust_idx)[1]
					print(paste("Reshaping to array:",
										state_idx,
										", ", 
										which(state_idx == EVID_STATE_CLUST_SIG.clust_idx),
										"of",
										length(EVID_STATE_CLUST_SIG.clust_idx),
										prof_idx,clust_idx,
										format(Sys.time(),"%H:%M:%S")
							))
					CLUST_EVID_STATE.state_idx <- data.frame(rbind(CLUST_EVID_STATE.clust_idx[state_idx,]))
					COND_JDF_ARR.clust_idx[,,state_idx] <- as.matrix(merge(x = CLUST_EVID_STATE.state_idx,
																											y = COND_JDF.clust_idx)[c(TARG_PROF_CLUS.clust_idx,"MEAS")])
				}
		#
		} else {
			COND_JDF_ARR.clust_idx <- array(data = as.matrix(JDF.clust_idx),
																	dim = c(dim(JDF.clust_idx),1),
																	dimnames = list(1:nrow(JDF.clust_idx),
																								colnames(JDF.clust_idx),
																								"UNMEASURED")  )
		}
		# We now must cycle through EVID_STATE_CLUST_SIG.clust_idx and marginalize each associated slice of COND_JDF_ARR.clust_idx
		# with respect its variables in TARG_PROF_CLUS.clust_idx.  Store the result in another array.  We want the dimensions of the array to be
		# TARG_PROF_CLUS.clust_idx × IMPLIED_KNOW_STATE × EVID_STATE_CLUST_SIG.clust_idx. The cells contain the MEAS values. from
		# COND_JDF_ARR.clust_idx.
			LEARNER_KNOW_STATE.clust_idx <- array(dim = list(length(TARG_PROF_CLUS.clust_idx),
																								length(KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"]),
																								length(EVID_STATE_CLUST_SIG.clust_idx)),
																					dimnames = list(TARG_PROF_CLUS.clust_idx,
																												KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"],
																												EVID_STATE_CLUST_SIG.clust_idx))
		#
			for (evid_state_idx in EVID_STATE_CLUST_SIG.clust_idx){								## evid_state_idx <- EVID_STATE_CLUST_SIG.clust_idx[1]
				COND_JDF_ARR.evid_state_idx <- as.data.frame(COND_JDF_ARR.clust_idx[,, evid_state_idx])
				COND_JDF_ARR.evid_state_idx[,"MEAS"] <- as.numeric(COND_JDF_ARR.evid_state_idx[,"MEAS"])
				for (targ_prof_idx in TARG_PROF_CLUS.clust_idx){									## targ_prof_idx <- TARG_PROF_CLUS.clust_idx[1]
					print(paste("Marginalizing with to respect to target variable:",
										targ_prof_idx,
										", ", 
										which(targ_prof_idx == TARG_PROF_CLUS.clust_idx),
										"of",
										length(TARG_PROF_CLUS.clust_idx),
										evid_state_idx,
										"Evidentiary state",
										", ", 
										which(evid_state_idx == EVID_STATE_CLUST_SIG.clust_idx),
										"of",
										length(EVID_STATE_CLUST_SIG.clust_idx),
										prof_idx,clust_idx,
										format(Sys.time(),"%H:%M:%S")
							))
					LEARNER_KNOW_STATE.targ_prof_idx <- aggregate(formula = as.formula(paste("MEAS", targ_prof_idx, sep = " ~ ")),
																											data = COND_JDF_ARR.evid_state_idx,
																											FUN = sum)
					rownames(LEARNER_KNOW_STATE.targ_prof_idx) <- LEARNER_KNOW_STATE.targ_prof_idx[, targ_prof_idx]
					LEARNER_KNOW_STATE.clust_idx[targ_prof_idx,KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"], evid_state_idx] <-
									LEARNER_KNOW_STATE.targ_prof_idx[KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"],"MEAS"]
					#
				}
			}
		#
		# 	LEARNER_KNOW_STATE.clust_idx now contains the knowledge-state estimates for the clust_idxᵗʰ subgraph disconnected cluster of
		#  connected vertices for which evidence is not offered. We want concatenate to each evid_state_idx slice the corresponding evidentiary
		#  state for the corresponding vertices. The evidentiary state assigns a value of 1.0 for the IMPLIED_KNOW_STATE in which the variable was observed.
		#  The previously-built EVID_STATE_CLUST.clust_idx provides our starting point.  Proceed conditionally.  Only proceed if the length of the
		# evidentiary profile EVID_PROF_CLUST.clust_idx is greater than zero.
			if (length(EVID_PROF_CLUST.clust_idx) > 0){
				OBS_KNOW_STATE_ARR.clust_idx <- array(dimnames = list(EVID_PROF_CLUST.clust_idx,
																												KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"],
																												EVID_STATE_CLUST_SIG.clust_idx),
																						dim = list(length(EVID_PROF_CLUST.clust_idx),
																										length(KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"]),
																										length(EVID_STATE_CLUST_SIG.clust_idx)))
			#
			# Now cycle through EVID_STATE_CLUST_SIG.clust_idx, EVID_PROF_CLUST.clust_idx assigning unity values for the corresponding
			# rows of EVID_STATE_CLUST.clust_idx. Assign zero to the remaining values.
				for (evid_state_idx in EVID_STATE_CLUST_SIG.clust_idx){							## evid_state_idx <- EVID_STATE_CLUST_SIG.clust_idx[1]
					for (evid_prof_idx in EVID_PROF_CLUST.clust_idx){								## evid_prof_idx <- EVID_PROF_CLUST.clust_idx[1]
						OBS_KNOW_STATE_ARR.clust_idx[evid_prof_idx, CLUST_EVID_STATE.clust_idx[evid_state_idx, evid_prof_idx], evid_state_idx] <- 1
					} 
				}
				OBS_KNOW_STATE_ARR.clust_idx[which(is.na(OBS_KNOW_STATE_ARR.clust_idx))] <- 0
			#
			# Concatenate OBS_KNOW_STATE_ARR.clust_idx onto LEARNER_KNOW_STATE.clust_idx.  This provides a complete knowledge-state array
			# for the cluster.
				LEARNER_KNOW_STATE.clust_idx <- abind(LEARNER_KNOW_STATE.clust_idx,
																						OBS_KNOW_STATE_ARR.clust_idx,
																						along = 1)
			}
		#
		# Assign LEARNER_KNOW_STATE.clust_idx as the clust_idxᵗʰ element of list LEARNER_KNOW_STATE.prof_idx.
			LEARNER_KNOW_STATE.prof_idx[[clust_idx]] <- LEARNER_KNOW_STATE.clust_idx
		#
		}					## Close for (clust_idx in names(EVID_PROF_CLUST.prof_idx)) — Subnet clusters
		#
		# Assign the list LEARNER_KNOW_STATE.prof_idx of conditional knowledge-state arrays as the prof_idxᵗʰ element of 
		# LEARNER_KNOW_STATE.
			LEARNER_KNOW_STATE[[prof_idx]] <- LEARNER_KNOW_STATE.prof_idx
		#
	}						## Close prof_idx in rownames(EVID_PROF_STATE)) — Observed evidentiary profiles
#
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
#      ⓐ Expand the LEARNER_KNOW_STATE attributes into data frames. 
	KNOW_STATE <- list()
	for (prof_idx in rownames(EVID_PROF_STATE)){														## prof_idx <- rownames(EVID_PROF_STATE)[1]
	# First extract the EVID_STATE and KNOW_STATE attributes from EVID_PROF_STATE.  Also get EoL_WIDE records corresponding to the
	# prof_idxᵗʰ evidentiary profile.
		KNOW_STATE.prof_idx <- EVID_PROF_STATE[["LEARNER_KNOW_STATE"]][[prof_idx]]
		CLUST_EVID_STATE.prof_idx <- EVID_PROF_STATE[["CLUST_EVID_STATE"]][[prof_idx]]
		EoL_WIDE.prof_idx <- EoL_WIDE[EoL_WIDE[,"EVID_PROF_SIG"] == prof_idx,]
	#
	# Prepare EoL_WIDE.prof_idx to subsequently be merged with the wide-format KNOW_STATE table. We only need the
	# evidentiary-state attributes. We also want a STUDENT_ID attribute, derived from the rownames.
		EoL_WIDE.prof_idx <- EoL_WIDE.prof_idx[unlist(SUBMAP_VERTEX)]
		EoL_WIDE.prof_idx["STUDENT_ID"] <-  rownames(EoL_WIDE.prof_idx) 
	#
		KNOW_STATE_LONG.prof_idx <- list()
		for (clust_idx in names(KNOW_STATE.prof_idx)){												## clust_idx <- names(KNOW_STATE.prof_idx)[1]
		# Now extract CLUST_EVID_STATE and KNOW_STATE for the clust_idxᵗʰ cluster. 
			KNOW_STATE.clust_idx <- KNOW_STATE.prof_idx[[clust_idx]]
			CLUST_EVID_STATE.clust_idx <- CLUST_EVID_STATE.prof_idx[[clust_idx]]
			CLUST_EVID_PROF.clust_idx <- colnames(CLUST_EVID_STATE.clust_idx)
		#
		# Reshape the array for the clust_idxᵗʰ subgraph cluster × the prof_idxᵗʰ evidentiary profile into data frame.
			KNOW_STATE_WIDE.clust_idx <- list()
			for (evid_state_idx in dimnames(KNOW_STATE.clust_idx)[[3]]){						## evid_state_idx <- dimnames(KNOW_STATE.clust_idx)[[3]][1]
				KNOW_STATE_WIDE.clust_idx[[evid_state_idx]] <- as.data.frame(KNOW_STATE.clust_idx[,, evid_state_idx])
				KNOW_STATE_WIDE.clust_idx[[evid_state_idx]]["LEARNING_STANDARD_ID"] <- rownames(KNOW_STATE_WIDE.clust_idx[[evid_state_idx]])
				KNOW_STATE_WIDE.clust_idx[[evid_state_idx]]["CLUST_EVID_STATE"] <- evid_state_idx
			#	
			}																								## CLOSE evid_state_idx in dimnames(KNOW_STATE.clust_idx)[[3]]
		# Concatenate the elements of KNOW_STATE_WIDE.clust_idx into a single data frame.  
			KNOW_STATE_WIDE.clust_idx <- do.call(what = rbind,
																				args = KNOW_STATE_WIDE.clust_idx)
			rownames(KNOW_STATE_WIDE.clust_idx) <- NULL
		#
		# Now prepare CLUST_EVID_STATE.clust_idx to merge with KNOW_STATE_WIDE.clust_idx.  Proceed conditionally.  Unmeasured clusters must be 
		#  treated differently.
			if(length(CLUST_EVID_STATE.clust_idx) > 0){
			# Specifically, assign as attribute CLUST_EVID_STATE as the rownames. Then merge with KNOW_STATE_WIDE.clust_idx.
				CLUST_EVID_STATE.clust_idx["CLUST_EVID_STATE"] <- rownames(CLUST_EVID_STATE.clust_idx)
			#
				KNOW_STATE_WIDE.clust_idx <- merge(x = KNOW_STATE_WIDE.clust_idx,
																					y = CLUST_EVID_STATE.clust_idx)
			#
			# Now merge with EoL_WIDE.prof_idx. We expect number of rows in the post-merged KNOW_STATE_WIDE.clust_idx to be
			# length(unique(LEARNING_STANDARD_ID)) × length(unique(STUDENT_ID)), one for each coinciding pair.  Specify merging by
			# the cluster evidentiary profile, CLUST_EVID_PROF.clust_idx.
				KNOW_STATE_WIDE.clust_idx <- merge(x = KNOW_STATE_WIDE.clust_idx[c("LEARNING_STANDARD_ID", CLUST_EVID_PROF.clust_idx,
																																			KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"])],
																					y = EoL_WIDE.prof_idx[c("STUDENT_ID", CLUST_EVID_PROF.clust_idx)],
																					by = CLUST_EVID_PROF.clust_idx)
			#
			# Coerce IMPLIED_KNOW_STATE variables to numeric.
				for (state_idx in KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"]) KNOW_STATE_WIDE.clust_idx[,state_idx] <- 
													as.numeric(KNOW_STATE_WIDE.clust_idx[,state_idx])
			#
			# Melt KNOW_STATE_WIDE.clust_idx into a long table.  
				KNOW_STATE.clust_idx <- melt(data = KNOW_STATE_WIDE.clust_idx[c("STUDENT_ID","LEARNING_STANDARD_ID",
																																	KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"])],
																	id.vars = c("STUDENT_ID","LEARNING_STANDARD_ID"),
																	meas.vars = KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"],
																	variable.name = "IMPLIED_KNOW_STATE",
																	value.name = "MEAS")
			#
			} else {
			# For unmeasured clusters, simply merge KNOW_STATE_WIDE.clust_idx with the STUDENT_IDs in EoL_WIDE.prof_idx.  Then
			# melt.
				KNOW_STATE_WIDE.clust_idx <- merge(x = KNOW_STATE_WIDE.clust_idx,
																					y = EoL_WIDE.prof_idx["STUDENT_ID"])
				for (state_idx in KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"]) KNOW_STATE_WIDE.clust_idx[,state_idx] <- 
													as.numeric(KNOW_STATE_WIDE.clust_idx[,state_idx])
			#
			# Melt KNOW_STATE_WIDE.clust_idx into a long table.  
				KNOW_STATE.clust_idx <- melt(data = KNOW_STATE_WIDE.clust_idx[c("STUDENT_ID","LEARNING_STANDARD_ID",
																																	KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"])],
																	id.vars = c("STUDENT_ID","LEARNING_STANDARD_ID"),
																	meas.vars = KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"],
																	variable.name = "IMPLIED_KNOW_STATE",
																	value.name = "MEAS")
			#
			}       ### CLOSE else
		#	
		# Write KNOW_STATE_WIDE.clust_idx as the clust_idᵗʰ mebmer of list KNOW_STATE_WIDE.prof_idx.
			KNOW_STATE.prof_idx[[clust_idx]] <- KNOW_STATE.clust_idx
		#
		}									## CLOSE clust_idx in names(KNOW_STATE.prof_idx))
	# Concatenate the elements of KNOW_STATE.prof_idx into a single data frame.  Write KNOW_STATE.prof_idx as the prof_idxᵗʰ element
	# of data frame 
		KNOW_STATE.prof_idx <- do.call(what = rbind,
																args = KNOW_STATE.prof_idx)
		rownames(KNOW_STATE.prof_idx) <- NULL
		KNOW_STATE[[prof_idx]] <- KNOW_STATE.prof_idx
	#
	}								## CLOSE prof_idx in rownames(EVID_PROF_STATE))
# 
#       Concatenate the elements of KNOW_STATE into a single data frame.
	KNOW_STATE <- do.call(args = KNOW_STATE,
											what = rbind)
	rownames(KNOW_STATE) <- NULL
#       
#       Clean up the LEARNING_STANDARD_ID attribute. Truncate off the leading character "X".
	KNOW_STATE[,"LEARNING_STANDARD_ID"] <- gsub(x = KNOW_STATE[,"LEARNING_STANDARD_ID"],
																												pattern = "X",
																												replacement = "")
#
#          Now apply two date-stamp attributes to KNOW_STATE.  EVID_STATE_AS_OF is the date of the most-recent evidentiary state
#          measurement. KNOW_STATE_AS_OF is the date of the estimate, conditioned on the evidentiary state in the former.  We get
#          EVID_STATE_AS_OF by max-aggregation of EoL_MEAS and then merging the result by STUDENT_ID with KNOW_STATE.
#          KNOW_STATE_AS_OF is simply the system date of the calculation. 🐞🐜🕷🐝🕷🐞🐜🕷🐝🕷🐞🐜🕷🐝🕷🐞🐜🕷🐝🕷🐞🐜🕷🐝🕷
	EoL_MEAS[,"DATE_OF_MEAS"] <- as.Date(x = EoL_MEAS[,"DATE_OF_MEAS"], "%Y-%m-%d")
	EVID_STATE_AS_OF <- aggregate(formula = DATE_OF_MEAS ~ STUDENT_ID + LEARNING_STANDARD_ID,
															data = EoL_MEAS,
															FUN = max)
	colnames(EVID_STATE_AS_OF) <- c("STUDENT_ID","LEARNING_STANDARD_ID","EVID_STATE_AS_OF")
	EVID_STATE_AS_OF[,"LEARNING_STANDARD_ID"] <- gsub(x = EVID_STATE_AS_OF[,"LEARNING_STANDARD_ID"],
																										pattern = "X",
																										replacement = "")
	KNOW_STATE <- merge(x = KNOW_STATE,
											y = EVID_STATE_AS_OF,
											all.x = TRUE)
	KNOW_STATE["KNOW_STATE_AS_OF"] <- Sys.Date()
	KNOW_STATE <- merge(x = KNOW_STATE,
											y = LEARNING_STANDARD)
#
#          Finally, reorder the records according to STUDENT_ID, LEARNING_STANDARD_ID, IMPLIED_KNOW_STATE.  Then
#         coerce attributs to UTF-8 character and write out as a csv table.  
	KNOW_STATE[,"IMPLIED_KNOW_STATE"] <- factor(x = KNOW_STATE[,"IMPLIED_KNOW_STATE"],
																						levels = KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"])
	KNOW_STATE <- KNOW_STATE[order(KNOW_STATE[,"STUDENT_ID"],
																	KNOW_STATE[,"LEARNING_STANDARD_ID"]),
															c("STUDENT_ID","LEARNING_STANDARD_ID","LEARNING_STANDARD_CD",
																"IMPLIED_KNOW_STATE","MEAS","EVID_STATE_AS_OF",
																"KNOW_STATE_AS_OF")]
	for (col_idx in c("EVID_STATE_AS_OF","KNOW_STATE_AS_OF","IMPLIED_KNOW_STATE")) KNOW_STATE[,col_idx] <- as.character(KNOW_STATE[,col_idx])
	for (col_idx in colnames(KNOW_STATE)) {
		KNOW_STATE[,col_idx] <- enc2utf8(as.character(KNOW_STATE[,col_idx]))
		KNOW_STATE[is.na(KNOW_STATE[,col_idx]),col_idx] <- ""
	}
#
	write.csv(x = KNOW_STATE,
					file = paste(PROF_TASK.dir, "LEARNER_KNOW_STATE_BRUTE_FORCE.csv", sep = "/"),
					row.names = FALSE,
					eol = "\r\n",
					fileEncoding = "UTF-8",
					quote = TRUE)
#