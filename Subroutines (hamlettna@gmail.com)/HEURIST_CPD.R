## PURPOSE: CALCULATE CONDITIONAL PROBABILITY-DISTRIBUTION (CPD) TABLES FOR USE IN A BAYESIAN NETWORK.
## Heuristic CPDs here are based on the multivariate-normal (MVN) distributions of the 𝕃²-Norm of the distance beteween 
## a discrete conditioned variable and its discrete conditioning variables. All variables share the same discrete state-space. This state
## space is defined by according to the KNOW_STATE_SPEC, a table specifying the knowledge states for a proficiency model
## according to the evidence-centered design (ECD) framework.  Additionally, the CPDs are uniform throughout the Bayesian
## network.  That is, any two vertices having the same number of immediate parents also have the same CPDs.
##
## MAJOR STEPS:
## 1︎⃣ DATA INGESTION.  Read in USE_CASE_QUERY_ATTRIBUTES and and KNOW_STATE_SPEC.
## 2︎⃣ DERIVE MVN-DISTRIBUTION PARAMETERS.  
## 3︎⃣ CALCULATE CPDs.  The CPDs are built out incrementally as a list of data frames.  Each increment adds another
##      conditioning variable.
## 4︎⃣ RESHAPE AND CONCATENATE THE CPD TABLES. We need for all of the CPDs to be contained in a single long-format
##      table.
## 5︎⃣ WRITE THE CPDs OUT TO A CSV FILE. 
#
# Initialize environment.
	options(stringsAsFactors = FALSE)
	options(java.parameters = "-Xmx16g")
	library(stringr)
	library(reshape2)
#
# 1︎⃣ DATA INGESTION.  
#      Load in the required tables, LEARNING_STANDARD and UNIT_MAP_EDGE_LIST. LEARNING_STANDARD is needed to
#      associate the LEARNING_STANDARD_IDs from UNIT_MAP_EDGE_LIST.
	proto.dir <- "/Users/nahamlet/Box Sync/IBM-Watson ED K12/Pathway-Centric CONOPS/Learning-Map Prototype"
	USE_CASE_ATTRIBUTES <- read.csv(file = paste(proto.dir, "USE_CASE_QUERY_ATTRIBUTES.csv", sep = "/"),
																colClasses = "character")
	rownames(USE_CASE_ATTRIBUTES) <- USE_CASE_ATTRIBUTES[,"QUERY_ATTRIBUTE"]
	Case.dir <- USE_CASE_ATTRIBUTES["Case.dir","VALUE"]
	setwd(Case.dir)
	PROF_TASK.dir <- paste(Case.dir, "PROF_TASK_MODEL", sep = "/")
	KNOW_STATE_SPEC <- read.csv(file = paste(PROF_TASK.dir, "KNOW_STATE_SPEC.csv", sep = "/"),
															colClasses = "character")
#
# 2︎⃣ DERIVE MVN-DISTRIBUTION PARAMETERS.  
#     The the discrete-variable states are the unique occurrences of IMPLIED_KNOW_STATE in KNOW_STATE_SPEC.
	know_state <- length(unique(KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"]))
	CPD_SDEV <- as.numeric(USE_CASE_ATTRIBUTES["CPD_SDEV","VALUE"])
#
# 3︎⃣ CALCULATE CPDs.
#      Iteratively build up CPD tables for each conditioning-variable scenario.  Initialize our variables as the IMPLIED_KNOW_STATE
#      variable in KNOW_STATE_SPEC.
	CPD <- data.frame(Resp_Attr = 1:length(KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"]))
	Cond_Attr <- data.frame(Cond_Attr = 1:length(KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"]))
	Heur_CPD <- list()
#
#      Built out the CPD tables incrementally. 
	for (cond_var_idx in 1:8){											## cond_var_idx <- 1
	# Define the parameter-state matrix for the cond_var_idxᵗʰ conditioning parameter. Accomplish this by "merging"
	# the the conditioning-attribute matrix Cond_Attr with CPD.  Rename the colum for Cond_Attr by appending
	# the value of cond_var_idx onto it.   #1:7){											## cond_var_idx <- 2
		CPD = merge(Cond_Attr, CPD)		 ## ‼️✋‼️✋‼️✋‼️✋‼️✋‼️✋‼️✋‼️✋‼️✋‼️✋‼️✋‼️✋
		Cond_Attrb.cond_var_idx = paste("Cond_Attr", cond_var_idx,sep = "_")
		colnames(CPD)[colnames(CPD) == "Cond_Attr"] = Cond_Attrb.cond_var_idx
	#
	# Calculate the pairwise measures — 𝕃²-Norms of Euclidian distance — between cond_var_idxᵗʰ Cond_Attr instance and the
	# Resp_Attr. Then calculate the 𝕃⁴-Norm.
		CPD[,paste('L_Dist',cond_var_idx, sep = "_")] = (CPD[, Cond_Attrb.cond_var_idx] - CPD[, 'Resp_Attr'])^2
		CPD[,'L_Norm'] = apply(X = CPD[colnames(CPD)[grep(x = colnames(CPD), pattern = 'L_Dist')]],
												MARGIN = 1,
												FUN = sum)^(1/2)
	# Reshape CPD into wide-table format.  We want the conditioning-attribute states Cond_Attr's as the rows and
	# the Conditioned-Attribute Resp_Attr states in the columns. The cells contain Measure.
	# Rename the resulting columns for convenience.  Also, reverse the order in which the conditioning-attribute
	# appear in the data frame so that the end result cnoforms to the HUGIN specification and that the measures
	# populate into the SAMIAM tool in the right order.
		Cond_Attr_Cols.cond_var_idx <- colnames(CPD)[grep(x = colnames(CPD),
																								pattern = "Cond_Attr")]
		WIDE_FORM.cond_var_idx = as.formula(paste(paste((Cond_Attr_Cols.cond_var_idx),
																								collapse = " + "),
																			"Resp_Attr",
																			sep = " ~ " ))
		CPD_WIDE.cond_var_idx = dcast(data = CPD,
																formula = WIDE_FORM.cond_var_idx,
																value.var = "L_Norm")
		colnames(CPD_WIDE.cond_var_idx) <- c(Cond_Attr_Cols.cond_var_idx,
																			paste("Resp_Norm",
																					setdiff(colnames(CPD_WIDE.cond_var_idx),
																								Cond_Attr_Cols.cond_var_idx),
																					sep = "_"))
		Resp_Attr_Cols.cond_var_idx <- setdiff(colnames(CPD_WIDE.cond_var_idx),
																		Cond_Attr_Cols.cond_var_idx)
	#	CPD_WIDE.cond_var_idx <- CPD_WIDE.cond_var_idx[,c(rev(Cond_Attr_Cols.cond_var_idx), Resp_Attr_Cols.cond_var_idx)]
	#
	# We now convert our L_Norm into a measure. This measure meets the properties of a CPD. It consequently satisfies following properties:
	# ① Rows sum to unity, conditioned on ∑ᵢP(Respᵢ | {Cond_Attr_j}) = 1; and
	# ② Monotonically decreases with decreasing L_Norm.
	# Apply dnorm, the density function corresponding to a Gaussian-Normal distribution. Then normalize with respect
	# to rowSums.
		Meas_Col.cond_var_idx <- paste("Meas",
															str_sub(string = Resp_Attr_Cols.cond_var_idx ,
																		start = unlist(lapply(X = str_locate_all(string = Resp_Attr_Cols.cond_var_idx, 
																															pattern = "_"),
																										FUN ="[",2,2)),
																		end = -1),
																sep = "")
		for (col.idx in Resp_Attr_Cols.cond_var_idx) {							## col.idx <- Resp_Attr_Cols.cond_var_idx[1]
			Meas_Col.cond_var_idx.col.idx <- Meas_Col.cond_var_idx[grep(pattern = col.idx,
																													x = Resp_Attr_Cols.cond_var_idx)]
			CPD_WIDE.cond_var_idx[, Meas_Col.cond_var_idx.col.idx] <- pnorm(q = CPD_WIDE.cond_var_idx[, col.idx],
																														mean = 0,
																														sd = CPD_SDEV,
																														lower.tail = FALSE)
		}
		CPD_WIDE.cond_var_idx[Meas_Col.cond_var_idx] <- CPD_WIDE.cond_var_idx[Meas_Col.cond_var_idx]/
						rowSums(CPD_WIDE.cond_var_idx[Meas_Col.cond_var_idx])
	#
	# Finally, "filter" CPD_WIDE.cond_var_idx, retaining only the columns pertaining to "Cond_Attr" and "Meas".
		CPD_WIDE.cond_var_idx <- CPD_WIDE.cond_var_idx[union(rev(grep(x = colnames(CPD_WIDE.cond_var_idx),
																													pattern = "Cond_Attr")),
																											grep(x = colnames(CPD_WIDE.cond_var_idx),
																													pattern = "Meas"))]
	#
	# Add CPD artifacts to Heur_CPD.
		Heur_CPD[[paste("CPD", cond_var_idx,sep = "_")]] <- CPD_WIDE.cond_var_idx
	}
#
# 4︎⃣ RESHAPE AND CONCATENATE THE CPD TABLES. 
#      Reshape each CPD into a long-table format.  First declare Heur_CPD_LONG, a list into which the individual long-table-format
#      CPDs are stored. Then, Specify a "CPD_0" probability distribution for "orphan" edge vertices.  Specify it arbitrarily as the second row of 
#      HEUR_CPD[["CPD_1"]].
	Heur_CPD_LONG <- list()
	Heur_CPD_LONG[["CPD_0"]] <- data.frame(COND_PROB = unlist(Heur_CPD[["CPD_1"]][2,-1]))
	Heur_CPD_LONG[["CPD_0"]]["TARG_VERT_CFG"] <- 0
#
#      Now loop through the data frames in list Heur_CPD, reshaping each into a long-table data frame. Store the resulting data frames in
#      Heur_CPD_LONG.
	for (cpd_idx in names(Heur_CPD)){													## cpd_idx <- names(Heur_CPD)[1]
	# First extract the wide-table CPD for the cpd_idxᵗʰ vertex configuration.
		Heur_CPD.cpd_idx <- Heur_CPD[[cpd_idx]]
	#
	# Use the melt function to reshape into a long-table format.
		Heur_CPD_LONG.cpd_idx <- melt(data = Heur_CPD.cpd_idx,
																meas.vars = colnames(Heur_CPD.cpd_idx)[grep(x = colnames(Heur_CPD.cpd_idx),
																																				pattern = "Meas")],
																id.vars = colnames(Heur_CPD.cpd_idx)[grep(x = colnames(Heur_CPD.cpd_idx),
																																				pattern = "Cond_Attr")],
																variable.name = "Resp_Var",
																value.name = "COND_PROB")
	#
	# We know the sequencing of the variables by specification. We just need to retain the CPD_VALUE attribute.  We add attributes to specify
	# the curricular area and parent-vertex configuration with which the CPD is associated.
		Heur_CPD_LONG.cpd_idx <- Heur_CPD_LONG.cpd_idx["COND_PROB"]
		Heur_CPD_LONG.cpd_idx["TARG_VERT_CFG"] <- grep(x = names(Heur_CPD),
																								pattern = cpd_idx)
	#
	# Write the long table as the cpd_idxᵗʰ data-frame element of list Heur_CPD_LONG.
		Heur_CPD_LONG[[cpd_idx]] <- Heur_CPD_LONG.cpd_idx
	#
	}
#
#      Concatenate the members of Heur_CPD_LONG into a single data frame.
	Heur_CPD_LONG <- do.call(what = rbind,
													args = Heur_CPD_LONG)
	rownames(Heur_CPD_LONG) <- NULL
#
#      Add attributes to Heur_CPD_LONG associating it with a course and school. These are contained in USE_CASE_ATTRIBUTES.
	Heur_CPD_LONG["SUBJECT_TITLE"] <- USE_CASE_ATTRIBUTES["SUBJECT_TITLE","VALUE"]
	Heur_CPD_LONG["SCHOOL_DISTRICT"] <- USE_CASE_ATTRIBUTES["SCHOOL_DISTRICT","VALUE"]
	Heur_CPD_LONG["APPROACH"] <- "HEURISTIC_MVN"
	Heur_CPD_LONG["CPD_SDEV"] <- USE_CASE_ATTRIBUTES["CPD_SDEV","VALUE"]
#
#      Coerce the columns to UTF-8 character and write the result to a csv file.
	for (col_idx in colnames(Heur_CPD_LONG)) Heur_CPD_LONG[,col_idx] <- enc2utf8(as.character(Heur_CPD_LONG[,col_idx]))
#
	write.csv(x = Heur_CPD_LONG,
					file = paste(PROF_TASK.dir, "CPD_LONG.csv", sep = "/"),
					row.names = FALSE,
					eol = "\r\n",
					fileEncoding = "UTF-8",
					quote = TRUE)
#