## PURPOSE: CALCULATE CONDITIONAL PROBABILITY-DISTRIBUTION (CPT) TABLES FOR USE IN A BAYESIAN NETWORK.
## Heuristic CPTs here are based on the multivariate-normal (MVN) distributions of the 𝕃²-Norm of the distance beteween 
## a discrete conditioned variable and its discrete conditioning variables. All variables share the same discrete state-space. This state
## space is defined by according to the KNOW_STATE_SPEC, a table specifying the knowledge states for a proficiency model
## according to the evidence-centered design (ECD) framework.  Additionally, the CPTs are uniform throughout the Bayesian
## network.  That is, any two vertices having the same number of immediate parents also have the same CPTs.
##
## MAJOR STEPS:
## 1︎⃣ DATA INGESTION.  Read in USE_CASE_QUERY_ATTRIBUTES and and KNOW_STATE_SPEC.
## 2︎⃣ DERIVE MVN-DISTRIBUTION PARAMETERS.  
## 3︎⃣ CALCULATE CPTs.  The CPTs are built out incrementally as a list of data frames.  Each increment adds another
##      conditioning variable.
## 4︎⃣ RESHAPE AND CONCATENATE THE CPT TABLES. We need for all of the CPTs to be contained in a single long-format
##      table.
## 5︎⃣ WRITE THE CPTs OUT TO A CSV FILE. 
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
	MAX_PAR_VERTEX <- as.numeric(USE_CASE_ATTRIBUTES["MAX_PAR_VERTEX","VALUE"])
#
# 2︎⃣ DERIVE MVN-DISTRIBUTION PARAMETERS.  
#     The the discrete-variable states are the unique occurrences of IMPLIED_KNOW_STATE in KNOW_STATE_SPEC.
	know_state <- length(unique(KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"]))
	CPT_SDEV <- as.numeric(USE_CASE_ATTRIBUTES["CPT_SDEV","VALUE"])
#
# 3︎⃣ CALCULATE CPTs.
#      Iteratively build up CPT tables for each conditioning-variable scenario.  Initialize our variables as the IMPLIED_KNOW_STATE
#      variable in KNOW_STATE_SPEC.
	CPT <- data.frame(Resp_Attr = 1:length(KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"]))
	Cond_Attr <- data.frame(Cond_Attr = 1:length(KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"]))
	Heur_CPT <- list()
#
#      Built out the CPT tables incrementally. 
	for (cond_var_idx in 1: MAX_PAR_VERTEX){															## cond_var_idx in (1: MAX_PAR_VERTEX)[1]
	# Define the parameter-state matrix for the cond_var_idxᵗʰ conditioning parameter. Accomplish this by "merging"
	# the the conditioning-attribute matrix Cond_Attr with CPT.  Rename the colum for Cond_Attr by appending
	# the value of cond_var_idx onto it.   #1:7){											## cond_var_idx <- 2
		CPT = merge(Cond_Attr, CPT)		 ## ‼️✋‼️✋‼️✋‼️✋‼️✋‼️✋‼️✋‼️✋‼️✋‼️✋‼️✋‼️✋
		Cond_Attrb.cond_var_idx = paste("Cond_Attr", cond_var_idx,sep = "_")
		colnames(CPT)[colnames(CPT) == "Cond_Attr"] = Cond_Attrb.cond_var_idx
	#
	# Calculate the pairwise measures — 𝕃²-Norms of Euclidian distance — between cond_var_idxᵗʰ Cond_Attr instance and the
	# Resp_Attr. Then calculate the 𝕃⁴-Norm.
		CPT[,paste('L_Dist',cond_var_idx, sep = "_")] = (CPT[, Cond_Attrb.cond_var_idx] - CPT[, 'Resp_Attr'])^2
		CPT[,'L_Norm'] = apply(X = CPT[colnames(CPT)[grep(x = colnames(CPT), pattern = 'L_Dist')]],
												MARGIN = 1,
												FUN = sum)^(1/2)
	# Reshape CPT into wide-table format.  We want the conditioning-attribute states Cond_Attr's as the rows and
	# the Conditioned-Attribute Resp_Attr states in the columns. The cells contain Measure.
	# Rename the resulting columns for convenience.  Also, reverse the order in which the conditioning-attribute
	# appear in the data frame so that the end result cnoforms to the HUGIN specification and that the measures
	# populate into the SAMIAM tool in the right order.
		Cond_Attr_Cols.cond_var_idx <- colnames(CPT)[grep(x = colnames(CPT),
																								pattern = "Cond_Attr")]
		WIDE_FORM.cond_var_idx = as.formula(paste(paste((Cond_Attr_Cols.cond_var_idx),
																								collapse = " + "),
																			"Resp_Attr",
																			sep = " ~ " ))
		CPT_WIDE.cond_var_idx = dcast(data = CPT,
																formula = WIDE_FORM.cond_var_idx,
																value.var = "L_Norm")
		colnames(CPT_WIDE.cond_var_idx) <- c(Cond_Attr_Cols.cond_var_idx,
																			paste("Resp_Norm",
																					setdiff(colnames(CPT_WIDE.cond_var_idx),
																								Cond_Attr_Cols.cond_var_idx),
																					sep = "_"))
		Resp_Attr_Cols.cond_var_idx <- setdiff(colnames(CPT_WIDE.cond_var_idx),
																		Cond_Attr_Cols.cond_var_idx)
	#	CPT_WIDE.cond_var_idx <- CPT_WIDE.cond_var_idx[,c(rev(Cond_Attr_Cols.cond_var_idx), Resp_Attr_Cols.cond_var_idx)]
	#
	# We now convert our L_Norm into a measure. This measure meets the properties of a CPT. It consequently satisfies following properties:
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
			CPT_WIDE.cond_var_idx[, Meas_Col.cond_var_idx.col.idx] <- pnorm(q = CPT_WIDE.cond_var_idx[, col.idx],
																														mean = 0,
																														sd = CPT_SDEV,
																														lower.tail = FALSE)
		}
		CPT_WIDE.cond_var_idx[Meas_Col.cond_var_idx] <- CPT_WIDE.cond_var_idx[Meas_Col.cond_var_idx]/
						rowSums(CPT_WIDE.cond_var_idx[Meas_Col.cond_var_idx])
	#
	# Finally, "filter" CPT_WIDE.cond_var_idx, retaining only the columns pertaining to "Cond_Attr" and "Meas".
		CPT_WIDE.cond_var_idx <- CPT_WIDE.cond_var_idx[union(rev(grep(x = colnames(CPT_WIDE.cond_var_idx),
																													pattern = "Cond_Attr")),
																											grep(x = colnames(CPT_WIDE.cond_var_idx),
																													pattern = "Meas"))]
	#
	# Add CPT artifacts to Heur_CPT.
		Heur_CPT[[paste("CPT", cond_var_idx,sep = "_")]] <- CPT_WIDE.cond_var_idx
	}
#
# 4︎⃣ RESHAPE AND CONCATENATE THE CPT TABLES. 
#      Reshape each CPT into a long-table format.  First declare Heur_CPT_LONG, a list into which the individual long-table-format
#      CPTs are stored. Then, Specify a "CPT_0" probability distribution for "orphan" edge vertices.  Specify it arbitrarily as the second row of 
#      HEUR_CPT[["CPT_1"]].
	Heur_CPT_LONG <- list()
	Heur_CPT_LONG[["CPT_0"]] <- data.frame(MEAS = unlist(Heur_CPT[["CPT_1"]][2,-1]))
	Heur_CPT_LONG[["CPT_0"]]["VERT_CPT_BRIDGE_IDX"] <- 0
# Assign a CPT_CELL_IDX index.  The CPT_CELL_IDX is a character-coerced integer with leading zeros.  We want for the string length of
# CPT_CELL_IDX to be constant for all  CPTs.  We also want to minimize the length so as to support at most the longest CPT table.
	Heur_CPT_LONG[["CPT_0"]]["CPT_CELL_IDX"] <- paste(paste(rep(x = "0",
																									times = nchar(length(unique(
																													KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"]))^ (MAX_PAR_VERTEX + 1))),
																								collapse = ""),
																								1:nrow(Heur_CPT_LONG[["CPT_0"]]),
																								sep = "")
	Heur_CPT_LONG[["CPT_0"]]["CPT_CELL_IDX"] <- str_sub(string = Heur_CPT_LONG[["CPT_0"]][,"CPT_CELL_IDX"],
																								end = -1,
																								start = -nchar(length(unique(
																													KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"]))^ (MAX_PAR_VERTEX + 1)))
#
#
#      Now loop through the data frames in list Heur_CPT, reshaping each into a long-table data frame. Store the resulting data frames in
#      Heur_CPT_LONG.
	for (CPT_idx in names(Heur_CPT)){													## CPT_idx <- names(Heur_CPT)[1]
	# First extract the wide-table CPT for the CPT_idxᵗʰ vertex configuration.
		Heur_CPT.CPT_idx <- Heur_CPT[[CPT_idx]]
	#
	# Use the melt function to reshape into a long-table format.
		Heur_CPT_LONG.CPT_idx <- melt(data = Heur_CPT.CPT_idx,
																meas.vars = colnames(Heur_CPT.CPT_idx)[grep(x = colnames(Heur_CPT.CPT_idx),
																																				pattern = "Meas")],
																id.vars = colnames(Heur_CPT.CPT_idx)[grep(x = colnames(Heur_CPT.CPT_idx),
																																				pattern = "Cond_Attr")],
																variable.name = "Resp_Var",
																value.name = "MEAS")
	#
	# We know the sequencing of the variables by specification. We just need to retain the CPT_VALUE attribute.  We add attributes to specify
	# the curricular area and parent-vertex configuration with which the CPT is associated.
		Heur_CPT_LONG.CPT_idx <- Heur_CPT_LONG.CPT_idx["MEAS"]
		Heur_CPT_LONG.CPT_idx["VERT_CPT_BRIDGE_IDX"] <- grep(x = names(Heur_CPT),
																											pattern = CPT_idx)
	#
	# Assign a CPT_CELL_IDX index.  The CPT_CELL_IDX is a character-coerced integer with leading zeros.  We want for the string length of
	# CPT_CELL_IDX to be constant for all  CPTs.  We also want to minimize the length so as to support at most the longest CPT table.
		Heur_CPT_LONG.CPT_idx["CPT_CELL_IDX"] <- paste(paste(rep(x = "0",
																										times = nchar(length(unique(
																														KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"]))^ (MAX_PAR_VERTEX + 1))),
																									collapse = ""),
																									1:nrow(Heur_CPT_LONG.CPT_idx),
																									sep = "")
		Heur_CPT_LONG.CPT_idx["CPT_CELL_IDX"] <- str_sub(string = Heur_CPT_LONG.CPT_idx[,"CPT_CELL_IDX"],
																									end = -1,
																									start = -nchar(length(unique(
																														KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"]))^ (MAX_PAR_VERTEX + 1)))
	#
	# Write the long table as the CPT_idxᵗʰ data-frame element of list Heur_CPT_LONG.
		Heur_CPT_LONG[[CPT_idx]] <- Heur_CPT_LONG.CPT_idx
	#
	}
#
#      Concatenate the members of Heur_CPT_LONG into a single data frame.
	Heur_CPT_LONG <- do.call(what = rbind,
													args = Heur_CPT_LONG)
	rownames(Heur_CPT_LONG) <- NULL
#
#      Add attributes to Heur_CPT_LONG associating it with a course and school. These are contained in USE_CASE_ATTRIBUTES.
#	Heur_CPT_LONG["SUBJECT_TITLE"] <- USE_CASE_ATTRIBUTES["SUBJECT_TITLE","VALUE"]
	Heur_CPT_LONG["SCHOOL_DISTRICT"] <- USE_CASE_ATTRIBUTES["SCHOOL_DISTRICT","VALUE"]
	Heur_CPT_LONG["APPROACH"] <- "HEURISTIC_MVN"
	Heur_CPT_LONG["CPT_SDEV"] <- USE_CASE_ATTRIBUTES["CPT_SDEV","VALUE"]
	Heur_CPT_LONG["VERT_CPT_BRIDGE_IDX"] <- paste("HERUISTIC_MVN",
																							Heur_CPT_LONG[,"VERT_CPT_BRIDGE_IDX"],
																							sep = "_")
	Heur_CPT_LONG["VERT_CPT_BRIDGE_IDX"] <- apply(X = Heur_CPT_LONG[c("VERT_CPT_BRIDGE_IDX","CPT_SDEV")],
																							MARGIN = 1,
																							FUN = paste,
																							collapse = "_SDEV_")
	Heur_CPT_LONG <- Heur_CPT_LONG[c("SCHOOL_DISTRICT","VERT_CPT_BRIDGE_IDX","CPT_CELL_IDX","MEAS",
																		"APPROACH","CPT_SDEV")]
#
#      Coerce the columns to UTF-8 character and write the result to a csv file.
	for (col_idx in colnames(Heur_CPT_LONG)) Heur_CPT_LONG[,col_idx] <- enc2utf8(as.character(Heur_CPT_LONG[,col_idx]))
#
	write.csv(x = Heur_CPT_LONG,
					file = paste(PROF_TASK.dir, "CPT_LONG.csv", sep = "/"),
					row.names = FALSE,
					eol = "\r\n",
					fileEncoding = "UTF-8",
					quote = TRUE)
#