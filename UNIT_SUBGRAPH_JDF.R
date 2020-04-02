## PURPOSE: CALCULATE CONDITIONAL PROBABILITY DISTRIBUTION FOR A BAYESIAN NETWORK.
## The JDF results from the product of CPD factors as described in (3.17), p. 62 of Koller, Probablistic Graphical Models.
## We use here a given set of conditioned-variable-invarient CPD factors.  We have a factor for each vertex configuration
## distinguished in terms of the number of conditioning variables. The vertex configurations are obtained from the graph itself. This
## CPD stops short of a Joint Distribution Function (JDF) in that it that it has not accounted for the prior probabilities of
## the "root" vertices.  "Root" vertices are edge vertices for which no parents are specified.
##
## MAJOR STEPS:
## 1︎⃣ DATA INGESTION.  Read in the CPD factors and an edge list specifying the graph. Our CPD factors are stored in a long-table
##      format.
## 2︎⃣ RESHAPE THE CPD FACTORS INTO A WIDE-TABLE FORMAT. We want one column of variable states for each variable
##      plus a "measure" column containing the probability for the state configuration.  We need one such wide-table CPD
##      representation for each network-vertex configuration.
## 3︎⃣ ASCERTAIN THE VERTEX CONFIGURATION FOR EACH VERTEX IN THE NETWORK. This results from the network edge list.
##      Each row in the edge list corresponds to a directed edge, specified by the source vertex in the first column and the destination
##      vertex in the second. We therefore count the number of occurrences of each variable in the edge-destination column of the
##      edge list.
## 4︎⃣ ASSOCIATE A CPD FACTOR WITH EACH VERTEX.  Construct a data frame containing a row for each vertex in the edge list.
##      The second column contains the counts of each vertex' occurrences in the vertex-destination column of the edge list.  This
##      occurrence-counts attribute associates each vertex with a CPD.
## 5︎⃣ EXTRACT FOR EACH CPD THE CORRESPONDING CPD FROM THE LIST OF CPD "WIDE" TABLES AND RENAME ITS VARIABLES.
##      We need for the CPD-table attributes to be labeled accoring to its associated vertex and its parents.
## 6︎⃣ MERGE (INNER-JOIN) ALL OF THE CPDs  TO CONTRUCT THE JDF.  The JDF probability measure is the product of all of the
##      CPD measure columns following the merging.
## 7︎⃣ WRITE THE UNIT-SUBGRAPH CPD OUT AS AN RData OBJECT. 
#
# Initialize environment.
	options(stringsAsFactors = FALSE)
	options(java.parameters = "-Xmx16g")
	library(igraph)
	library(data.table)
#
# 1︎⃣ DATA INGESTION.  
#      Load in the required tables. Start with the USE_CASE_ATTRIBUTES table. From that, extract parameters specifying the use-case attributes,
#      including locations from which to extract files. Then load in the edge list UNIT_MAP_EDGE_LIST and the table of heuristically derived
#      CPDs CPD_LONG. We also need KNOW_STATE_SPEC, which gives us the variable states for each variable in our network.
	proto.dir <- "/Users/nahamlet/Box Sync/IBM-Watson ED K12/Pathway-Centric CONOPS/Learning-Map Prototype"
	USE_CASE_ATTRIBUTES <- read.csv(file = paste(proto.dir, "USE_CASE_QUERY_ATTRIBUTES.csv", sep = "/"),
																colClasses = "character")
	rownames(USE_CASE_ATTRIBUTES) <- USE_CASE_ATTRIBUTES[,"QUERY_ATTRIBUTE"]
	Case.dir <- USE_CASE_ATTRIBUTES["Case.dir","VALUE"]
	setwd(Case.dir)
	PROF_TASK.dir <- paste(Case.dir, "PROF_TASK_MODEL", sep = "/")
	UNIT_MAP_EDGE_LIST <- read.csv(file = paste(PROF_TASK.dir, "UNIT_MAP_EDGE_LIST.csv", sep = "/"),
																colClasses = "character") 
	KNOW_STATE_SPEC <- read.csv(file = paste(PROF_TASK.dir, "KNOW_STATE_SPEC.csv", sep = "/"),
														colClasses = "character")
	CPD_LONG <- read.csv(file = paste(PROF_TASK.dir, "CPD_LONG.csv", sep = "/"),
											colClasses = "character")
	COURSE_ENROLL <- read.csv(file = paste(PROF_TASK.dir, "COURSE_ENROLL.csv", sep = "/"),
													colClasses = "character")
	EoL_MEAS <- read.csv(file = paste(PROF_TASK.dir, "EoL_MEAS.csv", sep = "/"),
										colClasses = "character")
#      Coerce the COND_PROB attribute of CPD_LONG to numeric.
	CPD_LONG[,"COND_PROB"] <- as.numeric(CPD_LONG[,"COND_PROB"])
#
#      Clean prior UNIT_SUBMAP_JDF.csv files from the working directory.
	old_jdf_files <- list.files(PROF_TASK.dir)[grep(x = list.files(PROF_TASK.dir),
																			pattern = "UNIT_MAP_JDF.csv")] 
	if (length(old_jdf_files) > 0) for (file_idx in old_jdf_files) file.remove(paste(PROF_TASK.dir, file_idx, sep = "/"))
#
# Ⓐ CALCULATE PRIOR DISTRIBUTIONS FOR ROOT-EDGE VERTICES. The CPDs for the root vertices are priors, not conditionals.
#      We derive them from actual in-scope EoL history for the subject group.  We establish the scope by "filtering" EoL_Meas to include
#       only subjects (students) in the section of interest.  Identify the section from the COURSE, SECTION attributes of USE_CASE_ATTRIBUTES.
#       Use this to filter the COURSE_ENROLL table. Then select by STUDENT_ID subjects enrolled in the CLASS_ID.  Calculate a histogram of all
#       EoL_MEAS for this group using the break points specified by KNOW_STATE_SPEC. Normalize this histogram to provide the prior probabilities.
#       Overwrite these priors with the default priors in CPD_LONG for TARGERT_CFG = "0".
#
#       First "filter" COURSE_ENROLL to get a list of subjects enrolled in the section of interest.
	SECTION_ENROLL <- data.frame(rbind(USE_CASE_ATTRIBUTES[c("COURSE_ID","CLASS_ID"),"VALUE"]))
	colnames(SECTION_ENROLL) <- c("COURSE_ID","CLASS_ID")
	SECTION_ENROLL <- merge(x = SECTION_ENROLL,
													y = COURSE_ENROLL,
													by = c("COURSE_ID","CLASS_ID"))
#
#      Apply a similar procedure to reduce EoL_MEAS to just subjects with STUDENT_IDs in SECTION_ENROLL.
	EoL_MEAS_SECTION <- merge(x = SECTION_ENROLL["STUDENT_ID"],
														y = EoL_MEAS,
														by = "STUDENT_ID")
#
#      Coerce the MEAS_EVIDENCE attribute in EoL_MEAS_SECTION to numeric. Then break into intervals.
	EoL_MEAS_SECTION[,"MEAS_EVIDENCE"] <- as.numeric(EoL_MEAS_SECTION[,"MEAS_EVIDENCE"])
	EoL_MEAS_SECTION[,"IMPLIED_KNOW_STATE"] <- cut(x = EoL_MEAS_SECTION[,"MEAS_EVIDENCE"],
																								breaks = as.numeric(unique(unlist(KNOW_STATE_SPEC[,c("LOW_BOUND","UP_BOUND")]))),
																								labels = KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"])
#
#      Finally, write the unit-normalized distribution to the to the rows of CPD_LONG for which TARG_VERT_CFG is zero-valued.
	CPD_LONG[CPD_LONG[,"TARG_VERT_CFG"] == "0","COND_PROB"] <- table(EoL_MEAS_SECTION[,"IMPLIED_KNOW_STATE"])/
																																sum(table(EoL_MEAS_SECTION[,"IMPLIED_KNOW_STATE"]))
#
# 2︎⃣ RESHAPE THE CPD FACTORS INTO WIDE-TABLE FORMAT.  
#      We now loop through CPD_LONG, creating a list of wide-format CPDs.  This technically isn't an actual reshaping transformation.
#      CPD_LONG contains the measure columns for each CPD.  Its measure columns are vertically stacked with a TARG_VERT_CFG
#      indicating the number of parent vertices for the configuration.  For each iteration through CPD_LONG, we:
#      ⓐ Extract into a single-column data frame the CPD measure values from COND_PROB column for the corresponding parent-vertex
#           configuration;
#      ⓑ Concatenate row-wise columns related to the variable states.
#      ⓒ Relabel data-frame columns so as to distinguish parent variables from children.
#      ⓓ Store the result as a data frame in list CPD_WIDE.
#      ⓔ Extract the state variables and merge with KNOW_STATE for the next pass through the control loop.
#      Declare a CPD_WIDE list.  Then initiate the loop.
	CPD_WIDE <- list()
	STATE_VAR <-  data.frame(TARG_VAR = KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"])
	for (vert_cfg_idx in unique(CPD_LONG[,"TARG_VERT_CFG"])[-1]){		## vert_cfg_idx <- unique(CPD_LONG[,"TARG_VERT_CFG"])[-1][1]
	# Define an OBS_VAR data frame for the vert_cfg_idxᵗʰ vertex configuration.  Merge it with STATE_VAR.
		OBS_VAR.vert_cfg_idx <- data.frame(KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"])
		colnames(OBS_VAR.vert_cfg_idx) <- paste("OBS_VAR", vert_cfg_idx, sep = "_")
		STATE_VAR <- merge(OBS_VAR.vert_cfg_idx, STATE_VAR)
	#
	# Append TARG_VAR_PROB column to OBS_VAR.vert_cfg_idx. TARG_VAR_PROB comes from CPD_LONG records for which TARG_VERT_CFG
	# equals vert_cfg_idx. 
		TARG_VAR_PROB.vert_cfg_idx <- data.frame(TARG_VAR_PROB = CPD_LONG[CPD_LONG[,"TARG_VERT_CFG"] == vert_cfg_idx,
																																		"COND_PROB"])
		CPD_WIDE.vert_cfg_idx <- cbind(STATE_VAR, TARG_VAR_PROB.vert_cfg_idx)
	#
	# Store the "wide-table" CPD — containing the target-, observed-variable states — as the vert_cfg_idxᵗʰ of list CPD_WIDE.
		CPD_WIDE[[paste("CPD",vert_cfg_idx,"PAR",sep = "_")]] <- CPD_WIDE.vert_cfg_idx
	#
	}
#
#      Now add to list CPD_WIDE a data frame corresponding to the root-edge vertices for which no parents are defined.
	CPD_WIDE[["CPD_0_PAR"]] <- data.frame(TARG_VAR = KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"],
																		TARG_VAR_PROB = CPD_LONG[CPD_LONG[,"TARG_VERT_CFG"] == "0","COND_PROB"])
#
# 3︎⃣ ASCERTAIN THE VERTEX CONFIGURATION FOR EACH VERTEX IN THE NETWORK. 
# 4︎⃣ ASSOCIATE A CPD FACTOR WITH EACH VERTEX.  The UNIT_MAP_EDGE_LIST, a list of graph edges
#      for the unit learning map, contains this information. This includes all vertices except for "root vertices".  Root vertices are edge vertices
#      for which no parents are specified. Identify edge vertices as those in LEARNING_STANDARD_ID_FROM but not in 
#      LEARNING_STANDARD_ID_TO.  Constructing a table of LEARNING_STANDARD_ID_TO produces this count easily.  This gives us
#      the LEARNING_STANDARD_ID_TO for each target vertex — vertices for which parent vertices are specified — and the count
#      of each.  Rename the resulting TARG_VERT columns to LEARNING_STANDARD_ID_TO and CPD_WIDE. The latter attribute specifies
#      the index of the CPD in CPD_WIDE corresponding to the target vertex of interest. 
	ROOT_VERT <- setdiff(UNIT_MAP_EDGE_LIST[,"LEARNING_STANDARD_ID_FROM"],
										UNIT_MAP_EDGE_LIST[,"LEARNING_STANDARD_ID_TO"])
	LEAF_VERT <- setdiff(UNIT_MAP_EDGE_LIST[,"LEARNING_STANDARD_ID_TO"],
										UNIT_MAP_EDGE_LIST[,"LEARNING_STANDARD_ID_FROM"])
	TARG_VERT <- data.frame(table(UNIT_MAP_EDGE_LIST["LEARNING_STANDARD_ID_TO"]))
	colnames(TARG_VERT) <- c("LEARNING_STANDARD_ID_TO","CPD_WIDE")
	TARG_VERT <- merge(x = TARG_VERT,
										y = unique(UNIT_MAP_EDGE_LIST[c("LEARNING_STANDARD_ID_TO","CLUST")]),
										by = "LEARNING_STANDARD_ID_TO",
										all = FALSE)
#
#      Now add the root-edge vertices to TARG_VERT.
	ROOT_VERT.data_frame <- unique(UNIT_MAP_EDGE_LIST[UNIT_MAP_EDGE_LIST [,"LEARNING_STANDARD_ID_FROM"] %in% ROOT_VERT,
																									c("LEARNING_STANDARD_ID_FROM","CLUST")])
	colnames(ROOT_VERT.data_frame) <- c("LEARNING_STANDARD_ID_TO","CLUST")
	ROOT_VERT.data_frame["CPD_WIDE"] <- 0
	TARG_VERT <- rbind(TARG_VERT, ROOT_VERT.data_frame)
	rownames(TARG_VERT) <- TARG_VERT[,"LEARNING_STANDARD_ID_TO"]
#
#      The table function by which TARG_VERT was constructed assigns an integer value to the counts column, CPD_WIDE.
#      Prepend and append character strings to these integers in order to reconcile their values to the indices of list
#      CPD_WIDE. Also, coerce each attribute to character (table created LEARNING_STANDARD_ID_TO as factor).
	TARG_VERT[,"CPD_WIDE"] <- paste("CPD",TARG_VERT[,"CPD_WIDE"],"PAR",sep = "_")
	rownames(TARG_VERT) <- TARG_VERT[,"LEARNING_STANDARD_ID_TO"]
	TARG_VERT[,"LEARNING_STANDARD_ID_TO"] <- as.character(TARG_VERT[,"LEARNING_STANDARD_ID_TO"])
#
# 5︎⃣ EXTRACT FOR EACH TARGET VERTEX THE CORRESPONDING CPD FROM CPD_WIDE.  First append the corresponding 
#      CPD_WIDE element — indexed by the CPD_WIDE column in TARG_VERT — as a column of lists to TARG_VERT. 
	TARG_VERT[["VERT_CPD"]] <- CPD_WIDE[TARG_VERT[,"CPD_WIDE"]]
	names(TARG_VERT[["VERT_CPD"]]) <- rownames(TARG_VERT)
#
#      Now rename the columns of each VERT_CPD in TARG_VERT.  Change the column names so that the OBS_VAR columns
#      correspond to the corresponding LEARNING_STANDARD_ID_FROM instances in UNIT_MAP_EDGE_LIST, the 
#      TARG_VAR becomes the LEARNING_STANDARD_ID_TO, and TARG_VAR_PROB becomes PROB_"LEARNING_STANDARD_ID_TO".
	for (targ_vert_idx in rownames(TARG_VERT)){						## targ_vert_idx <- tail(rownames(TARG_VERT),1)
	#  First identify the observed variables.  These are the LEARNING_STANDARD_ID_FROM instances in UNIT_MAP_EDGE_LIST for which 
	#  LEARNING_STANDARD_ID_TO equals targ_vert_idx.
		OBS_VAR.targ_vert_idx <- UNIT_MAP_EDGE_LIST[UNIT_MAP_EDGE_LIST[,"LEARNING_STANDARD_ID_TO"] == targ_vert_idx,
																							"LEARNING_STANDARD_ID_FROM"]
	#
	#  Now reassign the column names.
		TARG_VERT_COLS.targ_vert_idx <- c(OBS_VAR.targ_vert_idx,
																	targ_vert_idx,
																	paste("PROB", targ_vert_idx, sep = "_"))
		TARG_VERT[targ_vert_idx,][["VERT_CPD"]] <- lapply(X = TARG_VERT[targ_vert_idx,"VERT_CPD"],
																						FUN = setNames,
																						TARG_VERT_COLS.targ_vert_idx)
	}
#
# 6︎⃣ MERGE (INNER-JOIN) ALL OF THE CPDs  TO CONTRUCT THE JDF.  Use here the merge function from the base package.  The order in which 
#      the merges are performed requires some consideration. Merges are performed pair-wise.  We need therefore to select CPD pairs sharing
#      common scope.
#
#      This is challenging for at least two reasons which preclude use of a simple control loop.
#      ⪧ Some vertices are parents to more than one child.  A simple "linear scan" through TARG_VERT would merge in 
#          these vertices more than once.
#      ⪧ CPD joins are accomplished pairwise.  A pair must have common scope in order to be merged. This is particularly challenging
#          for the first few merges. The final merges contain all of the scope.
#      ⪧ Limited graph connectivity. Particularly in a sparsely connected graph, not all vertices may be accessible from a single path. Given the
#         need for pairwise CPD merges of CPDS only once, this resembles a Seven Bridges of Königsberg problem, in the best case.  Indeed,
#         the graph may contain unconnected vertex "clusters".
#
#      Address these difficulties by working the graph one cluster at a time.  Specifically, apply the following procedure.
#      ⓐ Reconstruct the UNIT_MAP graph.  Apply the igraph cluster function to associate each vertex with a disconnnected "cluster".
#           Join this cluster-membership attribute onto TARG_VERT.
#      ⓑ Construct "supervisory" lists of root vertices, leaf vertices, unmerged vertices, and merged vertices. We use these lists
#           to leave out merging of CPDs for root vertices — which we infer from distributions of actual evidence of learning — and
#           to avoid joining in a vertex' CPD more than once.
#      ⓒ Loop through the clusters. Costruct a distinct CPD table for each cluster.  For each cluster
#           ⅰ. Develop of "in-scope" vertices that excludes root-edge vertices.
#          ⅱ. Pick one of the "in-scope" vertices.  Use its CPD to initialize the cluster component of the UNIT_JDF.
#              Remove the corresponding target-variable LEARNING_STANDARD_ID from the UNMERGED_VERTS list of unmerged
#              vertices and add it to the MERGED_VERTS list of merged vertices.
#         ⅲ. Identify another "in-scope" vertex lying in the intersection of the MERGED_VERTS and the observed variables of 
#              the UNIT_JDF table.  Extract its corresponding VERT_CPD from TARG_VERT and merge onto UNIT_JDF. Remove the 
#              corresponding vertex from the UNMERGED_VERTS, add to the MERGED_VERTS.
#         ⅳ. Repeat until the all of the vertices connected to the cluster have been joined.
#         Use a for loop to work through the clusters and a while loop to step through each cluster.
#
#      ⓐ Reconstruct the UNIT_MAP graph and identify clusters. Merge onto TARG_VERT data frame. (Already done.  Function reallocated to
#           UNIT_SUBGRAPH subroutine.)
#
#      ⓑ Construct "supervisory" lists of root vertices, leaf vertices, unmerged vertices, and merged vertices. 	We previously defined
#           TARG_VERT so that it excludes root-edge vertices. No further adjustment is necessary.
	UNMERGED_VERTS <- rownames(TARG_VERT)
	MERGED_VERTS <- character()
#
#      ⓒ Loop through the clusters.  First declare a list UNIT_JDF into which to store the cluster CPDs.
	UNIT_JDF <- list()
# Extract from TARG_VERT a list of vertices corresponding to the clust_idxᵗʰ cluster.
	for(clust_idx in unique(TARG_VERT[,"CLUST"])){
	CLUST_VERT.clust_idx <- TARG_VERT[TARG_VERT[,"CLUST"] == clust_idx, "LEARNING_STANDARD_ID_TO"]
#
# Randomly select LEARNING_STANDARD_ID from CLUST_VERT.clust_idx. Start with a root-edge vertex.
	TARG_VERT.clust_idx <- intersect(CLUST_VERT.clust_idx, ROOT_VERT)[1]
#
# Decrement the sampled vertex TARG_VERT.clust_idx from CLUST_VERT.clust_idx and from UNMERGED_VERTS. Add to 
# MERGED_VERTS. Extract the corresponding CPD from TARG_VERT.
	CLUST_VERT.clust_idx <- setdiff(CLUST_VERT.clust_idx, TARG_VERT.clust_idx)
	UNMERGED_VERTS <- setdiff(UNMERGED_VERTS, TARG_VERT.clust_idx)
	MERGED_VERTS <- union(MERGED_VERTS, TARG_VERT.clust_idx)
	UNIT_JDF.clust_idx <- TARG_VERT[["VERT_CPD"]][[TARG_VERT.clust_idx]]
#
# Now initiate the while loop.  
	while(length(CLUST_VERT.clust_idx) > 0){
	# Select a new TARG_VERT.clust_idx. This must be a variable for which the scope of the associated VERT_CPD coincides with that of UNIT_JDF.clust_idx.
		TARG_VERT.clust_idx <- unlist(lapply(X = lapply(X = lapply(X = TARG_VERT[["VERT_CPD"]][CLUST_VERT.clust_idx], 
																										FUN = colnames), 
																					FUN = intersect,
																					colnames(UNIT_JDF.clust_idx)),
																	FUN = length))
		TARG_VERT.clust_idx <- names(TARG_VERT.clust_idx[which(TARG_VERT.clust_idx != 0)])
		TARG_VERT.clust_idx <- TARG_VERT.clust_idx[1]
		print(paste("Target Vertex",
							TARG_VERT.clust_idx,
							"Merged",
							length(MERGED_VERTS),
							"Unmerged",
							length(UNMERGED_VERTS),
							"Starting at",
							format(Sys.time(),"%H:%M:%S")
						))
	#
	# Repeat the procedure above with the "supervisory" variables.
		CLUST_VERT.clust_idx <- setdiff(CLUST_VERT.clust_idx, TARG_VERT.clust_idx)
		UNMERGED_VERTS <- setdiff(UNMERGED_VERTS, TARG_VERT.clust_idx)
		MERGED_VERTS <- union(MERGED_VERTS, TARG_VERT.clust_idx)
	#
	# Merge the corresponding VERT_CPD onto UNIT_JDF.clust_idx.
		UNIT_JDF.clust_idx <- merge(x = UNIT_JDF.clust_idx,
														y = TARG_VERT[["VERT_CPD"]][[TARG_VERT.clust_idx]])
		print(paste("Target Vertex",
							TARG_VERT.clust_idx,
							"Merged",
							length(MERGED_VERTS),
							"Unmerged",
							length(UNMERGED_VERTS),
							"Ending at",
							format(Sys.time(),"%H:%M:%S")
						))
	#
	# Repeat until all of the VERT_CPDs corresponding to vertices in cluster have been merged into UNIT_JDF.clust_idx.
	#	
	}					## end while
#	
# Finally, multiply measure columns — demarcated by column names containing the text string "PROB_" — together and drop.
	UNIT_JDF.clust_idx["MEAS"] <- apply(X = UNIT_JDF.clust_idx[grep(x = colnames(UNIT_JDF.clust_idx),
																													pattern = "PROB_")],
																	MARGIN = 1,
																	FUN = prod)
	for (col_idx in colnames(UNIT_JDF.clust_idx)[grep(x = colnames(UNIT_JDF.clust_idx),
																					pattern = "PROB_")] ) UNIT_JDF.clust_idx[col_idx] <- NULL
#
# Store the clust_idxᵗʰ UNIT_JDF in list UNIT_JDF.
	UNIT_JDF[[clust_idx]] <- UNIT_JDF.clust_idx
#	
	}					## end for clust_idx
#
# 7︎⃣ WRITE THE UNIT-SUBGRAPH CPD OUT AS AN RData OBJECT.
	for (clust_idx in unique(TARG_VERT[,"CLUST"])) write.csv(x = UNIT_JDF[[clust_idx]],
																								file = paste(PROF_TASK.dir,
																													paste("GRAPHCLUST", clust_idx,
																																"UNIT_MAP_JDF.csv",
																																sep = "_"),
																													sep = "/"),
																								row.names = FALSE,
																								eol = "\r\n",
																								fileEncoding = "UTF-8",
																								quote = TRUE)
#