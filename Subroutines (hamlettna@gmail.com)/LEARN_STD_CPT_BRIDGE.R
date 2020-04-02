## PURPOSE: CONSTRUCT A PROTOTYPE BRIDGE FUNCTION BETWEEN LEARNING STANDARDS AND
## CPT TABLES. We require in Bayesian-network calculations to associate each learning standard — represented
## as a graph vertex — with a conditional-probability table. We use for prototype purposes a standard, heuristically derived
## CPT.  The CPT pertaining to a particular learning standard is completely specified by the number of parent vertices
## in LEARNING_STANDARD_PROGRESSION, a list of graph edges.  We therfore for this subroutine, peform the following steps.
## 1︎⃣ Read in the needed table. We specifically only need LEARNING_STANDARD_PROGRESSION.
## 2︎⃣ Ascertain the number of LEARNING_STANDARD_ID_FROM instances there are corresponding to 
##      each distinct LEARNING_STNDARD_ID_TO in the LEARNING_STANDARD_PROGRESSION table.  This
##      can be accomplished by list-aggregate operation.
## 3︎⃣ Synthesize the place-holder character-string VERT_CPT_BRIDGE_IDX index value into the CPT_LONG table.
##      this simply involves prepending and appending character strings onto the attribute containing the number of
##      parent vertices.
## 4︎⃣ Clean up, format, write out to csv file.
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
	LEARNING_PROGRESSION <- read.csv(file = paste(PROF_TASK.dir, "LEARNING_STANDARD_PROGRESSION.csv", sep = "/"),
																	colClasses = "character")[c("LEARNING_STANDARD_ID_FROM","LEARNING_STANDARD_ID_TO")]
# 
# 2︎⃣ Ascertain the number of LEARNING_STANDARD_ID_FROM instances.  Apply a list-aggregate operation and apply
#      length to the result.
	LEARN_STD_CPT_BRIDGE <- aggregate(formula = LEARNING_STANDARD_ID_FROM ~ LEARNING_STANDARD_ID_TO,
																		data = LEARNING_PROGRESSION[c("LEARNING_STANDARD_ID_TO","LEARNING_STANDARD_ID_FROM")],
																		FUN = list)
	LEARN_STD_CPT_BRIDGE["VERT_CPT_BRIDGE_IDX"] <- unlist(lapply(X = LEARN_STD_CPT_BRIDGE[["LEARNING_STANDARD_ID_FROM"]],
																													FUN = length))
#
# 3︎⃣ Synthesize the place-holder character-string VERT_CPT_BRIDGE_IDX index value into the CPT_LONG table.
	LEARN_STD_CPT_BRIDGE["VERT_CPT_BRIDGE_IDX"] <- paste("HEURISTIC_MVN",	
																											LEARN_STD_CPT_BRIDGE[,"VERT_CPT_BRIDGE_IDX"],
																											"SDEV_1",
																											sep = "_")
#
# 4︎⃣ Clean up, format, write out to csv file.  Prune and rename attributes.  Coerce all attributes to UTF-8 character. Write out to csv.
	LEARN_STD_CPT_BRIDGE <- LEARN_STD_CPT_BRIDGE[c("LEARNING_STANDARD_ID_TO","VERT_CPT_BRIDGE_IDX")]
	LEARN_STD_CPT_BRIDGE <- setNames(object = LEARN_STD_CPT_BRIDGE,
																		nm = c("LEARNING_STANDARD_ID","VERT_CPT_BRIDGE_IDX"))
#
	for (col_idx in colnames(LEARN_STD_CPT_BRIDGE)) LEARN_STD_CPT_BRIDGE[,col_idx] <- enc2utf8(as.character(LEARN_STD_CPT_BRIDGE[,col_idx]))
#
	write.csv(x = LEARN_STD_CPT_BRIDGE,
					file = paste(PROF_TASK.dir, "LEARN_STD_CPT_BRIDGE.csv", sep = "/"),
					row.names = FALSE,
					eol = "\r\n",
					fileEncoding = "UTF-8",
					quote = TRUE)
#