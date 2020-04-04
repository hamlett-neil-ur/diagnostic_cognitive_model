## PURPOSE: Built a "Unit-blanket" graph for the learning standards within scope of the instructional unit.  This "blanket" graph
## includes the in-scope standards themselves, as well as ascestors likely to exert non-negligible influence and descendants
## over which the in-scope standards exert non-negligible influence. We particularly want  common ancestors and descendants
## through which influence might flow according to the calculus of conditional probability. 
##
## MAJOR STEPS:
## 1︎⃣ DATA PREPARATION.  Read in proficiency-, task-model tables and prepare them for definition of the graph.  Specifying
##      vertices in terms of educator-recognized LEARNING_STANDARD_CD.
## 2︎⃣ INITIALIZE GRAPH.  Our initial graph contains all edges and vertices specified by the LEARNING_PROGRESSION table.
## 3︎⃣ IDENTIFY "UNIT BLANKET" SUBMAP.  The "unit blanket" is the graph neighborhood of vertices sufficiently proximate to vertices within
##      scope of the course unit as to have non-negligible influence according to the calculus of conditional probability. 
## 4︎⃣ CONVERT UNIT SUBMAP INTO DATA FRAME AND EXPORT AS CSV FILE.
#
# Initialize environment.
	options(stringsAsFactors = FALSE)
	options(java.parameters = "-Xmx16g")
	library(igraph)
	quartzFonts(avenir = c("Avenir Book", "Avenir Black","Avenir Book Oblique",  "Avenir Black Oblique"))
#
# Define graphics parameters of interest later.
# 1︎⃣ DATA PREPARATION.  
#     Begin reading in the required tables.  We need the following:
#     ⪧ USE_CASE_ATTRIBUTES contains the parameters specifying the particular prototype case; and
#     ⪧ COURSE_UNIT contains the content of courses in terms of units and learning standards.  
#     We then need the following proficiency-model tables:
#     ⪧ LEARNING_PROGRESSION contains an edge list for all learning standards;
	proto.dir <- "/Users/nahamlet/Box Sync/IBM-Watson ED K12/Pathway-Centric CONOPS/Learning-Map Prototype"
	USE_CASE_ATTRIBUTES <- read.csv(file = paste(proto.dir, "Use_CASE_QUERY_ATTRIBUTES.csv", sep = "/"),
																colClasses = "character")
	rownames(USE_CASE_ATTRIBUTES) <- USE_CASE_ATTRIBUTES[,"QUERY_ATTRIBUTE"]
	Case.dir <- USE_CASE_ATTRIBUTES["Case.dir","VALUE"]
	setwd(Case.dir)
	PROF_TASK.dir <- paste(Case.dir, "PROF_TASK_MODEL", sep = "/")
	COURSE_UNIT <- read.csv(file = paste(PROF_TASK.dir, "COURSE_UNIT.csv", sep = "/"),
												colClasses = "character")[c("COURSE_ID","SUBJECT_TITLE","COURSE_TITLE","UNIT_ID",
																							"UNIT_TITLE","LEARNING_STANDARD_ID")]
	LEARNING_PROGRESSION <- read.csv(file = paste(PROF_TASK.dir, "LEARNING_STANDARD_PROGRESSION.csv", sep = "/"),
																	colClasses = "character")[c("LEARNING_STANDARD_ID_FROM","LEARNING_STANDARD_ID_TO")]
# 
# 2︎⃣ INITIALIZE GRAPH.  The initial graph is comprised of all vertices and edges specified in LEARNING_PROGRESSION.
	LEARN_MAP <- graph_from_data_frame(d = LEARNING_PROGRESSION[,c("LEARNING_STANDARD_ID_TO",
																		  													"LEARNING_STANDARD_ID_FROM")],
																		 directed = TRUE,
																		 vertices = sort(unique(unlist(LEARNING_PROGRESSION))))
#
# 3︎⃣ IDENTIFY "UNIT BLANKET" SUBMAP.
#      The LEARNING_STANDARD_CD instances now correspond to vertices in the LEARN_MAP graph.  It is convenient to associate
#       each LEARNING_STANDARD_CD in COURSE_UNIT with its corresponding index in the LEARN_MAP vertex list.  Do this using match.
	COURSE_UNIT[,"VERT_IDX"] <- match(x = COURSE_UNIT[,"LEARNING_STANDARD_ID"],
																	table = names(V(LEARN_MAP)))
#
#      "Filter" COURSE_UNIT so that it only contains records associated the COURSE_ID and UNIT_ID in USE_CASE_ATTRIBUTES. Construct
#       a one-row data frame of COURSE_ID, COURSE_TITLE, UNIT_ID and inner-join COURSE_UNIT onto that one-row data frame.
	USE_CASE_UNIT <- data.frame(COURSE_ID = USE_CASE_ATTRIBUTES["COURSE_ID","VALUE"],
													COURSE_TITLE = USE_CASE_ATTRIBUTES["COURSE_TITLE","VALUE"],
													UNIT_ID = USE_CASE_ATTRIBUTES["UNIT_ID","VALUE"])
	USE_CASE_UNIT <- merge(x = USE_CASE_UNIT,
													y = COURSE_UNIT,
													by = c("COURSE_ID","COURSE_TITLE","UNIT_ID"))
#
#       Now define an induced subgraph from the vertices in the neighborhood of those within scope of USE_CASE_UNIT.  Use the
#       function identify ego to identify all of the vertices within a specified distance of each LEARNING_STANDARD_CD in USE_CASE_UNIT.
#       The function ego produces a list of vertex lists: One vertex list for each LEARNING_STANDARD_CD instance in USE_CASE_UNIT.
#       Unlist this list of lists, and use it as an argument to induced_sub_graph.
	UNIT_MAP.verts <- unique(unlist(ego(graph = LEARN_MAP,
																nodes = USE_CASE_UNIT[,"VERT_IDX"],
																order = as.numeric(USE_CASE_ATTRIBUTES["NEIGHBOR_RADIUS","VALUE"]),
																mode = "all")))
	UNIT_MAP <- induced_subgraph(graph = LEARN_MAP,
															vids = UNIT_MAP.verts,
															impl = "create_from_scratch")
#
#
#       Our UNIT_MAP might not be fully connected.  We want to identified unconnected clusters of vertices. We want our final edge list
#        to indicate cluster membership.  We associate cluster membership with the destination vertex in the directed edge.
	UNIT_MAP_CLUST <- data.frame(CLUST = clusters(graph = UNIT_MAP)$membership)
	UNIT_MAP_CLUST["LEARNING_STANDARD_ID_TO"] <- rownames(UNIT_MAP_CLUST)
#
# 4︎⃣ CONVERT UNIT SUBMAP AND EXPORT. Use the igraph-package as_data_frame to extract an edge list from the UNIT_MAP object.
#      Rename the columns so that the result resembles WEDC-schema LEARNING_STANDARD_PROGRESSION.  Then, write to
#      csv file.  Add a logical column to specify whether or not a vertex is in scope of the unit. Join the UNIT_MAP_CLUST onto UNIT_MAP.edges
#      by LEARNING_STANDARD_ID_TO get that information into the subgraph.  Coerce all columns to UTF-8 character and write
#      out to a csv file.
	UNIT_MAP.edges <- as_data_frame(x = UNIT_MAP,
															what = "edges")
	colnames(UNIT_MAP.edges) <- c("LEARNING_STANDARD_ID_TO",
															"LEARNING_STANDARD_ID_FROM")
	UNIT_MAP.edges <- merge(x = UNIT_MAP.edges,
														y = UNIT_MAP_CLUST,
														by = "LEARNING_STANDARD_ID_TO")
	UNIT_MAP.edges["IN_UNIT_FROM"] <- ifelse(
						test = UNIT_MAP.edges[,"LEARNING_STANDARD_ID_FROM"] %in% USE_CASE_UNIT[,"LEARNING_STANDARD_ID"],
						yes = TRUE,
						no = FALSE)
	UNIT_MAP.edges["IN_UNIT_TO"] <- ifelse(
						test = UNIT_MAP.edges[,"LEARNING_STANDARD_ID_TO"] %in% USE_CASE_UNIT[,"LEARNING_STANDARD_ID"],
						yes = TRUE,
						no = FALSE)
	for (col_idx in colnames(UNIT_MAP.edges)) UNIT_MAP.edges[,col_idx] <- enc2utf8(as.character(UNIT_MAP.edges[,col_idx]))
	write.csv(x = UNIT_MAP.edges,
					file = paste(PROF_TASK.dir,
										"UNIT_MAP_EDGE_LIST.csv",
										sep = "/"),
					row.names = FALSE,
					eol = "\r\n",
					fileEncoding = "UTF-8",
					quote = TRUE)
#
