## PURPOSE: CONSTRUCT A GRAPH-LAYOUT TABLE FOR THE UNIT_SUBGRAPH SPECIFIED BY UNIT_MAP_EDGE_LIST.
## The result is used by plot.igraph to specify the vertex-location coordinates in a cartesian grid. UNIT_MAP_EDGE_LIST 
## contains incident vertices specified in terms of LEARNING_STANDARD_ID.  The learning standards span multiple grade/course
## levels of which a unit-map blanket is comprised. We want to separate these grade/course levels in one dimension.  The
## within-grade/course learning standards will be distributed across the other dimension.
##
## We want our vertices distributed over an approximately-rectangular lattice such that edge crossings are minimized. Accomplish this
## by partitioning the x-dimension according to STANDARD_CONTENT and the y-dimension according to membership in a disconnnected
## cluster of connected subgraph vertices.  Vertex distributions are approximately rectangular because of non-uniform distributions of vertices
## along these two attribute dimensions.
##
## MAJOR STEPS:
## 1︎⃣ DATA INGESTION.  Read in proficiency-model tables including UNIT_MAP_EDGE_LIST, LEARNING_STANDARD.  The first
##      gives the graph itself, and the second is used to distinguish between grade/course levels.  The essential information resides in the 
##      following tables.
##      ⓐ USE_CASE_ATTRIBUTES contains the specifications of the case study.
##      ⓑ UNIT_MAP_EDGE_LIST contains the edge list defining the course-unit learning map.  It also contains vertex-cluster membership
##           and a logical flag indicating whether the the vertex pertains to a learning standard within scope of the course unit.
##      ⓒ LEARNING_STANDARD contains the link between LEARNING_STANDARD_ID, by which vertex identies are specified, and
##           the STANDARD_CONTENT_ID, by which we associate out-of-course-unit-scope vertices with the academic courses within which
##           they are delivered.
##      ⓓ STANDARD_CONTENT_PROGRESSION contains the STANDARD_CONTENT progressions. We use this to "bin" vertices 
##           in terms of their x-dimension "band" of the vertex-distribution "grid."
## 2︎⃣ CONVERT EDGE LIST TO VERTEX LIST.  We want our vertex list to contain the IN_UNIT and CLUST attributes.  
## 3︎⃣ DISTRIBUTE THE VERTICES across a grid. We have to distribute across dimensions.
##      ⓐ The x-coordinate distribution is determined by STANDARD_CONTENT_ID. We first need to get the progression of STANDARD_CONTENT_IDs.
##           Then we bin the vertices according to their STANDARD_CONTENT_ID "band" of the x-dimension.  The STANDARD_CONTENT_ID bands
##            are equal in dimension.
##      ⓑ The y-cordinate is straightforwardly assigned by the CLUST attribute. We want the width of the CLUST y-coordinate "band" to be proportion
##           with the number of vertices in each cluster.
## 4︎⃣ PREPARE THE VERTEX LIST AND WRITE OUT TO CSV. 
#
# Initialize environment.
	options(stringsAsFactors = FALSE)
	options(java.parameters = "-Xmx16g")
	library(igraph)
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
	UNIT_MAP_EDGE_LIST <- read.csv(file = paste(PROF_TASK.dir, "UNIT_MAP_EDGE_LIST.csv", sep = "/"),
												colClasses = "character")
	LEARNING_STANDARD <- read.csv(file = paste(PROF_TASK.dir, "SIHLEARNING_STANDARD.csv", sep = "/"),
																colClasses = "character")[c("LEARNING_STANDARD_ID","STANDARD_CONTENT_ID",
																											"LEARNING_STANDARD_CD")]
	STANDARD_CONTENT_PROGRESSION <- read.csv(file = paste(PROF_TASK.dir, "STANDARD_CONTENT_PROGRESSION.csv", sep = "/"),
																						colClasses = "character")[c("STANDARD_CONTENT_ID_FROM",
																																	"STANDARD_CONTENT_ID")]
#
#      For subsequent graph-algebra convenience, retain only STANDARD_CONTENT_PROGRESSION records corresponding to the
#      SUBJECT_TITLE specified in STANDARD_CONTENT_ID by the SUBJECT_TITLE VALUE in USE_CASE_ATTRIBUTES.
	STANDARD_CONTENT <- read.csv(file = paste(PROF_TASK.dir, "SIHSTANDARD_CONTENT.csv", sep = "/"),
															colClasses = "character")[c("STANDARD_CONTENT_ID","SUBJECT_TITLE")]
	STANDARD_CONTENT <- STANDARD_CONTENT[STANDARD_CONTENT[,"SUBJECT_TITLE"] == 
																											USE_CASE_ATTRIBUTES["SUBJECT_TITLE","VALUE"],
																						"STANDARD_CONTENT_ID"]
	STANDARD_CONTENT_PROGRESSION <- STANDARD_CONTENT_PROGRESSION[
									STANDARD_CONTENT_PROGRESSION[,"STANDARD_CONTENT_ID"] %in% STANDARD_CONTENT,][
												c("STANDARD_CONTENT_ID_FROM","STANDARD_CONTENT_ID")]
#
# 2︎⃣ CONVERT EDGE LIST TO VERTEX LIST.  The UNIT_MAP_EDGE_LIST contains head "TO" and tail "FROM" attributes for LEARNING_STANDARD_ID
#      and IN_UNIT.  The edge-delimiting vertices by definition reside in the same cluster CLUST.  So select the "FROM" and "TO" columns from
#      UNIT_MAP_EDGE_LIST, reconcile their column names, concatenate them, and then drop duplicates.
#      Invoke graph_from_data_frame to get a graph object for which a layout specification can be derived. 
	UNIT_MAP_VERTICES <- unique(rbind(setNames(object = UNIT_MAP_EDGE_LIST[c("LEARNING_STANDARD_ID_TO","IN_UNIT_TO","CLUST")],
																					nm = c("LEARNING_STANDARD_ID","IN_UNIT","CLUST")),
																	setNames(object = UNIT_MAP_EDGE_LIST[c("LEARNING_STANDARD_ID_FROM","IN_UNIT_FROM","CLUST")],
																					nm = c("LEARNING_STANDARD_ID","IN_UNIT","CLUST"))))
	rownames(UNIT_MAP_VERTICES) <- UNIT_MAP_VERTICES[,"LEARNING_STANDARD_ID"]
#
# 3︎⃣ DISTRIBUTE THE VERTICES.
#      ⓐ The x-coordinate distribution is determined by STANDARD_CONTENT_ID. Accomplish via the following steps.
#              ⅰ Merge UNIT_MAP_VERTICES with LEARNING_STANDARD in order to introduce the STANDARD_CONTENT_ID attribute.
#             ⅱ Ascertain the sequential progression of STANDARD_CONTENT_ID attributes using the STANDARD_CONTENT_PROGRESSION.
#            ⅲ Specify the x-coordinate centroid for each STANDARD_CONTENT_ID attribute.  Apply the corresponding x_coord attribute
#                 to the UNIT_MAP_VERTICES entity.  Then apply a random dither to the position of each.
	UNIT_MAP_VERTICES <- merge(x = UNIT_MAP_VERTICES,
														y = LEARNING_STANDARD)
	UNIT_MAP_VERTICES[,"IN_UNIT"] <- as.logical(UNIT_MAP_VERTICES[,"IN_UNIT"])
#
#                "Unwind" the STANDARD_CONTENT progression by extracting a unique list of STANDARD_CONTENT_ID, IN_UNIT pairs. Exactly
#                 one STANDARD_CONTENT_ID should have IN_UNIT as true.  Begin whith this.  It is possible that out-of-unit vertices may have the
#                 same STANDARD_CONTENT_ID as in-unit-vertices. This leads to two instances of the STANDARD_CONTENT_ID for the in-unit
#                 vertices. Drop the record for which IN_UNIT is "FALSE" for when another record for IN_UNIT is "TRUE" exists. Accomplish this by
#                 list-aggregating the data frame of STANDARD_CONTENT_ID × IN_UNIT pairs.  Then apply the any function to the result.
	STANDARD_CONTENT <- unique(UNIT_MAP_VERTICES[c("STANDARD_CONTENT_ID","IN_UNIT")])
	STANDARD_CONTENT <- aggregate(formula = IN_UNIT ~ STANDARD_CONTENT_ID,
																data = STANDARD_CONTENT,
																FUN = list)
	STANDARD_CONTENT["IN_UNIT"] <- unlist(lapply(X = STANDARD_CONTENT[["IN_UNIT"]],
																						FUN = any))
#
#                 We now want to get the paths involving the vertices. We have a STANDARD_CONTENT_PROGRESSION table. Create a directed graph
#                  from that table.  Then use the subgraph.edges command to get a subgraph with just the desired edges. These edges are 
#                  specified by the path variable of the edge function E. Use the E function to query the graph for a directed path
#                  through the content-progression vertices specified via STANDARD_CONTENT_ID from STANDARD_CONTENT_PROGRESSION.
#
#                  A wrinkle sometimes occurs.  Specifically, our STANDARD_CONTENT_PROGRESSION sequence within the span
#                  of STANDARD_CONTENT may not always contain all of the vertices for a complete path. 
	CONTENT_ID_GRAPH.dag <- graph_from_data_frame(d = STANDARD_CONTENT_PROGRESSION[c("STANDARD_CONTENT_ID_FROM",
																																										"STANDARD_CONTENT_ID")],
																							directed = TRUE)
	CONTENT_ID_GRAPH.paths <- unique(unlist(lapply(
					X = all_shortest_paths(
						graph = CONTENT_ID_GRAPH.dag,
						from = V(CONTENT_ID_GRAPH.dag)[STANDARD_CONTENT[STANDARD_CONTENT[,"IN_UNIT"], "STANDARD_CONTENT_ID"]],
						to = V(CONTENT_ID_GRAPH.dag)[STANDARD_CONTENT[!STANDARD_CONTENT[,"IN_UNIT"], "STANDARD_CONTENT_ID"]],
						mode = "all")$res,
					FUN = as_ids)))
	PATH_THRU_IN_UNIT.edges <- subgraph.edges(graph = CONTENT_ID_GRAPH.dag,
																					eids =E(graph = CONTENT_ID_GRAPH.dag,
																									path = V(CONTENT_ID_GRAPH.dag)[names(V(CONTENT_ID_GRAPH.dag)) %in% 
																																							              CONTENT_ID_GRAPH.paths]   ))
#
#                  Assign as STD_CONTENT_SEQ the vertices of the STANDARD_CONTENT_PROGRESSION subgraph
#                  PATH_THRU_IN_UNIT.edges.  Keep only those STANDARD_CONTENT vertices actually in STANDARD_CONTENT.
	STD_CONTENT_SEQ <- names(V(PATH_THRU_IN_UNIT.edges))
	STD_CONTENT_SEQ <- STD_CONTENT_SEQ[STD_CONTENT_SEQ %in% STANDARD_CONTENT[,"STANDARD_CONTENT_ID"]]
#
#                Convert STD_CONTENT_SEQ to a data frame. Then incorporate an x_coord column containing the vertex-location
#                centroids for the corresponding STANDARD_CONTENT_ID. Merge the result onto UNIT_MAP_VERTICES and apply a random
#                perturbation.
	STANDARD_CONTENT.levels = STD_CONTENT_SEQ
	STD_CONTENT_SEQ <- data.frame(STANDARD_CONTENT_ID = STD_CONTENT_SEQ)
	STD_CONTENT_SEQ["x_coord"] <- seq(from = -.875,
																	to = .875,
																	by = (2*0.875)/(nrow(STD_CONTENT_SEQ)-1))
	UNIT_MAP_VERTICES <- merge(x = UNIT_MAP_VERTICES,
														y = STD_CONTENT_SEQ)
	UNIT_MAP_VERTICES[,"STANDARD_CONTENT_ID"] <- factor(x = UNIT_MAP_VERTICES[,"STANDARD_CONTENT_ID"],
																										levels = STANDARD_CONTENT.levels)
	UNIT_MAP_VERTICES["x_coord"] <- UNIT_MAP_VERTICES["x_coord"] + runif(n = nrow(UNIT_MAP_VERTICES["x_coord"]),
																																	min = -0.675/(nrow(STD_CONTENT_SEQ)),
																																	max = 0.675/(nrow(STD_CONTENT_SEQ)))
#
#      ⓑ The y-cordinate is straightforwardly assigned by the CLUST attribute. Construct a cross-table of counts of UNIT_MAP_VERTICES
#           in terms of STANDARD_CONTENT_ID versus CLUST.  Find out the maximum count in each CLUST across all STANDARD_CONTENT_IDs.
#           This gives us the y-coordinate proportioning of the vertices.
	y_coord_prop <- apply(X = table(UNIT_MAP_VERTICES[c("STANDARD_CONTENT_ID","CLUST")]),
										MARGIN = 2,
										FUN = max)
	y_coord_prop <- c(0, cumsum(y_coord_prop)/sum(y_coord_prop))
	y_coord_prop <- 2*(y_coord_prop-1/2)
#
	y_coord_bands <- data.frame(CLUST = sort(unique(UNIT_MAP_VERTICES[,"CLUST"])),
													low_bound = head(y_coord_prop,-1),
													up_bound = tail(y_coord_prop,-1))
	rownames(y_coord_bands) <- y_coord_bands[,"CLUST"]
	y_coord_bands["centroid"] <- apply(X = y_coord_bands[c("low_bound","up_bound")],
																MARGIN = 1,
																FUN = mean)
	y_coord_bands["width"] <- apply(X = y_coord_bands[c("low_bound","up_bound")],
																MARGIN = 1,
																FUN = diff)
	y_coord_bands["low_bound"] <- y_coord_bands["centroid"] - 2*y_coord_bands["width"]/6
	y_coord_bands["up_bound"] <- y_coord_bands["centroid"] + 2*y_coord_bands["width"]/6
#
#           We now have to loop through the occurrences of STANDARD_CONTENT_ID and CLUST in UNIT_MAP_VERTICES and distribute
#            the vertices for each "bin".
	for (clust_idx in unique(UNIT_MAP_VERTICES[,"CLUST"])){													## clust_idx <- unique(UNIT_MAP_VERTICES[,"CLUST"])[3]
		for (std_cont_idx in levels(UNIT_MAP_VERTICES[,"STANDARD_CONTENT_ID"])){	
																					## std_cont_idx <- levels(UNIT_MAP_VERTICES[,"STANDARD_CONTENT_ID"])[1]
		# Find the records corresponding to the  clust_idx × std_cont_idx pair.
			y_coord_bin <- intersect(which(UNIT_MAP_VERTICES[,"CLUST"] == clust_idx),
													which(UNIT_MAP_VERTICES[,"STANDARD_CONTENT_ID"] == std_cont_idx))
		#	
		# Assign y_coord to elements of y_coord_bin conditioning on the vertex counts therein.
			if (length(y_coord_bin) > 1) {
				UNIT_MAP_VERTICES[y_coord_bin,"y_coord"] <- seq(from = y_coord_bands[clust_idx,"low_bound"],
																						 			to = y_coord_bands[clust_idx,"up_bound"],
																									by = diff(unlist(y_coord_bands[clust_idx,c("low_bound","up_bound")]))/(length(y_coord_bin) - 1)  )
			} else if (length(y_coord_bin) == 1) {
				UNIT_MAP_VERTICES[y_coord_bin,"y_coord"] <- mean(unlist(y_coord_bands[clust_idx,c("low_bound","up_bound")]))
			}
		#
		}
	}
#
#           Add plot parameters for igraph plotting.
	UNIT_MAP_VERTICES["vert_size"] <- ifelse(test = UNIT_MAP_VERTICES[,"IN_UNIT"],
																		yes = 18,
																		no = 12)
	UNIT_MAP_VERTICES["frame_width"] <- ifelse(test = UNIT_MAP_VERTICES[,"IN_UNIT"],
																		yes = 5,
																		no = 1)
	UNIT_MAP_VERTICES["vert_label_size"] <- ifelse(test = UNIT_MAP_VERTICES[,"IN_UNIT"],
																		yes = 2,
																		no = 1.25)
#
#           Apply the random-perturbation dither to the y_coord attributes.
	UNIT_MAP_VERTICES["y_coord"] <- UNIT_MAP_VERTICES["y_coord"] + runif(n = nrow(UNIT_MAP_VERTICES["y_coord"]),
																																	min = -.0375,
																																	max = .0375)
#
	for (col_idx in colnames(UNIT_MAP_VERTICES)) UNIT_MAP_VERTICES[,col_idx] <- enc2utf8(as.character(UNIT_MAP_VERTICES[,col_idx]))
	write.csv(x = UNIT_MAP_VERTICES,
					file = paste(PROF_TASK.dir,
										"UNIT_MAP_VERTICES.csv",
										sep = "/"),
					row.names = FALSE,
					eol = "\r\n",
					fileEncoding = "UTF-8",
					quote = TRUE)
#
#
