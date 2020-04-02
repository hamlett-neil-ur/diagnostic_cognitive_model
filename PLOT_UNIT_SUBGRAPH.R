## PURPOSE:  PLOT A UNIT SUBGRAPH WITH LEARNING_STANDARD_ID VERTEX LABELS.

# Initialize environment.
	options(stringsAsFactors = FALSE)
	options(java.parameters = "-Xmx16g")
	library(igraph)
	quartzFonts(avenir = c("Avenir Book", "Avenir Black","Avenir Book Oblique",  "Avenir Black Oblique"))
	ibm.blue <- rgb(red=75/255,green=107/255,blue=175/255)
	ibm.blue.xpar <- rgb(red=75/255,green=107/255,blue=175/255,alpha = 0.5)
	bg.col <- rgb(red=255/255,green=250/255,blue=240/255,alpha = 0.25)
	hl.col <- rgb(red=255/255,green=255/255,blue=0/255,alpha = 0.25)

#
# 1︎⃣ DATA INGESTION.  
	proto.dir <- "/Users/nahamlet/Box Sync/IBM-Watson ED K12/Pathway-Centric CONOPS/Learning-Map Prototype"
	USE_CASE_ATTRIBUTES <- read.csv(file = paste(proto.dir, "USE_CASE_QUERY_ATTRIBUTES.csv", sep = "/"),
																colClasses = "character")
	rownames(USE_CASE_ATTRIBUTES) <- USE_CASE_ATTRIBUTES[,"QUERY_ATTRIBUTE"]
	Case.dir <- USE_CASE_ATTRIBUTES["Case.dir","VALUE"]
	setwd(Case.dir)
	PROF_TASK.dir <- paste(Case.dir, "PROF_TASK_MODEL", sep = "/")
	UNIT_MAP_EDGES <- read.csv(file = paste(PROF_TASK.dir, "UNIT_MAP_EDGE_LIST.csv", sep = "/"),
																colClasses = "character") 
	UNIT_MAP_VERTICES <- read.csv(file = paste(PROF_TASK.dir, "UNIT_MAP_VERTICES.csv", sep = "/"),
													colClasses = "character") 
	UNIT_MAP_VERTEX_STATES <- read.csv(file = paste(PROF_TASK.dir, "VERTEX_STATE_DIST.csv", sep = "/"),
																		colClasses = "character") 
	UNIT_MAP_VERTEX_STATES[,"MEASURED"] <- as.logical(UNIT_MAP_VERTEX_STATES[,"MEASURED"])
	KNOW_STATE_SPEC <- read.csv(file = paste(PROF_TASK.dir, "KNOW_STATE_SPEC.csv", sep = "/"),
															colClasses = "character") 
	LEARNING_STANDARD <- read.csv(file = paste(PROF_TASK.dir, "SIHLEARNING_STANDARD.csv", sep = "/"),
																colClasses = "character")[c("LEARNING_STANDARD_ID","LEARNING_STANDARD_CD")]
	COURSE_ENROLL <- read.csv(file = paste(PROF_TASK.dir, "COURSE_ENROLL.csv", sep = "/"),
																colClasses = "character")
	COURSE_UNIT <- read.csv(file = paste(PROF_TASK.dir, "COURSE_UNIT.csv", sep = "/"),
																colClasses = "character")[c("COURSE_ID","COURSE_TITLE","UNIT_ID","UNIT_TITLE")]
	UNIT_MAP_VERTICES <- merge(x = UNIT_MAP_VERTICES,
														y = LEARNING_STANDARD)
	rownames(UNIT_MAP_VERTICES) <- UNIT_MAP_VERTICES[,"LEARNING_STANDARD_ID"]
	rownames(UNIT_MAP_VERTEX_STATES) <- UNIT_MAP_VERTEX_STATES[,"LEARNING_STANDARD_ID"]
	for (col_idx in c("x_coord","y_coord","vert_size","vert_label_size")) UNIT_MAP_VERTICES[,col_idx] <- as.numeric(UNIT_MAP_VERTICES[,col_idx])
	for (col_idx in KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"]) UNIT_MAP_VERTEX_STATES[,col_idx] <- as.numeric(UNIT_MAP_VERTEX_STATES[,col_idx])
#
# Construct the Graph. 
	UNIT_MAP <- graph_from_data_frame(d  = UNIT_MAP_EDGES[c("LEARNING_STANDARD_ID_FROM",
																												"LEARNING_STANDARD_ID_TO")],
																	directed = TRUE,
																	vertices = sort(unique(unlist(UNIT_MAP_EDGES[c("LEARNING_STANDARD_ID_FROM",
																																							"LEARNING_STANDARD_ID_TO")]))))
#
# Reorder the vertices in UNIT_MAP_VERTICES to correspond to that in the vertices in UNIT_MAP.  Also add an attribute indicating whether 
# the vertex is instantiated with direct evidence. Detect this by detecting whether any of the IMPLIED_KNOW_STATE attributes have values of
# unity.
	UNIT_MAP_VERTICES <- UNIT_MAP_VERTICES[match(x =  names(V(UNIT_MAP)[[]]), 
																				table =UNIT_MAP_VERTICES[,"LEARNING_STANDARD_ID"]),]
	UNIT_MAP_VERTEX_STATES <- UNIT_MAP_VERTEX_STATES[match(x =  names(V(UNIT_MAP)[[]]), 
																													table = UNIT_MAP_VERTEX_STATES[,"LEARNING_STANDARD_ID"]),]
#  Produce plot-legend attributes.  Extract attributes from COURSE_UNIT and COURSE_ENROLL.
	ANALYSIS_CASE <- data.frame(rbind(USE_CASE_ATTRIBUTES[c("SCHOOL_DISTRICT","COURSE_ID","COURSE_TITLE",
																											"UNIT_ID","DATE_LATEST_MEAS"),"VALUE"]))
	colnames(ANALYSIS_CASE) <- c("SCHOOL_DISTRICT","COURSE_ID","COURSE_TITLE",
														"UNIT_ID","DATE_LATEST_MEAS")
	ANALYSIS_CASE <- unique(merge(x = ANALYSIS_CASE,
															y = COURSE_UNIT))
	ANALYSIS_CASE <- ANALYSIS_CASE[c("SCHOOL_DISTRICT","COURSE_ID","COURSE_TITLE",
														"UNIT_ID","DATE_LATEST_MEAS")]
														
	ANALYSIS_CASE["UNIT_TITLE"] <- COURSE_UNIT[COURSE_UNIT[,"UNIT_ID"] == ANALYSIS_CASE[,"UNIT_ID"],"UNIT_TITLE"][1]
	ANALYSIS_CASE <- ANALYSIS_CASE[c("SCHOOL_DISTRICT","COURSE_ID","COURSE_TITLE",
																	"UNIT_TITLE","UNIT_ID","DATE_LATEST_MEAS")]													
#
	SUBJ_CASE <- data.frame(rbind(USE_CASE_ATTRIBUTES[c("STUDENT_ID","COURSE_ID","CLASS_ID"),"VALUE"]))
	colnames(SUBJ_CASE) <- c("STUDENT_ID","COURSE_ID","CLASS_ID")
	if (SUBJ_CASE[,"STUDENT_ID"] == "ALL"){
		SUBJ_CASE <- unique(merge(x = SUBJ_CASE[c("COURSE_ID","CLASS_ID")],
											y = COURSE_ENROLL[setdiff(colnames(COURSE_ENROLL),c("STUDENT_ID","STUDENT_NAME"))]))
	} else {
		SUBJ_CASE <- unique(merge(x = SUBJ_CASE,
											y = COURSE_ENROLL))
	}
#
#   Transform the USE_CASE, and ANALYSIS_CASE into a vertical data frame with attributes and values.
	LEGEND_ATTRIBUTES <- data.frame(ATTRIBUTE = colnames(ANALYSIS_CASE),
																VALUE  = unlist(ANALYSIS_CASE))
	LEGEND_ATTRIBUTES <- rbind(LEGEND_ATTRIBUTES,
														data.frame(ATTRIBUTE = colnames(SUBJ_CASE),
																	VALUE  = unlist(SUBJ_CASE)))
	LEGEND_ATTRIBUTES <- LEGEND_ATTRIBUTES[!duplicated(LEGEND_ATTRIBUTES[,"VALUE"]),]
	if(USE_CASE_ATTRIBUTES["STUDENT_ID","VALUE"] == "ALL") {
		LEGEND_ATTRIBUTES["STUDENT_NAME","VALUE"] <- "Class Average"
		LEGEND_ATTRIBUTES["STUDENT_NAME","ATTRIBUTE"] <- "STUDENT_NAME"
	} else {
	LEGEND_ATTRIBUTES <- LEGEND_ATTRIBUTES[-grep(x = LEGEND_ATTRIBUTES[,"ATTRIBUTE"],
																							pattern = "STUDENT_ID"),]
	}
#
#  We want a logical variable to conditionally specify the scale of plot points appearing as "halos" behind vertices 
#  to which evidence has been instantiated. First, we only the halos for measured vertices. Second, we want "in-scope"
#  vertices for the unit to be larger than "out-of-scope. These logical variables are inconveniently in different data frames,
#  right now. The MEASURED flag attribute appears in UNIT_MAP_VERTEX_STATES. The IN_UNIT flag is in UNIT_MAP_VERTICES.
# Merge selected columns to get these values together.
	halo_dim_flag <- merge(x = UNIT_MAP_VERTICES[c("LEARNING_STANDARD_ID","IN_UNIT")],
											y = UNIT_MAP_VERTEX_STATES[c("LEARNING_STANDARD_ID","MEASURED")])
	rownames(halo_dim_flag) <- halo_dim_flag[,"LEARNING_STANDARD_ID"]
	halo_dim_flag["LEARNING_STANDARD_ID"] <- NULL
#
# Now apply the selection and distinction logic.  Only retain halo_dim_flag records for that are measured, and distinguish among those
# between IN_UNIT and not.    ifelse(test = halo_dim_flag.test, yes = 18, no = 13)
	for (col_idx in colnames(halo_dim_flag)) halo_dim_flag[,col_idx] <- as.logical(halo_dim_flag[,col_idx])
	halo_dim_flag.test <- apply(X = halo_dim_flag[halo_dim_flag[,"MEASURED"],],
												MARGIN = 1,
												FUN = all)
#
#  Plot the graph.
	png(filename = paste(proto.dir,"UNIT_SUBGRAPH_PLOT.png", sep = "/"),
							width=4000,height=3500,pointsize=36,bg= bg.col)
#
	plot(y = c(-1.1,1.4), 
			x = c(-1.4,1.1),
			type="n",axes=TRUE,ann=FALSE)

	points(x = UNIT_MAP_VERTICES[UNIT_MAP_VERTEX_STATES[UNIT_MAP_VERTEX_STATES[,"MEASURED"],
																											"LEARNING_STANDARD_ID"],"x_coord"],
					y =  UNIT_MAP_VERTICES[UNIT_MAP_VERTEX_STATES[UNIT_MAP_VERTEX_STATES[,"MEASURED"],
																											"LEARNING_STANDARD_ID"],"y_coord"],
					pch = 19,
					col ="#EE82EE80",
					cex = ifelse(test = halo_dim_flag.test, 
										yes = 17, 
										no = 11))
	plot.igraph(x = UNIT_MAP,
						layout =as.matrix(UNIT_MAP_VERTICES[,c("x_coord","y_coord")]),
						vertex.size = UNIT_MAP_VERTICES[,"vert_size"],
						vertex.shape = "pie",
						vertex.pie = lapply(X = split(x = UNIT_MAP_VERTEX_STATES[KNOW_STATE_SPEC[,"IMPLIED_KNOW_STATE"]],
																	f = seq(nrow(UNIT_MAP_VERTEX_STATES))),
													FUN = unlist),
						vertex.pie.color = list(KNOW_STATE_SPEC[,"PLOT_COLOR"]),
						vertex.label.cex = UNIT_MAP_VERTICES[,"vert_label_size"],
						vertex.label = UNIT_MAP_VERTICES[,"LEARNING_STANDARD_CD"],
						vertex.label.color = "chocolate1",
						vertex.label.font = 2,
						vertex.label.family = "avenir",
						edge.width = 1.25,
						edge.arrow.size = .75,
						edge.color = ibm.blue,
						add = TRUE,
						rescale = FALSE)
	box(col= ibm.blue,lwd=2,which="plot")

#
# Add the legend
	for (row_idx in rownames(LEGEND_ATTRIBUTES)){							## row_idx <- rownames(LEGEND_ATTRIBUTES)[1]
		text(labels = paste(LEGEND_ATTRIBUTES[row_idx,"ATTRIBUTE"],
										": ",
										LEGEND_ATTRIBUTES[row_idx,"VALUE"],
										sep = ""),
				x = -1.4,
				y = 1.4 - 0.065*(grep(pattern = row_idx,
											x = rownames(LEGEND_ATTRIBUTES)) - 1),
				adj = 0,
				col = ibm.blue,
				cex = 1.5)
	}
#
	dev.off()
#
