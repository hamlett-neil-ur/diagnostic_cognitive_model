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
	UNIT_MAP_EDGE_LIST <- read.csv(file = paste(PROF_TASK.dir, "UNIT_MAP_EDGE_LIST.csv", sep = "/"),
																colClasses = "character") 
	VERTEX_LAYOUT <- read.csv(file = paste(PROF_TASK.dir, "UNIT_MAP_VERTICES.csv", sep = "/"),
													colClasses = "character") 
	rownames(VERTEX_LAYOUT) <- VERTEX_LAYOUT[,"LEARNING_STANDARD_ID"]
	for (col_idx in c("x_coord","y_coord","vert_size","vert_label_size")) VERTEX_LAYOUT[,col_idx] <- as.numeric(VERTEX_LAYOUT[,col_idx])
#
# Construct the Graph. 
	UNIT_MAP <- graph_from_data_frame(d  = UNIT_MAP_EDGE_LIST[c("LEARNING_STANDARD_ID_FROM",
																													"LEARNING_STANDARD_ID_TO")],
																	directed = TRUE,
																	vertices = sort(unique(unlist(UNIT_MAP_EDGE_LIST[c("LEARNING_STANDARD_ID_FROM",
																																							"LEARNING_STANDARD_ID_TO")]))))
#
# Reorder the vertices in VERTEX_LAYOUT to correspond to that in the vertices in UNIT_MAP.
	VERTEX_LAYOUT <- VERTEX_LAYOUT[match(x =  names(V(UNIT_MAP)[[]]), 
																				table =VERTEX_LAYOUT[,"LEARNING_STANDARD_ID"]),]
#
	png(filename = paste(unique(USE_CASE_ATTRIBUTES["COURSE_TITLE","VALUE"]),
										"UNIT", 
										USE_CASE_ATTRIBUTES["UNIT_ID","VALUE"],
										"SUBGRAPH.png",
										sep = "_"),
							width=4000,height=3500,pointsize=36,bg= bg.col)
	plot(y = c(-1.1,1.4), 
			x = c(-1.4,1.1),
			type="n",axes=TRUE,ann=FALSE)

#
	plot.igraph(x = UNIT_MAP,
						layout =as.matrix(VERTEX_LAYOUT[,c("x_coord","y_coord")]),
						vertex.size = VERTEX_LAYOUT[,"vert_size"],
						vertex.label.cex = VERTEX_LAYOUT[,"vert_label_size"],
						vertex.label = VERTEX_LAYOUT[,"LEARNING_STANDARD_CD"],
						vertex.label.font = 2,
						vertex.label.family = "avenir",
						edge.width = 1.25,
						edge.arrow.size = .75,
						edge.color = ibm.blue,
						add = TRUE,
						rescale = FALSE)
#
	box(col= ibm.blue,lwd=2,which="plot")
#
	dev.off()
	png(filename = paste(unique(USE_CASE_ATTRIBUTES["COURSE_TITLE","VALUE"]),
										"UNIT", 
										USE_CASE_ATTRIBUTES["UNIT_ID","VALUE"],"(alt layout)",
										"SUBGRAPH.png",
										sep = "_"),
							width=4000,height=3500,pointsize=36,bg= bg.col)
#
	plot(y = c(-1.1,1.4), 
			x = c(-1.4,1.1),
			type="n",axes=TRUE,ann=FALSE)


	plot.igraph(x = UNIT_MAP,
						layout =layout.fruchterman.reingold(graph = UNIT_MAP,niter = 10000),
						vertex.size = VERTEX_LAYOUT[,"vert_size"],
						vertex.label.cex = VERTEX_LAYOUT[,"vert_label_size"],
						vertex.label = VERTEX_LAYOUT[,"LEARNING_STANDARD_CD"],
						vertex.label.font = 2,
						vertex.label.family = "avenir",
						edge.width = 1.25,
						edge.arrow.size = .75,
						edge.color = ibm.blue,
						add = TRUE)
#
	box(col= ibm.blue,lwd=2,which="plot")
#
	dev.off()
