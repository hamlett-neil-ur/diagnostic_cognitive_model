## PURPOSE:  "Reverse-Engineer" the gRain-package cptable function, which creates conditional-probability tables. We calculate 
## conditional-probability distributions heuristically. These are employed in a Bayesian-network calculation. The cptable function takes an input-vector
## of conditional probabilites for each vertex in the graph.  It reshapes this vector into an array of dimensionality commensurate with the valency of
## the corresponding graph vertex.  
##
## We construct our CPTs recursively. The CPTs themselves result from applying a Gaussian-weighting function to the square root of the Brier Alethic Accuracy function
## of the distance between the observed and target vertex states.  For this exercise we simply want to see the vertex states themselves. We want to observe the order
## in which cptable distributes conditional-probability values into the conditional-probability arrays.  Our conditional-probabilities in this state will be the indices 
## corresponding to the vertex states. 
##
## To proceed, perform the following steps.
## ① Define the Bayesian ntework.  The network is specified using the vpar argument. This argument specifies the conditional-probability
##     relationships for each vertex.
## ② Construct conditional-probability factors for each vertex.  Our contrived graph contains four in-degree valencies:  zero, one, two, and three.
##     Our vertices assume of three vertex states, indicated by the integers of one, two, or three.  Replicate the algorithmic logic for intended for the
##     HEURISTIC_CPT subroutine.  First construct a set of wide-table CPDs. Then reshape them.
## ③ Apply cptable to the results and inspect the resulting conditional tables.  Look for expected distribution of conditional probabilities
##    across the arrays.
#
# ⓪ INITIALIZE THE ENVIRONMENT. 
	options(stringsAsFactors = FALSE)
	options(java.parameters = "-Xmx16g")
	library(stringr)
	library(reshape2)
	library(gRain)
#
# ① Define the Bayesian ntework.  Define the conditional-probability relations as a list stored as a column in a data frame. 
#      Use the syntax as illlustrated in [Hǿjsgaard2012].  Also incorporate a VERT_CPT_INDEX indicating the in-degree valency
#       of the vertex correspond to each row.
	COND_PROB_RELATIONS <- data.frame(COND_PROB_RELATIONS = c( A = "~A",
																															B = "~B",
																															C = "~ C | A + B",
																															D = "~ D",
																															E = "~ E",
																															F = "~ F | C + D + E",
																															G = "~ G | F"),
																		VERT_CPT_BRIDGE_IDX = paste("IN_DEG",
																															c(0,0,2,0,0,3,1),
																															sep = "_"))
#	
# ② Construct conditional-probability factors for each vertex.  	Create two initial single-column data frames 
	HEUR_CPT <- list()
	CPT <- data.frame(Targ_Var = 1:3)
	Obs_Var <- data.frame( Obs_Var = 1:3)
#
#     Recursively build out a set of pseudo-CPT tables corresponding to the in-degree valencies of vertices in the graph. 
#     Root-edge vertices have in-degree valencies of zero.  Built out wide-table CPTs first. Our recursion logic produces the
#     (val_idx + 1)ᵗʰ table entry by merging — cartesian product — of Obs_Var with the val_idxᵗʰ wide table.
	HEUR_CPT[["IN_DEG_0"]] <- CPT
	HEUR_CPT[["IN_DEG_0"]]["CPT"] <- HEUR_CPT[["IN_DEG_0"]]
#
	for (val_idx in 1:3){  										## val_idx <- 1
	# Evaluate the cartesion product using merge function.
		addr.val_idx <- paste("IN_DEG",val_idx - 1,sep = "_")
		HEUR_CPT.val_idx <- merge(x = HEUR_CPT[[addr.val_idx]][head(colnames(HEUR_CPT[[addr.val_idx]]),-1)],
														y = Obs_Var)
	#
	# Concatenate val_idx onto the column name "Obs_Var" of the newly introduced conditional-attribute state vector.
		colnames(HEUR_CPT.val_idx)[ncol(HEUR_CPT.val_idx)] <- paste(tail(names(HEUR_CPT.val_idx),1), val_idx, sep = "_")
	#
	# Synthesize the conditional probabilities. Concatenate all of the attributes into a single column.
		HEUR_CPT.val_idx["CPT"] <- as.numeric(apply(X = HEUR_CPT.val_idx,
																						MARGIN = 1,
																						FUN = paste,
																						collapse = ""))
	#
	# Store HEUR_CPT.val_idx as the val_idxᵗʰ element of HEUR_CPT.
		HEUR_CPT[[paste("IN_DEG",val_idx,sep = "_")]] <- HEUR_CPT.val_idx
	#
	}
#
#     Assign a to COND_PROB_RELATIONS a CPT column corresonding to the members of list HEUR_CPT corresponding to the VERT_CPT_BRIDGE_IDX
	COND_PROB_RELATIONS[["CPT"]] <- setNames(object  = lapply(X = HEUR_CPT[COND_PROB_RELATIONS[,"VERT_CPT_BRIDGE_IDX"]], 
																												FUN = "[",
																												"CPT"),
																					nm = rownames(COND_PROB_RELATIONS))
#
# ③ Apply the columns of COND_PROB_RELATIONS to cptable to construct conditional-probability tables, CP_TABLE.
	CP_TABLE <- list()
	for (row_idx in rownames(COND_PROB_RELATIONS)) CP_TABLE[[row_idx]] <- cptable(vpar = as.formula(COND_PROB_RELATIONS[row_idx,
																																																		"COND_PROB_RELATIONS"]),
																																			values = unlist(COND_PROB_RELATIONS[["CPT"]][row_idx]),
																																			levels = unlist(CPT),
																																			normalize = FALSE)
#
# ④ CONSTRUCT MVN-BASED HERUSTIC CPT.  Apply a procedure similar to that above to construct an MVN_CPT, heuristic CPTs
#     in which a Gaussian "weighting" fucntion is applied to the root-BAC function.  We construct the same index scheme through
#     recursively merging the (val_idx-1)ᵗʰ list element with Obs_Var. Instead of synthesizing the CPT attribute through concatenation,
#     we calculate the root-BAC with respect to the Targ_Var column.
#
#    Initialize the IN_DEG_0 member with CPT values of unity.
	MVN_CPT <- list()
	MVN_CPT[["IN_DEG_0"]] <- CPT
	MVN_CPT[["IN_DEG_0"]]["CPT"] <- 1/nrow(CPT)
#
#    Now, begin the recursion.
	for (val_idx in 1:3){  										## val_idx <- 1
	# Evaluate the cartesion product using merge function.
		addr.val_idx <- paste("IN_DEG",val_idx - 1,sep = "_")
		MVN_CPT.val_idx <- merge(x = MVN_CPT[[addr.val_idx]][head(colnames(MVN_CPT[[addr.val_idx]]),-1)],
														y = Obs_Var)
	#
	# Concatenate val_idx onto the column name "Obs_Var" of the newly introduced conditional-attribute state vector.
		colnames(MVN_CPT.val_idx)[ncol(MVN_CPT.val_idx)] <- paste(tail(names(MVN_CPT.val_idx),1), val_idx, sep = "_")
	#
	# Now calculate the 𝕃²-Norm, the root-BAC.  
		MVN_CPT.val_idx["L2_NORM"] <- 0
		Obs_Var.val_idx <- colnames(MVN_CPT.val_idx)[grep(x = colnames(MVN_CPT.val_idx),
																								pattern = "Obs_Var")]
		for (col_idx in Obs_Var.val_idx) MVN_CPT.val_idx["L2_NORM"] <- MVN_CPT.val_idx["L2_NORM"] +
																																					(MVN_CPT.val_idx["Targ_Var"] -
																																						MVN_CPT.val_idx[col_idx])^2
		MVN_CPT.val_idx["L2_NORM"] <- sqrt(MVN_CPT.val_idx["L2_NORM"])
	#
	# Calcualte CPT by applying a Gaussian-weigting function to L2_NORM.
		MVN_CPT.val_idx["CPT"] <- pnorm(q = MVN_CPT.val_idx[,"L2_NORM"],
																	mean = 0,
																	sd = 1,
																	lower.tail = FALSE)
	#
	# Some careful thought must now be applied to the normalization of CPT.  We have 3^val_idx observed-variable configurations.
	# We want to normalize with each independently.  We sum-aggregate CPT with respect to the Obs_Var states. Calculate the
	# reciprocal of the result. Join onto MVN_CPT.val_idx by the Obs_Var attributes and multiply the result by CPT.  Since preserving
	# the row order of MVN_CPT.val_idx is important, we introduce a row_idx column by which to reorder after the merge.
		CPT_NORM.val_idx <- aggregate(formula = as.formula(paste("CPT",
																												paste(colnames(MVN_CPT.val_idx)[grep(x = colnames(MVN_CPT.val_idx),
																																											pattern = "Obs_Var")],
																															collapse = " + "),
																													sep = " ~ "  ) ),
																	data = MVN_CPT.val_idx[c("CPT",
																													colnames(MVN_CPT.val_idx)[grep(x = colnames(MVN_CPT.val_idx),
																																											pattern = "Obs_Var")])],
																	FUN = sum)
		CPT_NORM.val_idx["recip_CPT"] <- 1/CPT_NORM.val_idx[,"CPT"]
		CPT_NORM.val_idx["CPT"] <- NULL
		MVN_CPT.val_idx["row_idx"] <- 1:nrow(MVN_CPT.val_idx)
		MVN_CPT.val_idx <- merge(x = MVN_CPT.val_idx,
														y = CPT_NORM.val_idx,
														sort = T)
		MVN_CPT.val_idx <- MVN_CPT.val_idx[order(MVN_CPT.val_idx[,"row_idx"]),]
		MVN_CPT.val_idx["CPT"] <- apply(X = MVN_CPT.val_idx[c("CPT","recip_CPT")],
																	MARGIN = 1,
																	FUN = prod)
		Obs_Var.val_idx <- colnames(MVN_CPT.val_idx)[grep(x = colnames(MVN_CPT.val_idx),
																								pattern = "Obs_Var")]
		MVN_CPT.val_idx <- MVN_CPT.val_idx[c("Targ_Var",sort(Obs_Var.val_idx),"CPT")]
	#
	# Now, clean up. Get rid of unwanted columns. Reorder those that remain.  Store the result as the  val_idxᵗʰ element of MVN_CPT.
		for (col_idx in c("recip_CPT","row_idx","L2_NORM")) MVN_CPT.val_idx[col_idx] <- NULL
		MVN_CPT[[paste("IN_DEG",val_idx,sep = "_")]] <- MVN_CPT.val_idx
	#
	}
#
#     Build now MVN tables as done above.  Visually inspect.
	COND_PROB_RELATIONS[["MVN"]] <- setNames(object  = lapply(X = MVN_CPT[COND_PROB_RELATIONS[,"VERT_CPT_BRIDGE_IDX"]], 
																												FUN = "[",
																												"CPT"),
																					nm = rownames(COND_PROB_RELATIONS))
	MVN_TABLE <- list()
	for (row_idx in rownames(COND_PROB_RELATIONS)) MVN_TABLE[[row_idx]] <- cptable(vpar = as.formula(COND_PROB_RELATIONS[row_idx,
																																																		"COND_PROB_RELATIONS"]),
																																			values = unlist(COND_PROB_RELATIONS[["MVN"]][row_idx]),
																																			levels = unlist(CPT),
																																			normalize = FALSE)
#	
	
	
#⧻⧺+⧺⧻⧺+⧺⧻⧺+⧺⧻⧺+⧺⧻⧺+⧺⧻⧺+⧺⧻⧺+⧺⧻⧺+⧺⧻⧺+⧺⧻⧺+⧺⧻⧺+⧺⧻⧺+⧺⧻⧺+⧺⧻⧺+⧺⧻⧺+⧺⧻⧺+⧺⧻⧺+⧺⧻⧺+⧺⧻⧺+⧺⧻⧺+⧺⧻⧺+⧺⧻⧺+⧺⧻⧺+⧺
# REFERENCES.
# [Hǿjsgaard2012], S. Hǿjsgaard2012, "Graphical Independence Networks with the gRain Package for R"," Journal of Statistical Software, March 2012.
# 
#
