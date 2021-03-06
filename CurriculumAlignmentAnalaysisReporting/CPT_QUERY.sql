SELECT cpt.CONSTITUENT_COUNT, cpt.CPT_CELL_IDX, cpt.MEAS, cpt.IS_ROOT
FROM IBMSIH.CPT_LONG cpt
WHERE cpt.TENANT_ID = :tenant_id
	AND cpt.CONSTITUENT_COUNT <= 8
ORDER BY cpt.CONSTITUENT_COUNT, CPT.CPT_CELL_IDX
