SELECT DISTINCT config.DATABASE_SERVER_IP, config.DATABASE_PORT,
		config.DATABASE_NAME, config.USER_NAME, config.PASSWORD, config.TENANT_ID
FROM IBMSIH.TENANT_CONFIG config
WHERE config.TENANT_ID NOT IN  ('T001', 'T002')
	AND config.TENANT_ID NOT LIKE 'AT%'
	AND config.TENANT_ID NOT LIKE 'TR%'