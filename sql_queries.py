
GET_SITES = 'SELECT * FROM sites'
GET_SITENAMES = 'SELECT site_name FROM sites'
GET_SITE_IDS = 'SELECT site_id FROM sites'


GET_ALL_JOB_IDS_BY_STATE = 'SELECT job_id FROM jobs WHERE job_state = %s AND run_id = %s'

GET_SITE_IDS_BY_PROCESSING_STATE = 'SELECT site_id FROM processing_state WHERE (state=%s) AND (run_id=%s)'

GET_LAST_RUN_ID = 'SELECT run_id FROM run ORDER BY run_id DESC LIMIT 1'
CHECK_RUN_STATE = 'SELECT state FROM run WHERE run_id=%s'

