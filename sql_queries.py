
GET_SITES = 'SELECT * FROM sites'
GET_SITENAMES = 'SELECT site_name FROM sites'
GET_SITE_IDS = 'SELECT site_id FROM sites'


GET_ALL_JOB_IDS_BY_SITE_AND_STATE = 'SELECT job_id FROM jobs WHERE site_id = %s AND run_id = %s AND job_state=%s'


GET_SITE_IDS_BY_PROCESSING_STATE = 'SELECT site_id FROM processing_state WHERE (state=%s) AND (run_id=%s)'

GET_LAST_RUN_DATA = 'SELECT * FROM run WHERE (state="COMPLETED" or state="TIMED_OUT") ORDER BY run_id DESC LIMIT 1'
GET_LAST_RUN_ID = 'SELECT run_id FROM run WHERE (state="COMPLETED" or state="TIMED_OUT") ORDER BY run_id DESC LIMIT 1'
CHECK_RUN_STATE = 'SELECT state FROM run WHERE run_id=%s'

GET_PARAM = 'SELECT * FROM parameters WHERE (run_id=%s) AND (site_id=%s) AND (paramName=%s) AND (paramValue=%s)'
GET_MULTI_PARAM = 'SELECT DISTINCT job_id FROM parameters WHERE (run_id=%s) AND (site_id=%s) AND ({})'
GET_PARAMS_BY_JOB_ID = 'SELECT paramName,paramValue FROM parameters WHERE job_id=%s'