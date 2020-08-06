
GET_SITES = 'SELECT * FROM sites'
GET_ALL_SITENAMES = 'SELECT site_name FROM sites'
GET_SITENAME_BY_SITE_ID = 'SELECT site_name FROM sites where site_id=%s'
GET_SITE_IDS = 'SELECT site_id FROM sites'
GET_NUM_NODES_BY_SITE_ID = 'SELECT num_nodes FROM sites where site_id=%s'

GET_NODENAME_BY_JOB_ID = 'SELECT node_name FROM nodes where job_id=%s'


GET_ALL_JOB_IDS_OF_COVERED_NODES = 'SELECT job_id FROM nodes WHERE site_id = %s AND run_id = %s'


GET_SITE_IDS_BY_PROCESSING_STATE = 'SELECT site_id FROM processing_state WHERE (state=%s) AND (run_id=%s)'

GET_LAST_RUN_DATA = 'SELECT * FROM run WHERE (state="COMPLETED" or state="TIMED_OUT") ORDER BY run_id DESC LIMIT 1'
GET_ALL_RUNS_DATA = 'SELECT * FROM run WHERE (state="COMPLETED" or state="TIMED_OUT") ORDER BY run_id DESC'
GET_LAST_RUN_ID = 'SELECT run_id FROM run WHERE (state="COMPLETED" or state="TIMED_OUT") ORDER BY run_id DESC LIMIT 1'
CHECK_RUN_STATE = 'SELECT state FROM run WHERE run_id=%s'

GET_PARAM = 'SELECT * FROM parameters WHERE (run_id=%s) AND (site_id=%s) AND (paramName=%s) AND (paramValue=%s)'
GET_PARAMS_BY_JOB_ID = 'SELECT paramName,paramValue FROM parameters WHERE job_id=%s'

GET_SEARCH_KEYS = 'SELECT key_list FROM job_keys WHERE run_id=%s'