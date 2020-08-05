import csv 
import datetime
import time
import logging
import os

from config import DB_HOST,DB_USER,DB_PWD,DB_DATABASE
from sql_queries import *

import mysql.connector

# Utils
def normalize_ce_name(target_ce):
    return target_ce.replace("::", "_").lower()[len("alice_"):]

# Connection Functions

def get_connection(auto_commit=True):
    try:
        connection = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PWD,
        database=DB_DATABASE,
        autocommit=auto_commit
        )
        cursor = connection.cursor()
        return cursor,connection
    except mysql.connector.Error as error:
        logging.error("Error while connecting to MySQL", error)

# def get_last_run_data():
#     cursor, conn = get_connection()
#     try:
#         cursor.execute(GET_LAST_RUN_DATA)
#         run = cursor.fetchone()
#         run = {
#             'run_id' : run[0],
#             'started_at' : run[1],
#             'finished_at' : run[2],
#             'state' : run[3]
#         }
#         return run
#     except mysql.connector.Error as error:
#         logging.error("Failed to get last run data: {}".format(error))
#     finally:
#         if(conn.is_connected()):
#             cursor.close()
#             conn.close()

def get_all_runs_data():
    cursor, conn = get_connection()
    try:
        cursor.execute(GET_ALL_RUNS_DATA)
        results = cursor.fetchall()
        print (results)
        last_run = {
            'run_id' : results[0][0],
            'started_at' : results[0][1],
            'finished_at' : results[0][2],
            'state' : results[0][3]
        }
        run_data = []
        for row in results:
            run = {
                'run_id' : row[0],
                'started_at' : row[1],
                'finished_at' : row[2],
                'state' : row[3]
            }
            run_data.append(run)
        all_run_data = {
            'selected_run' : last_run,
            'all_runs' : run_data
        }
        return all_run_data
    except mysql.connector.Error as error:
        logging.error("Failed to get all runs data: {}".format(error))
    finally:
        if(conn.is_connected()):
            cursor.close()
            conn.close()


# def get_last_run_id():
#     cursor, conn = get_connection()
#     try:
#         cursor.execute(GET_LAST_RUN_ID)
#         run_id = cursor.fetchone()
#         return run_id[0]
#     except mysql.connector.Error as error:
#         logging.error("Failed to get last run id: {}".format(error))
#     finally:
#         if(conn.is_connected()):
#             cursor.close()
#             conn.close()


def check_run_exists(run_id):
    cursor,conn = get_connection()
    flag = True
    try:
        cursor.execute(CHECK_RUN_STATE,[run_id])
        state = cursor.fetchone()[0]
        if state == 'STARTED':
            logging.debug('Last run is still running')
            flag = True
        else:
            logging.debug('No currently executing runs')
            flag = False
    except mysql.connector.Error as error:
        logging.error("Failed to increment run id: {}".format(error))
        flag = False
    finally:
        if(conn.is_connected()):
            cursor.close()
            conn.close()
        return flag


def get_search_keys(run_id):
    cursor, conn = get_connection()
    try:
        cursor.execute(GET_SEARCH_KEYS,[run_id])
        results = cursor.fetchone()
        return results[0]
    except mysql.connector.Error as error:
            logging.debug("Failed to get search keys: {}".format(error))
    finally:
        if(conn.is_connected()):
            cursor.close()
            conn.close()


# def get_all_jobs_by_site(run_id,site_id):
#     cursor,conn = get_connection()
#     try:
#         cursor.execute(GET_ALL_JOBS_COUNT_BY_SITE,[site_id,run_id])
#         results = cursor.fetchone()
#         return results[0]
#     except mysql.connector.Error as error:
#         logging.error("Failed to increment run id: {}".format(error))
#     finally:
#         if(conn.is_connected()):
#             cursor.close()
#             conn.close()

def get_num_nodes_by_site(site_id):
    try:
        cursor, conn = get_connection()
        cursor.execute(GET_NUM_NODES_BY_SITE_ID, [site_id])
        result = cursor.fetchone()
        return result[0]
    except mysql.connector.Error as error:
        logging.error("Failed to get number of nodes by site id: {}".format(error))
    finally:
        if (conn.is_connected()):
            cursor.close()
            conn.close()


def get_nodename_by_job_id(job_id):
    try:
        cursor, conn = get_connection()
        cursor.execute(GET_NODENAME_BY_JOB_ID, [job_id])
        result = cursor.fetchone()
        return result[0]
    except mysql.connector.Error as error:
        logging.error("Failed to complete multi parameter search: {}".format(error))
    finally:
        if (conn.is_connected()):
            cursor.close()
            conn.close()


# def get_job_ids_of_site_and_state(run_id,site_id,state):
#     try:
#         cursor, conn = get_connection()
#         cursor.execute(GET_ALL_JOB_IDS_BY_SITE_AND_STATE, [site_id,run_id,state])
#         result = cursor.fetchall()
#         job_ids = []
#         for row in result:
#             job_ids.append(row[0])
#         return job_ids
#     except mysql.connector.Error as error:
#         logging.error("Failed to complete multi parameter search: {}".format(error))
#     finally:
#         if (conn.is_connected()):
#             cursor.close()
#             conn.close()

def get_job_ids_of_covered_nodes(run_id,site_id):
    try:
        cursor, conn = get_connection()
        cursor.execute(GET_ALL_JOB_IDS_OF_COVERED_NODES, [site_id,run_id])
        result = cursor.fetchall()
        job_ids = []
        for row in result:
            job_ids.append(row[0])
        return job_ids
    except mysql.connector.Error as error:
        logging.error("Failed to complete multi parameter search: {}".format(error))
    finally:
        if (conn.is_connected()):
            cursor.close()
            conn.close()


def get_job_params(job_id):
    try:
        cursor, conn = get_connection()
        cursor.execute(GET_PARAMS_BY_JOB_ID, [job_id])
        result = cursor.fetchall()
        params = {}
        for row in result:
            params.update({ row[0] : row[1] })
        return params
    except mysql.connector.Error as error:
        logging.error("Failed to get parameters by Job Id: {}".format(error))
    finally:
        if (conn.is_connected()):
            cursor.close()
            conn.close()

def check_key_val_exists_in_dict(key,val,dict):
    if key in dict.keys() and val == dict[key]:
        return True
    else:
        return False


def full_search_site(site_id,queries,equation,run_id):
    # Equation eg:-
    # in    - "A & (B | ~ (C))"
    # out   - "True & (True | ~ (False))""
    if type(site_id) == str:
        site_id = int(site_id)
    job_ids = get_job_ids_of_covered_nodes(run_id,site_id)
    #submitted_jobs = get_all_jobs_by_site(run_id,site_id)
    total_nodes = get_num_nodes_by_site(site_id)
    covered_nodes = len(job_ids)
    matching_nodes = 0
    equation = equation.replace("&"," and ").replace("|"," or ").replace("~"," not ")
    matching_job_data = {}
    unmatching_job_data = {}
    for job in job_ids:
        local_equation = equation
        params = get_job_params(job)
        nodename = get_nodename_by_job_id(job)
        #nodename = "node_" + str(job)
        for variable_key in queries:
            is_exists = check_key_val_exists_in_dict (queries[variable_key]['query_key'],queries[variable_key]['query_value'],params)
            if is_exists:
                local_equation = local_equation.replace(variable_key,'True')
            else:
                local_equation = local_equation.replace(variable_key, 'False')
        if (eval(local_equation) is True):
            matching_nodes += 1
            matching_job_data.update({nodename: params})
            print(matching_job_data)
            print (len(matching_job_data))
        else:
            unmatching_job_data.update({nodename: params})
        print (local_equation, eval(local_equation))
    return total_nodes, matching_nodes, covered_nodes, matching_job_data, unmatching_job_data

# def all_site_search(queries,equation):
#     run_id = get_last_run_id()
#     sites = get_sites_by_processing_state('COMPLETED')
#     matching_sites = 0
#     total_sites = len(sites)
#     total_covered_nodes = 0
#     total_matching_nodes = 0
#     for site_id in sites:
#         submitted_jobs, matching_jobs, completed_jobs = full_search_site(site_id,queries,equation)
#         if matching_jobs/completed_jobs > 0.5:
#             matching_sites += 1
#         total_covered_nodes += completed_jobs

# Site Related Functions
def get_sites():
    """
    Get all Grid sites

    Returns:
        sites(dict): All site data
    """
    cursor, conn = get_connection()
    try:
        cursor.execute(GET_SITES)
        results = cursor.fetchall()
        sites = []
        for row in results:
            site_id = row[0]
            site_name = row[1]
            normalized_name = row[2]
            num_nodes = row[3]
            last_update = row[4]
            site = {
                'site_id': site_id,
                'site_name': site_name,
                'normalized_name': normalized_name,
                'num_nodes': num_nodes,
                'last_update': last_update
                }
            sites.append(site)
        return sites
    except mysql.connector.Error as error:
            logging.error("Failed to retrieve sites: {}".format(error))
    finally:
        if(conn.is_connected()):
            cursor.close()
            conn.close()

# def get_site_ids():
#     """
#     Get Site IDs
#
#     Returns:
#         Site IDs(list)
#     """
#     cursor, conn = get_connection()
#     try:
#         cursor.execute(GET_SITE_IDS)
#         results = cursor.fetchall()
#         ids = []
#         for row in results:
#             site_id = row[0]
#             ids.append(site_id)
#         return ids
#     except mysql.connector.Error as error:
#             logging.debug("Failed to get site ids: {}".format(error))
#     finally:
#         if(conn.is_connected()):
#             cursor.close()
#             conn.close()

# def add_sites_from_csv(csv_filename):
#     """
#     Add Grid sites from CSV file
#
#     Args:
#         csv_filename (str): Name of the CSV file (Format: sitename,num_nodes)
#     """
#
#     cursor,conn = get_connection()
#     site_tuples = []
#     sitenames = get_sitenames()
#     with open(csv_filename, newline='') as csvfile:
#         csv_reader = csv.reader(csvfile, delimiter=' ', quotechar='|')
#         for row in csv_reader:
#             details = row[0].split(',')
#             current_site_name = details[0]
#             is_exists = check_sitename_exists(sitenames, current_site_name)
#             if (is_exists == True):
#                 logging.warning('%s already exists in the database. Please use the update function. Skipping...',current_site_name)
#             else:
#                 num_nodes = details[1]
#                 if not num_nodes:
#                     num_nodes = -1
#                 else:
#                     num_nodes = int(num_nodes)
#                 normalized_name = normalize_ce_name(current_site_name)
#                 site_tuples.append((current_site_name, normalized_name, num_nodes))
#                 logging.debug('Adding %s to the database', current_site_name)
#     try:
#         cursor.executemany(ADD_SITE, site_tuples)
#         conn.commit()
#         logging.debug("Total number of sites added : %s", cursor.rowcount)
#     except mysql.connector.Error as error:
#         logging.error("Failed to add sites to database: {}".format(error))
#         conn.rollback()
#     finally:
#         if(conn.is_connected()):
#             cursor.close()
#             conn.close()
#             logging.debug("connection is closed")
#
# def update_site_last_update_time(site_id):
#     """
#     Update last update time of the site
#
#     Args:
#         site_id (int): Site ID
#     """
#     cursor, conn = get_connection()
#     try:
#         cursor.execute(UPDATE_LAST_SITE_UPDATE_TIME,[site_id])
#     except mysql.connector.Error as error:
#         logging.error("Failed to update site last update times: {}".format(error))
#     finally:
#         if(conn.is_connected()):
#             cursor.close()
#             conn.close()
#
#
# def check_sitename_exists(sitenames,current_site_name):
#     """
#     Check whether current_site_name exists in sitenames
#
#     Args:
#         sitenames (list): List of available sites in the DB
#         current_site_name (str): Name of the new site
#
#     Returns:
#         Bool: True if current_site_name exists in sitenames
#     """
#     if not sitenames:
#         return False
#     else:
#         if current_site_name in sitenames:
#             return True
#         else:
#             return False

# def get_sitenames():
#     """
#     Get all names of sites
#
#     Returns:
#         sitenames
#     """
#     cursor, conn = get_connection()
#     sitenames=[]
#     try:
#         cursor.execute(GET_SITENAMES)
#         results = cursor.fetchall()
#         for row in results:
#             sitenames.append(row[0])
#         return sitenames
#     except mysql.connector.Error as error:
#         logging.error("Failed to get sitenames: {}".format(error))
#     finally:
#         if(conn.is_connected()):
#             cursor.close()
#             conn.close()


# def update_processing_state(state,initialize=True):
#     """
#     Update processing_states table
#
#     Return True if succesfull
#     """
#     success_flag = False
#     run_id = get_run_id()
#     cursor, conn = get_connection(auto_commit=False)
#     try:
#         if initialize:
#             site_ids = get_site_ids()
#             state_tuple = []
#             for site_id in site_ids:
#                 state_tuple.append((site_id,run_id,state))
#             cursor.executemany(INITIALIZE_PROCESSING_STATE,state_tuple)
#         else:
#             site_ids = get_sites_by_processing_state('WAITING')
#             state_tuple = []
#             for site_id in site_ids:
#                 state_tuple.append((state, site_id,run_id))
#             cursor.executemany(UPDATE_PROCESSING_STATE,state_tuple)
#         conn.commit()
#         logging.debug('Update processing states to %s successfully',state)
#         success_flag = True
#     except mysql.connector.Error as error:
#         logging.error("Failed to update processing states: {}".format(error))
#         success_flag = False
#     finally:
#         if(conn.is_connected()):
#             cursor.close()
#             conn.close()
#         return success_flag

# def get_sites_by_processing_state(state):
#     run_id = get_run_id()
#     site_ids = []
#     cursor, conn = get_connection()
#     try:
#         cursor.execute(GET_SITE_IDS_BY_PROCESSING_STATE,(state,run_id))
#         results = cursor.fetchall()
#         for row in results:
#             site_ids.append(row[0])
#     except mysql.connector.Error as error:
#         logging.error("Failed to fetch processing states: {}".format(error))
#     finally:
#         if(conn.is_connected()):
#             cursor.close()
#             conn.close()
#         return site_ids
        
# Job related functions

# def add_job(job_id,site_id):
#     """
#     Add jobs to the database
#
#     Args:
#         job_id (str): Job ID
#         site_id (int)
#     """
#     cursor, conn = get_connection()
#     try:
#         run_id = get_run_id()
#         cursor.execute(ADD_JOB, (job_id,run_id, site_id, 'STARTED'))
#         logging.debug('Job %s added to database succesfully',job_id.strip())
#     except mysql.connector.Error as error:
#         logging.error("Failed to add job: {}".format(error))
#     finally:
#         if(conn.is_connected()):
#             cursor.close()
#             conn.close()
#
# def get_all_job_ids_by_state(state):
#     """
#     Get all jobs in the database with given state
#
#     Args:
#         state (enum): ('STARTED','STALLED','COMPLETED','KILLED')
#
#     Returns:
#         job_ids(list): Job IDs of jobs in given  state
#     """
#     cursor, conn = get_connection()
#     try:
#         run_id = get_run_id()
#         cursor.execute(GET_ALL_JOB_IDS_BY_STATE,[state,run_id])
#         results = cursor.fetchall()
#         job_ids = []
#         for row in results:
#             job_ids.append(row[0])
#         return job_ids
#     except mysql.connector.Error as error:
#         logging.error("Failed to get jobs by state: {}".format(error))
#     finally:
#         if(conn.is_connected()):
#             cursor.close()
#             conn.close()
#
# def update_job_state_by_job_id(job_id,state):
#     """
#     Update state of the job
#
#     Args:
#         job_id (int): Single id or id list
#         state (enum): ('STARTED','ERROR','STALLED','COMPLETED','KILLED')
#     """
#     cursor, conn = get_connection(auto_commit=False)
#     run_id = get_run_id()
#     job_tuple = []
#     if type(job_id) == str:
#         job_tuple.append((state,job_id,run_id))
#     elif type(job_id) == list:
#         for id in job_id:
#             job_tuple.append((state,id,run_id))
#     try:
#         cursor.executemany(UPDATE_JOB_STATE_BY_JOBID, job_tuple)
#         conn.commit()
#     except mysql.connector.Error as error:
#         logging.error("Failed to update job state: {}".format(error))
#         conn.rollback()
#     finally:
#         if(conn.is_connected()):
#             cursor.close()
#             conn.close()
