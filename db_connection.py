import csv
import datetime
import time
import logging
import os
import fnmatch

from config import DB_HOST, DB_USER, DB_PWD, DB_DATABASE
from sql_queries import *

import mysql.connector

# Utils


def normalize_ce_name(target_ce):
    return target_ce.replace("::", "_").lower()[len("alice_"):]

# Connection Functions


def get_connection(auto_commit=True):
    """
    Establish database connection

    Args:
        auto_commit (bool, optional): Enable auto commit. Defaults to True.

    Returns:
        cursor: MySQL cursor object
        connection: MySQL connection object
    """
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PWD,
            database=DB_DATABASE,
            autocommit=auto_commit
        )
        cursor = connection.cursor()
        return cursor, connection
    except mysql.connector.Error as error:
        logging.error("Error while connecting to MySQL", error)


def get_all_runs_data():
    """
    Get data of all runs that are completed or finished

    Returns:
        all_run_data: Dictionary of run data
    """
    cursor, conn = get_connection()
    try:
        cursor.execute(GET_ALL_RUNS_DATA)
        results = cursor.fetchall()
        last_run = {
            'run_id': results[0][0],
            'started_at': results[0][1],
            'finished_at': results[0][2],
            'state': results[0][3]
        }
        run_data = []
        for row in results:
            run = {
                'run_id': row[0],
                'started_at': row[1],
                'finished_at': row[2],
                'state': row[3]
            }
            run_data.append(run)
        all_run_data = {
            'selected_run': last_run,
            'all_runs': run_data
        }
        return all_run_data
    except mysql.connector.Error as error:
        logging.error("Failed to get all runs data: {}".format(error))
    finally:
        if(conn.is_connected()):
            cursor.close()
            conn.close()


def get_all_runs_data_cli():
    """
    Get data of all runs in all states

    Returns:
        run_data: Dictionary of run data

    Returns:
        run_data: Array of run data dicts
    """
    cursor, conn = get_connection()
    try:
        cursor.execute(GET_ALL_RUNS_DATA_ANY_STATE)
        results = cursor.fetchall()
        run_data = []
        for row in results:
            run = {
                'run_id': row[0],
                'started_at': row[1],
                'finished_at': row[2],
                'state': row[3]
            }
            run_data.append(run)
        return run_data
    except mysql.connector.Error as error:
        logging.error("Failed to get all runs data: {}".format(error))
    finally:
        if(conn.is_connected()):
            cursor.close()
            conn.close()


def check_run_exists(run_id):
    """
    Check whether the given run_id is still running

    Args:
        run_id (int): Run ID

    Returns:
        flag: True if run_id is still running
    """
    cursor, conn = get_connection()
    flag = True
    try:
        cursor.execute(CHECK_RUN_STATE, [run_id])
        state = cursor.fetchone()[0]
        if state == 'STARTED':
            logging.info('Last run is still running')
            flag = True
        else:
            logging.info('No currently executing runs')
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
    """
    Get the command identifiers used in the given run

    Args:
        run_id (int): Run ID

    Returns:
        search_keys: list of identifiers
    """
    cursor, conn = get_connection()
    try:
        cursor.execute(GET_SEARCH_KEYS, [run_id])
        results = cursor.fetchone()
        return results[0]
    except mysql.connector.Error as error:
        logging.debug("Failed to get search keys: {}".format(error))
    finally:
        if(conn.is_connected()):
            cursor.close()
            conn.close()


def get_all_jobs_count_summary(run_id):
    """
    Get summary of all jobs

    Args:
        run_id (int): Run ID

    Returns:
        site_dict: { site_id: { job_state: count of jobs in this state } }
    """
    cursor, conn = get_connection()
    try:
        cursor.execute(GET_ALL_JOBS_COUNT_SUMMARY, [run_id])
        results = cursor.fetchall()
        site_dict = {}
        for row in results:
            # row[0] - COUNT(job_id),
            # row[1] - job_state
            # row[2] - site_id
            if row[2] not in site_dict.keys():
                site_dict.update({
                    row[2]: {
                        row[1]: row[0]
                    }
                })
            else:
                site_dict[row[2]].update({row[1]: row[0]})
        return site_dict
    except mysql.connector.Error as error:
        logging.error("Failed to get all job summary: {}".format(error))
    finally:
        if(conn.is_connected()):
            cursor.close()
            conn.close()


def get_num_nodes_by_site(site_id):
    """
    Get the number of nodes of site

    Args:
        site_id (int): Site ID

    Returns:
        num_nodes: Number of nodes
    """
    try:
        cursor, conn = get_connection()
        cursor.execute(GET_NUM_NODES_BY_SITE_ID, [site_id])
        result = cursor.fetchone()
        return result[0]
    except mysql.connector.Error as error:
        logging.error(
            "Failed to get number of nodes by site id: {}".format(error))
    finally:
        if (conn.is_connected()):
            cursor.close()
            conn.close()


def get_nodename_by_job_id(job_id):
    """
    Get the name of the node the job got exectued on

    Args:
        job_id (int): Job ID

    Returns:
        nodename: Nodename
    """
    try:
        cursor, conn = get_connection()
        cursor.execute(GET_NODENAME_BY_JOB_ID, [job_id])
        result = cursor.fetchone()
        return result[0]
    except mysql.connector.Error as error:
        logging.error("Failed to get nodename by job Id: {}".format(error))
    finally:
        if (conn.is_connected()):
            cursor.close()
            conn.close()


def get_job_ids_of_covered_nodes(run_id, site_id):
    """ Get all the job IDs of jobs which got executed in unique nodes

    Args:
        run_id (int): Run ID
        site_id (int): Site ID

    Returns:
        job_ids(list)
    """
    try:
        cursor, conn = get_connection()
        cursor.execute(GET_ALL_JOB_IDS_OF_COVERED_NODES, [site_id, run_id])
        result = cursor.fetchall()
        job_ids = []
        for row in result:
            job_ids.append(row[0])
        return job_ids
    except mysql.connector.Error as error:
        logging.error(
            "Failed to get job Ids of covered nodes: {}".format(error))
    finally:
        if (conn.is_connected()):
            cursor.close()
            conn.close()


def get_job_params(job_id):
    """
    Get command identifiers and results of the command of the given job id

    Args:
        job_id (int)

    Returns:
        params(dict): { identifier: result }
    """
    try:
        cursor, conn = get_connection()
        cursor.execute(GET_PARAMS_BY_JOB_ID, [job_id])
        result = cursor.fetchall()
        params = {}
        for row in result:
            params.update({row[0]: row[1]})
        return params
    except mysql.connector.Error as error:
        logging.error("Failed to get parameters by Job Id: {}".format(error))
    finally:
        if (conn.is_connected()):
            cursor.close()
            conn.close()


def check_key_val_exists_in_dict(key, val, dict):
    """
    Check whether the given command identifier and value exist in the dict

    Args:
        key (string): command identifier
        val (string): result of the command(Accept *,? wildcards)
        dict : job parameter dictionary

    Returns:
        bool: True if exists
    """
    if key in dict.keys() and fnmatch.fnmatch(dict[key], val):
        return True
    else:
        return False


def full_search_site(site_id, queries, equation, run_id):
    """
    Perform a search for the set of queries on the given site

    Args:
        site_id (int): Site ID
        queries (int): { letter: { identifier: result } } Eg: { A: { Singularity : SUPPORTED 
        equation (string): Logical relationship between query keys Eg: "A & (B | ~ (C))"
        run_id (int): Run ID

    Returns:
        total_nodes(int): Total nodes in site
        matching_nodes(int): Number of nodes matching the query
        covered_nodes(int): Number of nodes covered in the site
        matching_job_data(list): Data of nodes matching the query
        unmatching_job_data(list): Data of nodes noy matching the query
    """
    if type(site_id) == str:
        site_id = int(site_id)
    job_ids = get_job_ids_of_covered_nodes(run_id, site_id)
    total_nodes = get_num_nodes_by_site(site_id)
    covered_nodes = len(job_ids)
    matching_nodes = 0
    equation = equation.replace("&", " and ").replace(
        "|", " or ").replace("~", " not ")
    matching_job_data = {}
    unmatching_job_data = {}
    for job in job_ids:
        local_equation = equation
        params = get_job_params(job)
        nodename = get_nodename_by_job_id(job)
        for variable_key in queries:
            is_exists = check_key_val_exists_in_dict(
                queries[variable_key]['query_key'], queries[variable_key]['query_value'], params)
            if is_exists:
                local_equation = local_equation.replace(variable_key, 'True')
            else:
                local_equation = local_equation.replace(variable_key, 'False')
        if (eval(local_equation) is True):
            matching_nodes += 1
            matching_job_data.update({nodename: params})
        else:
            unmatching_job_data.update({nodename: params})
    return total_nodes, matching_nodes, covered_nodes, matching_job_data, unmatching_job_data


def all_site_search(queries, equation, run_id):
    """Perform a Grid search for the given equation

    Args:
        queries (dict): Search Query Dict
        equation (str): Boolean equation for combining search queries
        run_id (str): Run Id

    Returns:
        total_sites(int): Total number of sites
        total_completed_site_num(int): Total number of completed sites in the given run
        matching_sites(int): Number of sites that match the query
        matching_sites_list(list): Sites matching the query
        unmatching_sites_list(list): Sites not matching the query
        all_sites(list): List of all the sites
    """
    all_sites = get_all_sitenames()
    # instead of completed sites, get sites which have more than x % coverage
    completed_sites = get_sites_by_processing_state('COMPLETED', run_id)
    matching_sites = 0
    matching_sites_list = []
    unmatching_sites_list = []
    total_sites = len(all_sites)
    total_completed_site_num = len(completed_sites)
    for site_id in completed_sites:
        total_nodes, matching_nodes, covered_nodes, matching_job_data, unmatching_job_data = full_search_site(
            site_id, queries, equation, run_id)
        if matching_nodes/covered_nodes > 0.5:
            print("Site", site_id, "matching")
            matching_sites += 1
            matching_sites_list.append(get_sitename_by_site_id(site_id))
        else:
            print("Site", site_id, "not matching")
            unmatching_sites_list.append(get_sitename_by_site_id(site_id))
    incomplete_sites_list = list(set(all_sites).difference(
        matching_sites_list).difference(unmatching_sites_list))
    return total_sites, total_completed_site_num, matching_sites, matching_sites_list, unmatching_sites_list, incomplete_sites_list


def get_sites_by_processing_state(state, run_id):
    """
    Get Sites in the given state

    Args:
        state (enum ['WAITING','COMPLETED','STALLED','ABORTED']): Processing state of site
        run_id (int): Run ID

    Returns:
        site_ids(list): Site ID list
    """
    site_ids = []
    cursor, conn = get_connection()
    try:
        cursor.execute(GET_SITE_IDS_BY_PROCESSING_STATE, [state, run_id])
        results = cursor.fetchall()
        for row in results:
            site_ids.append(row[0])
    except mysql.connector.Error as error:
        logging.error("Failed to fetch processing states: {}".format(error))
    finally:
        if(conn.is_connected()):
            cursor.close()
            conn.close()
        return site_ids

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


def get_sitename_by_site_id(site_id):
    """
    Get the site name by site id

    Args:
        site_id (int): Site ID

    Returns:
        site_name(str): Site name
    """
    cursor, conn = get_connection()
    try:
        cursor.execute(GET_SITENAME_BY_SITE_ID, [site_id])
        results = cursor.fetchone()
        return results[0]
    except mysql.connector.Error as error:
        logging.error("Failed to get sitename by site Id: {}".format(error))
    finally:
        if(conn.is_connected()):
            cursor.close()
            conn.close()


def get_all_sitenames():
    """
    Get names of all sites in the sites table

    Returns:
        sitenames(list): List of sitenames
    """
    cursor, conn = get_connection()
    try:
        sitenames = []
        cursor.execute(GET_ALL_SITENAMES)
        results = cursor.fetchall()
        for row in results:
            sitenames.append(row[0])
        return sitenames
    except mysql.connector.Error as error:
        logging.error("Failed to get all sitenames: {}".format(error))
    finally:
        if(conn.is_connected()):
            cursor.close()
            conn.close()


def get_run_summary(run_id):
    """
    Get a full summary of the given run

    Args:
        run_id (int): Run ID

    Returns:
        site_data: Array of dict objects with data each site in the given run
    """
    cursor, conn = get_connection()
    try:
        site_data = []
        cursor.execute(GET_RUN_SUMMARY, [run_id])
        results = cursor.fetchall()
        for row in results:
            site = {
                'site_id': row[0],
                'sitename': row[1],
                'covered_nodes': row[2],
                'total_nodes': row[3],
                'coverage': row[4]
            }
            site_data.append(site)
        return site_data
    except mysql.connector.Error as error:
        logging.error("Failed to get run summary: {}".format(error))
    finally:
        if(conn.is_connected()):
            cursor.close()
            conn.close()


def get_job_count_ids_by_state(state, run_id):
    """
    Get job count in the database with given state

    Args:
        state (enum): ('STARTED','COMPLETED','KILLED')
        run_id (int): Run ID
    Returns:
        job_count(list): Count of jobs in given  state
    """
    cursor, conn = get_connection()
    try:
        cursor.execute(GET_ALL_JOB_IDS_BY_STATE, [state, run_id])
        results = cursor.fetchone()
        return results[0]
    except mysql.connector.Error as error:
        logging.error("Failed to get jobs by state: {}".format(error))
    finally:
        if(conn.is_connected()):
            cursor.close()
            conn.close()
