from flask import Flask, render_template, request, jsonify
from flask_cors import CORS
from db_connection import *

app = Flask(__name__)
CORS(app)

@app.route('/search_site', methods=['POST'])
def search_box():
    if request.method == 'POST':
        req = request.json['SearchFormInput']
        queries = req['SearchFields']
        equation = req['Equation']
        site_id = req['SiteId']
        run_id = req['RunId']
        print (site_id,equation,queries,run_id)
        if site_id == "all":
            total_sites, total_completed_site_num, matching_sites, matching_sites_list, unmatching_sites_list, incomplete_sites_list = all_site_search(queries,equation,run_id)
            result = {
		        'grid_search': True,
                'total_sites': total_sites,
                'covered_sites':total_completed_site_num,
                'matching_sites': matching_sites,
                'matching_sites_list': matching_sites_list,
                'unmatching_sites_list': unmatching_sites_list,
                'incomplete_sites_list': incomplete_sites_list
            }
            return jsonify (result)
        else:
            total_nodes, matching_nodes, covered_nodes, matching_job_data, unmatching_job_data = full_search_site(site_id,queries,equation,run_id)
            result = {
		        'grid_search': False,
                'total_nodes': total_nodes,
                'covered_nodes':covered_nodes,
                'matching_nodes': matching_nodes,
                'matching_nodes_data': matching_job_data,
                'unmatching_nodes_data': unmatching_job_data
            }
            return jsonify (result)
    if request.method == 'GET':
        return render_template('index.html',init='True')

@app.route('/last_run', methods=['GET'])
def get_last_run():
    result = get_last_run_data()
    return jsonify(result)

@app.route('/all_runs', methods=['GET'])
def get_all_runs():
    runs_data = get_all_runs_data()
    return jsonify(runs_data)

@app.route('/all_runs_cli', methods=['GET'])
def get_all_runs_cli():
    runs_data = get_all_runs_data_cli()
    return jsonify(runs_data)

@app.route('/all_sites', methods=['GET'])
def get_sites_data():
    all_site_data = get_sites()
    return jsonify(all_site_data)

@app.route('/search_keys', methods=['POST'])
def get_keys():
    if request.method == 'POST':
        req = request.json['Run']
        print ('req is ',req)
        run_id = req['RunId']
        search_keys = get_search_keys(run_id)
        print (search_keys)
        return jsonify(search_keys)

@app.route('/run_summary', methods=['POST'])
def run_summary():
    if request.method == 'POST':
        print (request.json)
        run_id = request.json['RunId']
        run_summary = get_run_summary(run_id)
        print(run_summary)
        return jsonify(run_summary)

@app.route('/jobs', methods=['POST'])
def job_summary():
    if request.method == 'POST':
        run_id = request.json['RunId']
        started_job_num = get_job_count_ids_by_state('STARTED', run_id)
        completed_job_num = get_job_count_ids_by_state('COMPLETED', run_id)
        killed_job_num = get_job_count_ids_by_state('KILLED', run_id)
        job_summary = {
            'started_jobs': started_job_num,
            'completed_jobs': completed_job_num,
            'killed_jobs': killed_job_num
        }
        return jsonify(job_summary)

if __name__ == "__main__":
    app.run(debug=True)
