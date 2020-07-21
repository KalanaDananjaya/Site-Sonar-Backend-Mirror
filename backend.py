from flask import Flask, render_template, request, jsonify
from flask_cors import CORS
from db_connection import *

app = Flask(__name__)
CORS(app)

@app.route('/search_site', methods=['POST','GET'])
def search_box():
    if request.method == 'POST':
        req = request.json['SearchFormInput']
        queries = req['SearchFields']
        equation = req['Equation']
        site_id = req['SiteId']
        submitted_jobs, matching_jobs, total_jobs = full_search_site(site_id,queries,equation)
        result = {
            'submitted_jobs': submitted_jobs,
            'completed_jobs':total_jobs,
            'matching_jobs': matching_jobs
        }
        print (result)
        return jsonify (result)
    if request.method == 'GET':
        return render_template('index.html',init='True')

@app.route('/last_run', methods=['GET'])
def get_last_run():
    result = get_last_run_data()
    return jsonify(result)


if __name__ == "__main__":
    app.run(debug=True)