from flask import Flask, render_template, request, jsonify
from flask_cors import CORS
from functions import search_results
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
        #total_nodes,coverage,supported = search_results(query,site_id)
        run_id = get_run_data()['run_id']
        for query in queries:
            search_site_param(run_id, site_id,query['query_key'],query['query_value'])
        result = {
            'total_nodes':total_nodes,
            'supported': supported,
            'coverage':coverage
        }
        return jsonify (result)
    if request.method == 'GET':
        return render_template('index.html',init='True')

@app.route('/last_run', methods=['GET'])
def get_last_run():
    result = get_run_data()
    return jsonify(result)


if __name__ == "__main__":
    app.run(debug=True)