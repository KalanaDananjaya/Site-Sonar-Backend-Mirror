from db_connection import *

def search_results(query,site_id):

    cursor, conn = get_connection()
    
    query = query.split(':')
    query_key = query[0].strip()
    query_value = query[1].strip()
    supported_sites = 0
    outputs = get_parsed_output_by_siteid(site_id)
    for node_id in outputs:
        output = json.loads(outputs[node_id])
        for section in output['sections']:
            current_section = section['data']
            for key in current_section:
                if key == query_key:
                    if current_section[key] == query_value:
                        supported_sites += 1

    collected_nodes = len(outputs)
    total_nodes = get_num_nodes_in_site(site_id)
    coverage = collected_nodes / total_nodes
    supported = supported_sites // total_nodes
    logging.info('%d nodes out of total %d nodes matches the query', supported, total_nodes)
    return total_nodes,coverage,supported