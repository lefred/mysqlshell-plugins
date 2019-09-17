import json
import requests

api_ver = "20190715"

def __router_call(route, router_ip, router_port, user, password):
    url = "http://" + router_ip + ":" + str(router_port) + "/api/" + api_ver + route
    resp = requests.get(url,auth=(user, password))
    if resp.status_code == 200:
        return resp
    else:
        return False

def __cluster_name(router_ip, router_port, user, password):
    result = __router_call("/metadata", router_ip, router_port, user, password)
    if result:    
        result_json = json.loads(result.content)
        cluster_name = result_json['items'][0]['name']
        print ("+" + "-" * 16 + "-" * len(cluster_name) + "+")
        print ("| Cluster name: %s |" % cluster_name)
        print ("+" + "-" * 16 + "-" * len(cluster_name) + "+")
        return cluster_name
    return False

def __cluster_metadata_status(router_ip, router_port, user, password, cluster_name):
    result = __router_call("/metadata/%s/status" % cluster_name, router_ip, router_port, user, password)
    if result:    
        result_json = json.loads(result.content)
        print ("     Refresh Succeeded: " + str(result_json['refreshSucceeded']))
        print ("        Refresh Failed: " + str(result_json['refreshFailed']))
        print (" Last Refresh Hostname: " + result_json['lastRefreshHostname'] \
                + ":" + str(result_json['lastRefreshPort']))

def __cluster_routes(router_ip, router_port, user, password):
    result = __router_call("/routes", router_ip, router_port, user, password)
    if result:    
        print ("   +--------+")
        print ("   | routes |")
        print ("   +--------+")
        result_json = json.loads(result.content)
        for item in result_json['items']:
                route_name = item['name']
                result_item = __router_call("/routes/%s/health" % route_name, router_ip, router_port, user, password)
                result_item_json = json.loads(result_item.content) 
                if result_item_json['isAlive']:
                        print("    * %s (alive) :" % route_name)
                else:
                        print("    * %s (dead)  :" % route_name)

                result_status = __router_call("/routes/%s/status" % route_name, router_ip, router_port, user, password)
                result_status_json= json.loads(result_status.content) 
                print("\tTotal Connections: %d\tActive Connections: %d\tBlocked Hosts: %d" 
                                % (result_status_json['totalConnections'], result_status_json['activeConnections'], 
                                result_status_json['blockedHosts']))
                result_routes = __router_call("/routes/%s/destinations" % route_name, router_ip, router_port, user, password)
                result_routes_json = json.loads(result_routes.content)
                for destination in result_routes_json['items']:
                        print ("\t---> %s : %d" % (destination['address'], destination['port']))

def status(router_ip, router_port, user, password):
    cluster_name = __cluster_name(router_ip, router_port, user, password)
    if cluster_name:
            __cluster_metadata_status(router_ip, router_port, user, password, cluster_name)
            __cluster_routes(router_ip, router_port, user, password)
    
