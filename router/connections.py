import importlib
from importlib import util
import json

requests_spec =  util.find_spec("requests")
found_requests = requests_spec is not None

if found_requests:
    import requests

api_ver = "20190715"

def __format_bytes(size):
    # 2**10 = 1024
    power = 2**10
    n = 0
    power_labels = {0 : 'bytes', 1: 'kb', 2: 'mb', 3: 'gb', 4: 'tb'}
    while size > power:
        size /= power
        n += 1
    return "%d %s" % (size, power_labels[n])

def __router_call(route, router_ip, router_port, user, password):
    url = "http://" + router_ip + ":" + str(router_port) + "/api/" + api_ver + route
    try:
        if found_requests: 
            resp = requests.get(url,auth=(user, password))
        else:
            print("ERROR: This module cannot be used, missing module 'requests'")
            return False
    except:
        print("ERROR: Impossible to connect to the MySQL Router REST API")
        return False
    if resp.status_code == 200:
        return resp
    else:
        print("ERROR: Got error %d when trying to connect to the MySQL Router REST API" % resp.status_code)
        return False


def __cluster_routes(router_ip, router_port, user, password, route_to_find):
    result = __router_call("/routes", router_ip, router_port, user, password)
    if result: 
        result_json = json.loads(result.content)
        
        route_size = 22
        source_size = 15
        destination_size = 12
        from_size = 20
        to_size = 20
        
        for item in result_json['items']:
           if len(item['name']) > route_size:
              route_size = len(item['name'])
              if route_to_find in route_name:
                    result_item = __router_call("/routes/%s/connections" % route_name, router_ip, router_port, user, password)
                    result_item_json = json.loads(result_item.content)
                    if len(result_item_json['items']) > 0:
                        for entry in result_item_json['items']:
                           if len(entry['sourceAddress']) > source_size:
                              source_size =  entry['sourceAddress']
                           if len(entry['destinationAddress']) > destination_size:
                              destination_size = entry['destinationAddress']
                           if len(entry['bytesFromServer']) > from_size:
                              from_size = len(entry['bytesFromServer'])
                           if len(entry['bytesToServer']) > to_size:
                              to_size = len(entry['bytesToServer'])
            
        fmt = "| {0:%ds} | {1:%ds} | {2:%ds} | {3:>%ds} | {4:>%ds} | {5:27s} |" % (route_size, source_size, destination_size, from_size)
        header = fmt.format("Route", "Source", "Destination", "From Server", "To Server", "Connection Started")
        bar = "+" + "-" * (route_size + 2) + "+" + "-" * (source_size + 2) + "+" + "-" * (destination_size + 2) + "+" + "-" * (from_size + 2) + "+" + "-" * (to_size + 2) + "+" + "-" * 29 + "+"
        print (bar)
        print (header)
        print (bar)
        for item in result_json['items']:
                route_name = item['name']
                if route_to_find in route_name:
                    result_item = __router_call("/routes/%s/connections" % route_name, router_ip, router_port, user, password)
                    result_item_json = json.loads(result_item.content)
                    if len(result_item_json['items']) > 0:
                        for entry in result_item_json['items']:
                            print (fmt.format(route_name, entry['sourceAddress'], entry['destinationAddress'], 
                            str(__format_bytes(entry['bytesFromServer'])), 
                            str(__format_bytes(entry['bytesToServer'])), entry['timeStarted']))
                            route_name=""
                    else:
                            print (fmt.format(route_name, " "," ", " ", " ",  " "))

                    print (bar)
                    

def connections(router_ip, router_port, user, password, route_to_find=""):
    __cluster_routes(router_ip, router_port, user, password, route_to_find)
    
