import json
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
        resp = requests.get(url,auth=(user, password))
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
        fmt = "| {0:22s} | {1:15s} | {2:12s} | {3:>20s} | {4:>20s} | {5:27s} |"
        header = fmt.format("Route", "Source", "Destination", "From Server", "To Server", "Connection Started")
        bar = "+" + "-" * 24 + "+" + "-" * 17 + "+" + "-" * 14 + "+" + "-" * 22 + "+" + "-" * 22 + "+" + "-" * 29 + "+"
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
    
