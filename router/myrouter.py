import sys
import importlib
from importlib import util
import json
requests_spec =  util.find_spec("requests")
found_requests = requests_spec is not None

if found_requests:
    import requests
    requests.packages.urllib3.disable_warnings()
else:
    print("Error importing module 'requests', check if it's installed (Python {}.{}.{})".format(
           sys.version_info[0], sys.version_info[1], sys.version_info[2]))

import mysqlsh
shell = mysqlsh.globals.shell

class MyRouter:
    def __init__(self,uri=False):
        self.uri = uri
        self.user = shell.parse_uri(self.uri)['user']
        self.ip = shell.parse_uri(self.uri)['host']
        self.port = shell.parse_uri(self.uri)['port']
        if not "password" in shell.parse_uri(self.uri):
            self.__password = shell.prompt('Password: ',{'type': 'password'})
        else:
            self.__password = shell.parse_uri(self.uri)['password']

    def __format_bytes(self, size):
        # 2**10 = 1024
        power = 2**10
        for unit in ('bytes', 'kb', 'mb', 'gb'):
            if size <= power:
                return "%d %s" % (size, unit)
            size /= power

        return "%d tb" % (size,)

    def __router_call(self,route):

        url = "https://" + self.ip + ":" + str(self.port) + "/api/" + self.api + route
        try:
            if found_requests:
                resp = requests.get(url,auth=(self.user,self.__password), verify=False)
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


    def __cluster_routes(self, route_to_find):
        result = self.__router_call("/routes")
        if result:
            l1 = l2 = l3 = l6 = 0
            l4 = l5 = 18
            header_printed = 0
            print_empty =[]
            result_json = json.loads(result.content)
            got_item=False
            for item in result_json['items']:
                    route_name = item['name']
                    if route_to_find in route_name:
                        result_item = self.__router_call("/routes/%s/connections" % route_name)
                        result_item_json = json.loads(result_item.content)
                        if len(result_item_json['items']) > 0:
                            l1 = len(route_name)+2
                            for entry in result_item_json['items']:
                                l2_tmp = len(entry['sourceAddress'])
                                if l2_tmp > l2: l2 = l2_tmp
                                l3_tmp = len(entry['destinationAddress'])
                                if l3_tmp > l3: l3 = l3_tmp
                                l4_tmp = len(str(entry['bytesFromServer']))
                                if l4_tmp > l4: l4 = l4_tmp
                                l5_tmp = len(str(entry['bytesToServer']))
                                if l5_tmp > l5: l5 = l5_tmp
                                l6_tmp = len(entry['timeStarted'])
                                if l6_tmp > l6: l6 = l6_tmp
                            fmt = "| {0:"+str(l1)+"s} | {1:"+str(l2)+"s} | {2:"+str(l3)+"s} | {3:>"+str(l4)+"s} | {4:>"+str(l5)+"s} | {5:"+str(l6)+"s} |"
                            header = fmt.format("Route", "Source", "Destination", "From Server", "To Server", "Connection Started")
                            bar = "+" + "-" * (l1+2) + "+" + "-" * (l2+2) + "+" + "-" * (l3+2) + "+" + "-" * (l4+2) + "+" + "-" * (l5+2) + "+" + "-" * (l6+2) + "+"
                            got_item=True
            if not got_item:
                print("No connections to show.")
                return
            print (bar)
            print (header)
            print (bar)
            header_printed=1
            for item in result_json['items']:
                    route_name = item['name']
                    if route_to_find in route_name:
                        result_item = self.__router_call("/routes/%s/connections" % route_name)
                        result_item_json = json.loads(result_item.content)
                        if len(result_item_json['items']) > 0:
                            if len(print_empty) > 0:
                                for routename in print_empty:
                                    print (fmt.format(routename, " "," ", " ", " ",  " "))
                                    print_empty.clear
                            for entry in result_item_json['items']:
                                print (fmt.format(route_name, entry['sourceAddress'], entry['destinationAddress'],
                                str(self.__format_bytes(entry['bytesFromServer'])),
                                str(self.__format_bytes(entry['bytesToServer'])), entry['timeStarted']))
                                route_name=""
                        else:
                            print (fmt.format(route_name, " "," ", " ", " ",  " "))

                    print (bar)



    def __cluster_metadata_status(self, cluster_name):
        result = self.__router_call("/metadata/%s/status" % cluster_name)
        if result:
            result_json = json.loads(result.content)
            print ("     Refresh Succeeded: " + str(result_json['refreshSucceeded']))
            print ("        Refresh Failed: " + str(result_json['refreshFailed']))
            print (" Last Refresh Hostname: " + result_json['lastRefreshHostname'] \
                    + ":" + str(result_json['lastRefreshPort']))

    def __cluster_all_routes_blocked_hosts(self):
        result = self.__router_call("/routes")
        if result:
            l1 = l2 = 17
            header_printed = 0
            print_empty =[]
            result_json = json.loads(result.content)
            for item in result_json['items']:
                    route_name = item['name']
                    result_blocked = self.__router_call("/routes/%s/blockedHosts" % route_name)
                    result_blocked_json = json.loads(result_blocked.content)
                    if len(result_blocked_json['items']) > 0:
                        if header_printed == 0:
                            l1 = len(route_name)+2
                            for entry in result_blocked_json['items']:
                                l2_tmp = len(entry)
                                if l2_tmp > l2: l2 = l2_tmp
                            fmt = "| {0:"+str(l1)+"s} | {1:"+str(l2)+"s} |"
                            header = fmt.format("Route", "Blocked Host(s)")
                            bar = "+" + "-" * (l1+2) + "+" + "-" * (l2+2) + "+"
            print (bar)
            print (header)
            print (bar)
            header_printed=1
            for item in result_json['items']:
                    route_name = item['name']
                    result_blocked = self.__router_call("/routes/%s/blockedHosts" % route_name)
                    result_blocked_json = json.loads(result_blocked.content)
                    if len(result_blocked_json['items']) > 0:
                        if len(print_empty) > 0:
                            for routename in print_empty:
                                print (fmt.format(routename, " "," ", " ", " ",  " "))
                                print_empty.clear

                        for entry in result_blocked_json['items']:
                            print (fmt.format(route_name, entry))
                            route_name=""
                    else:
                        print (fmt.format(route_name, " "," ", " ", " ",  " "))

                    print (bar)

    def __cluster_all_routes(self):
        result = self.__router_call("/routes")
        if result:
            print ("   +--------+")
            print ("   | routes |")
            print ("   +--------+")
            result_json = json.loads(result.content)
            for item in result_json['items']:
                    route_name = item['name']
                    result_item = self.__router_call("/routes/%s/health" % route_name)
                    result_item_json = json.loads(result_item.content)
                    if result_item_json['isAlive']:
                            print("    * %s (alive) :" % route_name)
                    else:
                            print("    * %s (dead)  :" % route_name)
                    result_config = self.__router_call("/routes/%s/config" % route_name)
                    result_config_json = json.loads(result_config.content)
                    print("\tRouting Strategy: {}\tProtocol: {}".format(result_config_json['routingStrategy'],
                                                                        result_config_json['protocol']))
                    result_status = self.__router_call("/routes/%s/status" % route_name)
                    result_status_json= json.loads(result_status.content)
                    print("\tTotal Connections: %d\tActive Connections: %d\tBlocked Hosts: %d"
                                    % (result_status_json['totalConnections'], result_status_json['activeConnections'],
                                    result_status_json['blockedHosts']))
                    result_routes = self.__router_call("/routes/%s/destinations" % route_name)
                    result_routes_json = json.loads(result_routes.content)
                    for destination in result_routes_json['items']:
                            print ("\t---> %s : %d" % (destination['address'], destination['port']))

    def __cluster_name(self):
        result = self.__router_call("/metadata")
        if result:
            result_json = json.loads(result.content)
            cluster_name = result_json['items'][0]['name']
            print ("+" + "-" * 16 + "-" * len(cluster_name) + "+")
            print ("| Cluster name: %s |" % cluster_name)
            print ("+" + "-" * 16 + "-" * len(cluster_name) + "+")
            return cluster_name
        return False


    def status(self):
        cluster_name = self.__cluster_name()
        if cluster_name:
                self.__cluster_metadata_status(cluster_name)
                self.__cluster_all_routes()


    def connections(self, route_to_find=""):
        self.__cluster_routes(route_to_find)

    def blocked_hosts(self):
        self.__cluster_all_routes_blocked_hosts()

    api = "20190715"
