import json
import requests
import mysqlsh
shell = mysqlsh.globals.shell

class MyRouter:
    def __init__(self, ip, port, user, password=False):
        self.user = user
        self.ip = ip
        self.port = str(port)
        self.user = user
        if not password:
            self.__password = shell.prompt('Password: ',{'type': 'password'})
        else:
            self.__password = password
   
    def __format_bytes(self, size):
        # 2**10 = 1024
        power = 2**10
        for unit in ('bytes', 'kb', 'mb', 'gb'):
            if size <= power:
                return "%d %s" % (size, unit)
            size /= power

        return "%d tb" % (size,)

    def __router_call(self,route):

        url = "http://" + self.ip + ":" + self.port + "/api/" + self.api + route
        try:
            resp = requests.get(url,auth=(self.user,self.__password))
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
            result_json = json.loads(result.content)
            fmt = "| {0:22s} | {1:18s} | {2:12s} | {3:>20s} | {4:>20s} | {5:27s} |"
            header = fmt.format("Route", "Source", "Destination", "From Server", "To Server", "Connection Started")
            bar = "+" + "-" * 24 + "+" + "-" * 20 + "+" + "-" * 14 + "+" + "-" * 22 + "+" + "-" * 22 + "+" + "-" * 29 + "+"
            print (bar)
            print (header)
            print (bar)
            for item in result_json['items']:
                    route_name = item['name']
                    if route_to_find in route_name:
                        result_item = self.__router_call("/routes/%s/connections" % route_name)
                        result_item_json = json.loads(result_item.content)
                        if len(result_item_json['items']) > 0:
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

    api = "20190715"


       
