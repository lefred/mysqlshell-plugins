Steps to create Group Replication using Shell

# A. Environment

Let say we have 3 instances:

- Node1, port 3306
- Node2, port 3306
- Node3, port 3306

# B. Configure Instance

Assume clusterAdmin = gradmin, clusterAdminPassword = grpass

Login to Node1:

```
mysqlsh -- dba configure-instance { --host=127.0.0.1 --port=3306 --user=root } --clusterAdmin=gradmin --clusterAdminPassword=grpass --interactive=false --restart=true
```

Login to Node2:

```
mysqlsh -- dba configure-instance { --host=127.0.0.1 --port=3306 --user=root } --clusterAdmin=gradmin --clusterAdminPassword=grpass --interactive=false --restart=true
```

Login to Node3:

```
mysqlsh -- dba configure-instance { --host=127.0.0.1 --port=3306 --user=root } --clusterAdmin=gradmin --clusterAdminPassword=grpass --interactive=false --restart=true
```

# C. Create Group Replication using Shell

Login to Node1 and create Group Replication:

```
$ mysqlsh gradmin:grpass@localhost:3306
mysqlsh > group_replication.create()
```

Still on MySQL Shell, add Node2:

```
mysqlsh > group_replication.addInstance("gradmin:grpass@node2:3306")
Please select a recovery method [C]lone/[I]ncremental recovery/[A]bort (default Clone): 
```

Still on MySQL Shell, add Node3:

```
mysqlsh > group_replication.addInstance("gradmin:grpass@node3:3306")
Please select a recovery method [C]lone/[I]ncremental recovery/[A]bort (default Clone): 
```

# D. View group replication status to ensure all nodes are ONLINE

```
mysqlsh > group_replication.status()
```

# E. How to Switch Primary Instance to Another node

Let say we want to switch PRIMARY node to Node2

```
mysqlsh > group_replication.setPrimaryInstance("gradmin:grpass@node2:3306")
```

Check the group replication status to ensure the result:

```
mysqlsh > group_replication.status()
```

# F. How to reboot Group Replication From Complete Outage

Let say all nodes are down. Start all nodes, and run rebootGRFromCompleteOutage below from one of the nodes:

```
mysqlsh gradmin:grpass@node2:3306
mysqlsh > group_replication.rebootGRFromCompleteOutage()
```

Key-in the cluster Admin Password.

# G. Check group replication status

```
mysqlsh > group_replication.status()
```

When any nodes is restarted, it will autojoin to Group Replication without manual intervention
This is because system variable `group_replication_start_on_boot` is set to ON.

# H. How to convert Group Replication to InnoDB Cluster

Let say we want to run as InnoDB Cluster instead of Group Replication, then login to PRIMARY node and run below:

```
mysqlsh > group_replication.convertToIC('mycluster')
```

# I. Check InnoDB Cluster status as follow:

```
mysqlsh > var cluster = dba.getCluster()
mysqlsh > cluster.status()
```

# J. How to convert InnoDB Cluster to Group Replication

Let say for some reasons we want to convert InnoDB Cluster to Group Replication (i.e. for DR purposes).

Login to PRIMARY node and run below:

```
mysqlsh > group_replication.adoptFromIC()
```
