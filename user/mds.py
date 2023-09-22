
# see  mysql-shell/modules/util/dump/compatibility.cc
#      https://docs.oracle.com/en-us/iaas/mysql-database/doc/mysql-server.html#GUID-410B2C03-5238-463E-85FA-7E9861ABA0C0#MYAAS-GUID-410B2C03-5238-463E-85FA-7E9861ABA0C0
mds_allowed_privileges = [
    "USAGE",
    "ALTER",
    "ALTER ROUTINE",
    "CREATE",
    "CREATE ROLE",
    "CREATE ROUTINE",
    "CREATE TEMPORARY TABLES",
    "CREATE USER",
    "CREATE VIEW",
    "DELETE",
    "DROP",
    "DROP ROLE",
    "EVENT",
    "EXECUTE",
    "INDEX",
    "INSERT",
    "LOCK TABLES",
    "PROCESS",
    "REFERENCES",
    "REPLICATION_APPLIER",
    "REPLICATION CLIENT",
    "REPLICATION SLAVE",
    "SELECT",
    "SHOW DATABASES",
    "SHOW VIEW",
    "TRIGGER",
    "UPDATE",
    "APPLICATION_PASSWORD_ADMIN",
    "AUDIT_ADMIN",
    "FLUSH TABLES",
    "CONNECTION_ADMIN",
    "FLUSH_OPTIMIZER_COSTS",
    "FLUSH_USER_RESOURCES",
    "FLUSH_STATUS",
    "RESOURCE_GROUP_ADMIN",
    "RESOURCE_GROUP_USER",
    "ROLE_ADMN",
    "XA_RECOVER_ADMIN"
]

mds_allowed_auth_plugins = [
    "caching_sha2_password",
    "mysql_native_password",
    "sha256_password"
]