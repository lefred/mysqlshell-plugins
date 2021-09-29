

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
    "CONNECTION_ADMIN",
    "RESOURCE_GROUP_ADMIN",
    "RESOURCE_GROUP_USER",
    "XA_RECOVER_ADMIN"
]

mds_allowed_auth_plugins = [
    "caching_sha2_password",
    "mysql_native_password",
    "sha256_password"
]