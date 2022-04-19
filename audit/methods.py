from mysqlsh.plugin_manager import plugin, plugin_function
from mysqlsh_plugins_common import run_and_show


def check_for_audit(session, schema, table):
    # check if there is already an audit table or column for this table
    type_to_return=None
    got_external_db=False
    try:
        audit_db = session.get_schema('information_audit_log')
        got_external_db=True
    except:
        got_external_db=False

    if got_external_db:
        try: 
            audit_db.get_table("{}_{}".format(schema,table))
            type_to_return = "external"
        except:
            # we should check if there is a JSON invisible column
            stmt = """SELECT column_name, extra from information_schema.columns
                       WHERE table_schema='{}' and table_name='{}'
                       and column_name='audit_info' and extra='INVISIBLE'""".format(schema, table)
            result = session.run_sql(stmt)
            rows = result.fetch_all()    
            if len(rows) > 0:
                type_to_return = "internal"

    return type_to_return

@plugin_function("audit.enable")
def enable(type='external', table=None, schema=None, session=None):
    """
    Enable Audit capture for a certain table in a certain schema.

    This function enbles de capture of audit tracking information for
    a specific table.

    Args:
        type (string): define the type of audit target, 'external' or 'internal'. External
            means on a dedicated table in audit_log schema. Internal means that
            the captured information is stored on a invisible column on the table itself.
        schema (string): the schema to use. Default is the current schema.
        table (string): the table name to use.
        session (object): The optional session object used to query the
            database. If omitted the MySQL Shell's current session will be used.

    """
    # Get hold of the global shell object
    import mysqlsh
    shell = mysqlsh.globals.shell

    if session is None:
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                  "function or connect the shell to a database")
            return
    
    uri_json = shell.parse_uri(session.get_uri())
    if uri_json['scheme'] == "mysql":
        print("This plugin requires a MySQL X connection !")
        return
    if schema is None:
        schema = session.get_current_schema()
        if schema is None:
            print("No schema specified neither one in use in the current session.")
            return
    else:
        try:
            schema = session.get_schema(schema)
        except:
            print("This schema name is invalid!")
            return
    if table is None:
        table = shell.prompt("Enter the name of the table to enable audit: ")
    try:
        table = schema.get_table(table)
    except:
        print("This table doesn't exist in the selected schema")
        return

    exist_type = check_for_audit(session, schema.name, table.name)
    if exist_type is not None and exist_type != type:
        print("Audit ({}) already enabled for this table !".format(exist_type))
        return

    if type == "external":        
        try:
            audit_db = session.get_schema('information_audit_log')
        except:
            audit_db = session.create_schema('information_audit_log')

        # search for audit table log called <schema>_<table>
        try:
            audit_db.get_table("{}_{}".format(schema.name,table.name))
            print("Audit table already exist, updating all the triggers!")
            trig_action = "updated"
        except:
            stmt = """CREATE TABLE {}.{}_{} (
                id int unsigned auto_increment primary key, 
                timestamp timestamp default current_timestamp,
                by_user varchar(100),
                action varchar(6),
                extra json)""".format(audit_db.name,schema.name, table.name)
            session.run_sql(stmt)
            print("External audit table created !")
            trig_action = "created"
        ##########################
        # creation of all triggers
        ##########################
        # get all fields of the table
        stmt = """select column_name from information_schema.columns where
                table_schema='{}' and table_name='{}' and 
                (column_name <> 'audit_info' and extra <> 'INVISIBLE')""".format(schema.name, table.name)
        result = session.run_sql(stmt)
        rows = result.fetch_all()    
        trigger_stmt = """drop trigger if exists {}.{}_audit_update""".format(schema.name, table.name)
        session.sql(trigger_stmt).execute()
        trigger_stmt = """
                        create trigger {}.{}_audit_update before update on {}.{}
                        for each row
                            begin
                                declare modif json default null;             
                                select json_object() into modif;                   

                        """.format(schema.name, table.name, schema.name, table.name)
        if len(rows) > 0:
            for row in rows:
                trigger_stmt = trigger_stmt + """if old.{} <> new.{} then
                select json_merge_patch(modif, json_object('{}', json_object("old", old.{}, "new", new.{}))) into modif;
                end if;
                """.format(row[0],row[0],row[0],row[0],row[0])

        trigger_stmt = trigger_stmt + """
        insert into information_audit_log.{}_{} set action="UPDATE", by_user=session_user(), extra=modif;
            
        END
            
        """.format(schema.name, table.name)
        session.sql(trigger_stmt).execute()
        trigger_stmt = """drop trigger if exists {}.{}_audit_insert""".format(schema.name, table.name)
        session.sql(trigger_stmt).execute()
        trigger_stmt = """
                            create trigger {}.{}_audit_insert after insert on {}.{}
                            for each row
                            begin
                                declare modif json default null;             
                                select json_object() into modif;                   

                        """.format(schema.name, table.name, schema.name, table.name)
        if len(rows) > 0:
            for row in rows:
                trigger_stmt = trigger_stmt + """
                select json_merge_patch(modif, json_object('{}',  new.{})) into modif;
                """.format(row[0],row[0])

        trigger_stmt = trigger_stmt + """
            insert into information_audit_log.{}_{} set action="INSERT", by_user=session_user(), extra=modif;
            
            END
            
        """.format(schema.name, table.name)
        session.sql(trigger_stmt).execute()
        trigger_stmt = """drop trigger if exists {}.{}_audit_delete""".format(schema.name, table.name)
        session.sql(trigger_stmt).execute()
        trigger_stmt = """
                            create trigger {}.{}_audit_delete before delete on {}.{}
                             for each row
                             begin
                                declare modif json default null;             
                                select json_object() into modif;                   

                        """.format(schema.name, table.name, schema.name, table.name)
        if len(rows) > 0:
            for row in rows:
                trigger_stmt = trigger_stmt + """
                select json_merge_patch(modif, json_object('{}',  old.{})) into modif;
                """.format(row[0],row[0])

        trigger_stmt = trigger_stmt + """
            insert into information_audit_log.{}_{} set action="DELETE", by_user=session_user(), extra=modif;
            
            END
            
        """.format(schema.name, table.name)
        session.sql(trigger_stmt).execute()

        print("External audit triggers {} !".format(trig_action))
    elif type == "internal": 
        if exist_type != type:
            # adding the new invisible column
            stmt = "alter table {}.{} add audit_info json invisible".format(schema.name, table.name)
            session.sql(stmt).execute()
            print("Invisible audit_log column created.") 
            trig_action = "created"
        else:
            trig_action = "updated"           
        ##########################
        # creation of all triggers
        ##########################
        # get all fields of the table
        stmt = """select column_name from information_schema.columns where
                table_schema='{}' and table_name='{}' and 
                (column_name <> 'audit_info' and extra <> 'INVISIBLE')""".format(schema.name, table.name)
        result = session.run_sql(stmt)
        rows = result.fetch_all()    
        trigger_stmt = """drop trigger if exists {}.{}_audit_update""".format(schema.name, table.name)
        session.sql(trigger_stmt).execute()
        trigger_stmt = """
                        create trigger {}.{}_audit_update before update on {}.{}
                        for each row
                            begin
                                declare modif json default null;
                                declare modif_arr json default null;
                                select json_object('updated_at', now(), 
                                'updated_by', session_user()) into modif;
                        """.format(schema.name, table.name, schema.name, table.name)
        if len(rows) > 0:
            for row in rows:
                trigger_stmt = trigger_stmt + """if old.{} <> new.{} then
                select json_merge_patch(modif, 
                      json_object('{}', 
                        json_object("old", old.{}, 
                                    "new", new.{})
                      )
               ) into modif;
                end if;
                """.format(row[0],row[0],row[0],row[0],row[0])

        trigger_stmt = trigger_stmt + """
        if old.audit_info is NULL then
          set new.audit_info = json_set("{}","$.modifications", modif);
        else
            if json_extract(old.audit_info, "$.modifications") 
               is NULL then
              set new.audit_info=json_merge_patch(old.audit_info,
                  json_set(old.audit_info, 
                           "$.modifications", modif));              
            else                
              set new.audit_info=json_merge_patch(old.audit_info,
                  json_array_append(old.audit_info,
                           "$.modifications", modif));
            end if;
        end if; 
            
        END
            
        """
        session.sql(trigger_stmt).execute()
        trigger_stmt = """drop trigger if exists {}.{}_audit_insert""".format(schema.name, table.name)
        session.sql(trigger_stmt).execute()
        trigger_stmt = """
                            create trigger {}.{}_audit_insert before insert on {}.{}
                            for each row
                             set new.audit_info=json_object('created_at', now(), 
                               'created_by', session_user());                

                        """.format(schema.name, table.name, schema.name, table.name)
        session.sql(trigger_stmt).execute()

        print("Internal audit triggers {} !".format(trig_action))

    else:
        print("Invalid type !")


@plugin_function("audit.disable")
def enable(table=None, schema=None, session=None):
    """
    Disable Audit capture for a certain table in a certain schema.

    This function enbles de capture of audit tracking information for
    a specific table.

    Args:
        schema (string): the schema to use. Default is the current schema.
        table (string): the table name to use.
        session (object): The optional session object used to query the
            database. If omitted the MySQL Shell's current session will be used.

    """
    # Get hold of the global shell object
    import mysqlsh
    shell = mysqlsh.globals.shell

    if session is None:
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                  "function or connect the shell to a database")
            return
    
    uri_json = shell.parse_uri(session.get_uri())
    if uri_json['scheme'] == "mysql":
        print("This plugin requires a MySQL X connection !")
        return
    if schema is None:
        schema = session.get_current_schema()
        if schema is None:
            print("No schema specified neither one in use in the current session.")
            return
    else:
        try:
            schema = session.get_schema(schema)
        except:
            print("This schema name is invalid!")
            return
    if table is None:
        table = shell.prompt("Enter the name of the table to disable audit: ")
    try:
        table = schema.get_table(table)
    except:
        print("This table doesn't exist in the selected schema")
        return

    exist_type = check_for_audit(session, schema.name, table.name)
    if exist_type is None:
        print("There is no audit enabled on this table !")
        return

    
    trigger_stmt = """drop trigger if exists {}.{}_audit_insert""".format(schema.name, table.name)
    session.sql(trigger_stmt).execute()
    trigger_stmt = """drop trigger if exists {}.{}_audit_update""".format(schema.name, table.name)
    session.sql(trigger_stmt).execute()
    trigger_stmt = """drop trigger if exists {}.{}_audit_delete""".format(schema.name, table.name)
    session.sql(trigger_stmt).execute()

    if exist_type == "external":
        remove = shell.prompt("Do you also want to remove the external audit table and its content ? (y/N) ", {'defaultValue': 'n'})
        if remove.upper() == "Y":
            stmt = "drop table information_audit_log.{}_{}".format(schema.name, table.name)
            session.sql(stmt).execute()
            print("External audit table is now dropped.")
    else:
        remove = shell.prompt("Do you also want to remove the invisible audit_info column ? (y/N) ", {'defaultValue': 'n'})
        if remove.upper() == "Y":
            stmt = "alter table {}.{} drop column audit_info".format(schema.name, table.name)
            session.sql(stmt).execute()
            print("Internal audit_info invisible column is now removed.")
