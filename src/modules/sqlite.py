import sqlite3
from datetime import datetime
import json

class SQLiteManager:
    """
    A class to manage SQLite connections and queries
    """

    def __init__(self):
        self.conn = None
        self.cur = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()

    def connect_with_url(self, url):
        self.conn = sqlite3.connect(url)
        self.cur = self.conn.cursor()

    def close(self):
        if self.conn:
            self.conn.close()

    def run_sql(self, sql) -> str:
        """
        Run a SQL query against the SQLite database
        """
        self.cur.execute(sql)
        columns = [desc[0] for desc in self.cur.description]
        res = self.cur.fetchall()

        list_of_dicts = [dict(zip(columns, row)) for row in res]

        json_result = json.dumps(list_of_dicts, indent=4, default=self.datetime_handler)

        return json_result

    def datetime_handler(self, obj):
        """
        Handle datetime objects when serializing to JSON.
        """
        if isinstance(obj, datetime):
            return obj.isoformat()
        return str(obj)  # or just return the object unchanged, or another default value

    def get_table_definition(self, table_name):
        """
        Generate the 'create' definition for a table
        """

        get_def_stmt = """
        SELECT sql FROM sqlite_master
        WHERE name = ? AND type='table';
        """
        self.cur.execute(get_def_stmt, (table_name,))
        return self.cur.fetchone()[0]

    def get_all_table_names(self):
        """
        Get all table names in the database
        """
        get_all_tables_stmt = (
            "SELECT name FROM sqlite_master WHERE type='table';"
        )
        self.cur.execute(get_all_tables_stmt)
        return [row[0] for row in self.cur.fetchall()]

    def get_table_definitions_for_prompt(self):
        """
        Get all table 'create' definitions in the database
        """
        table_names = self.get_all_table_names()
        definitions = []
        for table_name in table_names:
            definitions.append(self.get_table_definition(table_name))
        return "\n\n".join(definitions)

    def get_table_definition_map_for_embeddings(self):
        """
        Creates a map of table names to table definitions
        """
        table_names = self.get_all_table_names()
        definitions = {}
        for table_name in table_names:
            definitions[table_name] = self.get_table_definition(table_name)
        return definitions

    def get_related_tables(self, table_list, n=2):
        """
        Get tables that have foreign keys referencing the given table
        """

        related_tables_dict = {}

        for table in table_list:
            # Query to fetch tables that have foreign keys referencing the given table
            self.cur.execute(
                """
                SELECT 
                    tbl_name AS table_name
                FROM 
                    sqlite_master 
                WHERE 
                    sql LIKE '%FOREIGN KEY%(%s)%'
                LIMIT ?;
                """,
                (table, n),
            )

            related_tables = [row[0] for row in self.cur.fetchall()]

            # Query to fetch tables that the given table references
            self.cur.execute(
                """
                SELECT 
                    tbl_name AS referenced_table_name
                FROM 
                    sqlite_master 
                WHERE 
                    sql LIKE '%REFERENCES %s(%'
                LIMIT ?;
                """,
                (table, n),
            )

            related_tables += [row[0] for row in self.cur.fetchall()]

            related_tables_dict[table] = related_tables

        # convert dict to list and remove dups
        related_tables_list = []
        for table, related_tables in related_tables_dict.items():
            related_tables_list += related_tables

        related_tables_list = list(set(related_tables_list))

        return related_tables_list
    
