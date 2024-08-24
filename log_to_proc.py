import json
import re

READ_PATH = "logs/dbt.log"
WRITE_PATH = "stored_proc.sql"
BEGINNING_STORED_PROC = """
ALTER SESSION SET USE_CACHED_RESULT=FALSE;

CREATE OR REPLACE PROCEDURE dbt_pipeline()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
"""
ENDING_STORED_PROC = """
END;
$$
;

CALL dbt_pipeline();

ALTER SESSION SET USE_CACHED_RESULT=TRUE;
"""


def remove_comments(input_string):
    pattern = r"/\*.*?\*/"
    return re.sub(pattern, "", input_string, flags=re.DOTALL)


def is_sql_query(obj):
    is_sql = "sql" in obj["data"].keys()
    is_node = (
        "node_info" in obj["data"].keys()
        and obj["data"]["node_info"]["resource_type"] in ["model", "test", "snapshot"]
        and obj["data"]["node_info"]["node_status"] != "compiling"
    )
    return is_sql and is_node


if __name__ == "__main__":
    objects = []
    with open(READ_PATH, "r") as f:
        for line in f:
            try:
                obj = json.loads(line)
            except Exception:
                print("error with line: " + line)
            else:
                if is_sql_query(obj):
                    sql = remove_comments(obj["data"]["sql"])
                    if sql[-1] != ";":
                        sql += ";"
                    objects.append(sql)

    with open(WRITE_PATH, "w") as f:
        f.write(str(BEGINNING_STORED_PROC))
        for obj in objects:
            f.write(obj)
        f.write(str(ENDING_STORED_PROC))
