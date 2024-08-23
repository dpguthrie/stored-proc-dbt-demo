import json
import re

READ_PATH = "logs/dbt.log"
WRITE_PATH = "stored_proc.sql"
BEGINNING_STORED_PROC = """
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

ALTER SESSION SET USE_CACHED_RESULT=FALSE;

CALL dbt_pipeline();
"""


def remove_comments(input_string):
    pattern = r"/\*.*?\*/"
    return re.sub(pattern, "", input_string, flags=re.DOTALL)


if __name__ == "__main__":
    objects = []
    with open(READ_PATH, "r") as f:
        for line in f:
            try:
                obj = json.loads(line)
            except Exception:
                print("error with line: " + line)
            else:
                if "sql" in obj["data"].keys():
                    sql = remove_comments(obj["data"]["sql"])
                    if sql[-1] != ";":
                        sql += ";"
                    if not sql.strip().startswith("show"):
                        objects.append(sql)

    with open(WRITE_PATH, "w") as f:
        f.write(str(BEGINNING_STORED_PROC))
        for obj in objects:
            f.write(obj)
        f.write(str(ENDING_STORED_PROC))
