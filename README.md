# dbt to Stored Proc (Snowflake)

The purpose of this repo is to demonstrate the benefit of using dbt over a stored procedure, specifically from a performance perspective (there's a lot more we can get into but we'll save that for another day).

dbt has a concept of [threads](https://docs.getdbt.com/docs/running-a-dbt-project/using-threads), which means that dbt can work on up to n number of nodes (models, tests, snapshots) at once without violating the pipeline's dependencies.  Said another way, dbt can run things in parallel when the DAG allows it.

Contrast that with a stored procedure approach, which does things in a sequential manner from top to bottom, this becomes a pretty easy exercise in demonstrating cost savings in your warehouse.

## Instructions

1. Delete your log file (if it exists).  This will be found at `logs/dbt.log` unless you've changed the configuration within your `dbt_project.yml` file.

2. Run a dbt command.  The command should run a good portion of your project (or even all of it!).  The more you run, the more cost savings that you'll be able to demonstrate.

```bash
dbt build -s +fct_order_items+
```

**Note the time it took to run this command - you'll need it later to compare to the stored procedure!**

3. Run the script in this repo from the root of your dbt project:

```bash
python log_to_proc.py
```

If you've changed the location of your log file, pass that as a flag when running the script:

```bash
python log_to_proc.py --log-path my_logs/dbt.log
```

4. Copy/paste the code output from `stored_proc.sql` into your Snowflake Editor and execute all of the statements.  Take note of the time it takes to run your stored procedure.

**The output to the .sql file contains code to set the `USE_CACHED_RESULT` to `FALSE` so that the stored procedure is run without any of the cached results from the prior dbt run.**

5. Now compare!