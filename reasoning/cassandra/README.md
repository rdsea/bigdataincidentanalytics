Documentation to be done

## Deployment

**When running Cassandra for the first time, the keyspace and table must be set up**.

1. **Connect to Cassandra**'s built-in `CQLSH` shell. 

   * If the database is running in a docker container, you need to first "step" into the container with its bash open by executing `docker exec -it cassandra /bin/bash` in your terminal (here, `cassandra` is the name of the docker container).

   * Execute `cqlsh`. You should see something like this:

   ```bash
   Connected to Test Cluster at 127.0.0.1:9042.
   [cqlsh 5.0.1 | Cassandra 3.0.19 | CQL spec 3.4.0 | Native protocol v4]
   Use HELP for help.
   cqlsh> 
   ```

2. **Create keyspace**. Execute 

   ```cassandra
   CREATE KEYSPACE IF NOT EXISTS incident_analytics
   WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
   ```

   This creates a simple Cassandra keyspace with the name `incident_analytics`.

3. (Optional) To verify that the keyspace has been successfully created, run `DESCRIBE keyspaces;`.

4. **Create signals table**. Run

   ```cassandra
   CREATE TABLE IF NOT EXISTS signals(
   uuid uuid, 
   timestamp timestamp, 
   signal_name text, 
   pipeline_component text, 
   PRIMARY KEY(uuid)
   );
   ```

5. (Optional) To verify the table's existence, simply run `select * from incident_analytics.signals;` to display all the rows. If this is the first time setup, there will be no records displayed obviously.



