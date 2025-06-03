# neo4j_kg
repo for running neo4j in docker compose and also ingest data

Create Virtual Environment
----------------------------
1. Create a virtual env using VS code
2. Activate environment. Ensure that you see (.venv) in the terminal command line ( or the virtual env name that u provided). 
3. pip install -r requirements.txt


Running Neo4J
--------------
1. Open terminal in VS Code.
2. Be in the same directory as the docker-compose.yml file
3. Type --> sudo docker compose up
4. Observe the terminal start the neo4j services
5. Neo4j Browser: http://localhost:7474

About the code files (prereq: Neo4j must be running)
---------------------
clean_metadata --> cleans up the data in neo4j DB
ingest_metadata --> ingests the relation ships , features , columns into the KG.
query_metadata.py --> query the data ingested apriori