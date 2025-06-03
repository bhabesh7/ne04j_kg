from neo4j import GraphDatabase

class MetadataQuery:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def get_all_datasets_and_files(self):
        with self.driver.session() as session:
            result = session.run("""
                MATCH (ds:Dataset)-[:CONTAINS]->(df:DataFile)
                RETURN ds.name AS dataset, df.name AS file, df.type AS type
            """)
            return [record.data() for record in result]

    def get_features_for_file(self, file_name):
        with self.driver.session() as session:
            result = session.run("""
                MATCH (df:DataFile {name: $file_name})-[:HAS_FEATURE]->(f:Feature)
                OPTIONAL MATCH (f)-[:BELONGS_TO]->(c:Category)
                OPTIONAL MATCH (f)-[:MEASURED_IN]->(u:Unit)
                RETURN f.name AS feature, c.name AS category, u.name AS unit, u.description AS unit_description
            """, file_name=file_name)
            return [record.data() for record in result]

    def get_files_by_type(self, file_type):
        with self.driver.session() as session:
            result = session.run("""
                MATCH (df:DataFile)
                WHERE df.type = $file_type
                RETURN df.name AS file
            """, file_type=file_type)
            return [record["file"] for record in result]

    def get_all_units(self):
        with self.driver.session() as session:
            result = session.run("""
                MATCH (u:Unit)
                RETURN u.name AS unit, u.description AS description
            """)
            return [record.data() for record in result]

if __name__ == "__main__":
    URI = "bolt://localhost:7687"
    USER = "neo4j"
    PASSWORD = "cool@1983"  # Match this with password in docker compose yaml

    query = MetadataQuery(URI, USER, PASSWORD)

    print(" All Datasets and Files:")
    for record in query.get_all_datasets_and_files():
        print(record)

    print("\n Features for File 'train_FD001':")
    for record in query.get_features_for_file("train_FD001"):
        print(record)

    # print("\n All RUL Files:")
    # for file in query.get_files_by_type("RUL"):
    #     print(file)

    print("\n Features for File 'RUL_FD001':")
    for record in query.get_features_for_file("RUL_FD001"):
        print(record)

    print("\n All Units:")
    for unit in query.get_all_units():
        print(unit)

    query.close()
