from neo4j import GraphDatabase

# Neo4j DB config
uri = "bolt://localhost:7687"
username = "neo4j"
password = "cool@1983"  # Match this with password in  docker compose yaml

class MetadataQuery:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def list_all_datasets_and_files(self):
        with self.driver.session() as session:
            result = session.run("""
                MATCH (ds:Dataset)-[:CONTAINS]->(df:DataFile)
                RETURN ds.name AS dataset, df.name AS file, df.type AS type
                ORDER BY ds.name, df.name
            """)
            return result.data()

    def get_features_for_file(self, file_name):
        with self.driver.session() as session:
            result = session.run("""
                MATCH (df:DataFile {name: $file_name})-[:HAS_FEATURE]->(f)-[:BELONGS_TO]->(c)
                OPTIONAL MATCH (f)-[:MEASURED_IN]->(u)
                RETURN f.name AS feature, c.name AS category, u.name AS unit
                ORDER BY category, feature
            """, file_name=file_name)
            return result.data()

    def get_associated_files_for_rul(self, rul_file_name):
        with self.driver.session() as session:
            result = session.run("""
                MATCH (rul:DataFile {name: $rul})-[:ASSOCIATED_WITH]->(df:DataFile)
                RETURN df.name AS associated_file, df.type AS file_type
            """, rul=rul_file_name)
            return result.data()

# Example usage
if __name__ == "__main__":
    mq = None
    try:
        mq = MetadataQuery(uri, username, password)

        print("\nAll Datasets and Files:")
        for record in mq.list_all_datasets_and_files():
            print(record)

        print("\nFeatures for 'train_FD001':")
        for record in mq.get_features_for_file("train_FD001"):
            print(record)

        print("\n Files associated with 'RUL_FD001':")
        for record in mq.get_associated_files_for_rul("RUL_FD001"):
            print(record)

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if mq:
            mq.close()
