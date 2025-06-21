from neo4j import GraphDatabase

# Connection details
uri = "bolt://localhost:7687"
username = "neo4j"
password = "cool@1983"  # Match with docker compose yaml

# Function to clear all relevant metadata nodes and relationships
def clean_metadata(tx):
    # You can narrow the deletion scope by adjusting labels below
    query = """
    MATCH (n)
    WHERE any(label IN labels(n) WHERE label IN ['Dataset','DataFile', 'Feature', 'Category', 'Unit', 'Asset', 'Storage'])
    DETACH DELETE n
    """
    tx.run(query)

# Main execution
def main():
    driver = GraphDatabase.driver(uri, auth=(username, password))
    with driver.session() as session:
        session.execute_write(clean_metadata)
        print("Metadata nodes and relationships have been deleted.")
    driver.close()

if __name__ == "__main__":
    main()
