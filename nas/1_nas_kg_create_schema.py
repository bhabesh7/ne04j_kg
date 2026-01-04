from neo4j_config import driver, close_driver

# from neo4j import GraphDatabase

# NEO4J_URI = "bolt://localhost:7687"
# NEO4J_USER = "neo4j"
# NEO4J_PASSWORD = "password"  # change if needed

# driver = GraphDatabase.driver(
#     NEO4J_URI,
#     auth=(NEO4J_USER, NEO4J_PASSWORD)
# )

# def close_driver():
#     driver.close()

def create_constraints(tx):
    tx.run("""
    CREATE CONSTRAINT arch_name IF NOT EXISTS
    FOR (a:Architecture)
    REQUIRE a.name IS UNIQUE
    """)
    
    tx.run("""
    CREATE CONSTRAINT layer_name IF NOT EXISTS
    FOR (l:Layer)
    REQUIRE l.name IS UNIQUE
    """)
    
    tx.run("""
    CREATE CONSTRAINT dataset_name IF NOT EXISTS
    FOR (d:Dataset)
    REQUIRE d.name IS UNIQUE
    """)
    
    tx.run("""
    CREATE CONSTRAINT hardware_name IF NOT EXISTS
    FOR (h:Hardware)
    REQUIRE h.name IS UNIQUE
    """)

if __name__ == "__main__":
    with driver.session() as session:
        session.execute_write(create_constraints)
    close_driver()
    print("Schema created successfully")
