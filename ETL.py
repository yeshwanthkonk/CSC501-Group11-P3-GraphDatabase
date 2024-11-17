from neo4j import GraphDatabase, basic_auth


# Configure Neo4j connection
uri = "neo4j+s://<id>.databases.neo4j.io"
username = "neo4j"
password = "<password>ztMRrDgo"
driver = GraphDatabase.driver(uri, auth=basic_auth(username, password))


# Function to create nodes and relationships in Neo4j
def create_neo4j_entities(tx, row):
    print(row)
    # Create Statement Node
    tx.run("""
        MERGE (s:Statement {text: $text, statement_type: $statement_type})
        MERGE (t:Topic {name: $topic})
        MERGE (m:MediaSource {name: $media_source})
        MERGE (b:PublicBehavior {type: $public_behavior})
        MERGE (d:Date {date: $date})


        MERGE (s)-[:BELONGS_TO]->(t)
        MERGE (m)-[:AMPLIFIED]->(s)
        MERGE (s)-[:TRIGGERED]->(b)
        MERGE (m)-[:PUBLISHED_ON]->(d)
    """,
    text=row['full_text'],
    statement_type=row['statement_type'],
    topic=row['topic'],
    media_source=row['media_source'],
    public_behavior=row['public_behavior'],
    date=row['date'])


# Load Data into Neo4j
with driver.session() as session:
    for index, row in enriched_df.iterrows():
        session.write_transaction(create_neo4j_entities, row)


print("Data successfully loaded into Neo4j.")
