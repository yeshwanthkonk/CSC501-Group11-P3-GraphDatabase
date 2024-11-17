from neo4j import GraphDatabase, basic_auth
import networkx as nx
import matplotlib.pyplot as plt


# Connect to Neo4j
uri = "neo4j+s://<user>.databases.neo4j.io"  # Adjust to your Neo4j instance URI
username = "neo4j"  # Your Neo4j username
password = "<password>rDgo"  # Your Neo4j password
driver = GraphDatabase.driver(uri, auth=basic_auth(username, password))


# Function to run the query and get results
def get_lockdown_policy_influence(driver):
    query = """
    MATCH (s:Statement)-[:BELONGS_TO]->(t:Topic {name: 'lockdown'})
    MATCH (m:MediaSource)-[:AMPLIFIED]->(s)
    MATCH (s)-[:TRIGGERED]->(b:PublicBehavior {type: 'Public Defiance'})
    MATCH (m)-[:PUBLISHED_ON]->(d:Date)
    WITH t, m, s, b, d, COUNT(s) AS mediaCoverage
    RETURN t.name AS Topic,
           m.name AS MediaSource,
           s.text AS Statement,
           b.type AS PublicBehavior,
           d.date AS Date,
           mediaCoverage
    ORDER BY mediaCoverage DESC
    LIMIT 20
    """
    with driver.session() as session:
        result = session.run(query)
        records = [record for record in result]
        return records


# Get the query results
results = get_lockdown_policy_influence(driver)


# Create a graph using NetworkX
G = nx.DiGraph()


# Add nodes and edges based on the results
for record in results:
    topic = record.get('Topic', 'Unknown Topic')
    media_source = record.get('MediaSource', 'Unknown Source')
    statement = record.get('Statement', 'No Statement')
    public_behavior = record.get('PublicBehavior', 'Unknown Behavior')
    date = record.get('Date', 'No Date')
    media_coverage = record.get('mediaCoverage', 0)


    # Add nodes for media source, statement, topic, and public behavior
    G.add_node(media_source, type='MediaSource')
    G.add_node(statement, type='Statement', date=date)
    G.add_node(public_behavior, type='PublicBehavior')
    G.add_node(topic, type='Topic')


    # Add edges with labels
    G.add_edge(media_source, statement, label='AMPLIFIED')
    G.add_edge(statement, public_behavior, label='TRIGGERED')
    G.add_edge(statement, topic, label='BELONGS_TO')


# Check if the graph has nodes
if len(G.nodes) == 0:
    print("Graph is empty. No visualization to show.")
else:
    # Draw the graph
    plt.figure(figsize=(14, 10))


    # Assign different colors based on node types
    color_map = []
    for node in G:
        if G.nodes[node]['type'] == 'MediaSource':
            color_map.append('skyblue')
        elif G.nodes[node]['type'] == 'Statement':
            color_map.append('lightgreen')
        elif G.nodes[node]['type'] == 'PublicBehavior':
            color_map.append('red')
        elif G.nodes[node]['type'] == 'Topic':
            color_map.append('gold')


    # Create layout for visualization
    pos = nx.spring_layout(G, k=0.6)


    # Draw nodes and edges
    nx.draw_networkx_nodes(G, pos, node_color=color_map, node_size=600, alpha=0.8)
    nx.draw_networkx_edges(G, pos, edgelist=G.edges(), arrows=True, edge_color='gray')
    nx.draw_networkx_labels(G, pos, font_size=8, font_weight='bold')


    # Add edge labels
    edge_labels = nx.get_edge_attributes(G, 'label')
    nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, font_color='blue')


    # Display the graph
    plt.title("Media Influence on Lockdown Policies leading to Defiance Public Behavior", fontsize=15)
    plt.axis('off')
    plt.show()
