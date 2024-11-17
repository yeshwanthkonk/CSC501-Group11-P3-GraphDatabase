import re
import os
import json
import pandas as pd
from urllib.parse import urlparse
from multiprocessing import Pool, cpu_count
from neo4j import GraphDatabase, basic_auth

# Load a large JSON file in chunks for efficiency
def load_json_file(file_path, chunksize=100000):
    with open(file_path, 'r') as file:
        while True:
            lines = list(file.readlines(chunksize))
            if not lines:
                break
            # Convert lines to JSON format
            records = [json.loads(line[:-1]) for line in lines]
            yield pd.DataFrame(records)


# Define keywords for filtering
media_keywords = [
    # General Lockdown & Restrictions
    'lockdown', 'shutdown', 'restriction', 'curfew', 'quarantine', 'isolation', 'closure',


    # Social Distancing & Public Health Measures
    'distancing', 'social distancing', 'mask mandate', 'face covering', 'hand hygiene',
    'sanitization', 'public health measure',


    # Compliance & Defiance
    'compliance', 'adherence', 'defiance', 'protest', 'violation', 'penalty', 'fines', 'enforcement',


    # Policy Announcements & Government Actions
    'policy', 'guideline', 'advisory', 'regulation', 'announcement', 'directive', 'protocol',
    'government order',


    # Gatherings & Social Interactions
    'gathering', 'assembly', 'meet', 'congregate', 'event', 'celebration', 'crowd',
    'socializing', 'party',


    # Easing of Restrictions
    'easing', 'unlock', 'reopen', 'reopening', 'return to normal', 'phased reopening',


    # Behavioral Response
    'panic buying', 'hoarding', 'travel ban', 'stay-at-home', 'work from home', 'remote work',
    'online classes', 'vaccination drive'
]




media_keywords = [f" {each} " for each in media_keywords]


# Compile regex pattern for efficiency
media_keywords_pattern = re.compile('|'.join(media_keywords), re.IGNORECASE)


# Function to filter records based on keywords
def filter_records(df_chunk):
    return df_chunk[
        df_chunk['pre'].str.contains(media_keywords_pattern, na=False) |
        df_chunk['verb'].str.contains(media_keywords_pattern, na=False) |
        df_chunk['post'].str.contains(media_keywords_pattern, na=False)
    ]


# Multiprocessing function to handle chunks
def process_chunk(chunk):
    filtered_chunk = filter_records(chunk)
    return filtered_chunk


# Main function to load, filter, and parallelize the execution
def parallel_filtering(json_file_path):
    filtered_data = []


    # Create a pool of worker processes
    pool = Pool(cpu_count())


    # Load and process chunks in parallel
    for chunk in load_json_file(json_file_path):
        result = pool.apply_async(process_chunk, (chunk,))
        filtered_data.append(result.get())


    # Close and join the pool
    pool.close()
    pool.join()


    # Concatenate all filtered chunks
    filtered_df = pd.concat(filtered_data, ignore_index=True)
    return filtered_df


# Specify the path to your JSON file
json_file_path = '/content/grg-covid-20200101-20200713.vcn.json'


# Run the parallel filtering process
filtered_df = parallel_filtering(json_file_path)


# Display the number of filtered records and a preview of the data
print(f"Filtered Record Count: {len(filtered_df)}")
# print(filtered_df.head())

# ---------------------------------
# Load the filtered data (assumed to be pre-loaded in `filtered_df`)
# Example columns: ['date', 'pre', 'verb', 'post', 'urls']


# Sample data initialization (if not already available)
filtered_df['date'] = pd.to_datetime(filtered_df['date'])


# 1. Extracting Media Source from URLs (if available)
def extract_media_source(urls):
    if isinstance(urls, list) and len(urls) > 0:
        parsed_url = urlparse(urls[0]['url'])
        return parsed_url.netloc.split('.')[1] if parsed_url.netloc else None
    return None


filtered_df['media_source'] = filtered_df['urls'].apply(extract_media_source)


# 2. Classifying Statement Types (e.g., Compliance, Defiance)
compliance_keywords = [
    # Original Keywords
    'comply', 'adherence', 'follow', 'guidelines', 'support',


    # Expanded Keywords
    'abide', 'obey', 'conform', 'respect', 'cooperate', 'adhere',
    'agree', 'commit', 'observe', 'submit', 'align', 'accommodate',
    'stick to', 'complying', 'compliant', 'consent',
    'endorse', 'uphold', 'accept', 'embrace', 'commitment',
    'responsible', 'diligent', 'assent', 'positive response', 'approval',
    'reinforce', 'encourage', 'compliance',
    'stay safe', 'practice social distancing', 'wear masks',
    'isolation', 'quarantine', 'vaccination'
]


defiance_keywords = [
    # Original Keywords
    'defy', 'protest', 'violate', 'ignore', 'resist',


    # Expanded Keywords
    'disobey', 'oppose', 'reject', 'refuse', 'rebel', 'disregard',
    'non-compliance', 'flout', 'breach', 'transgress', 'challenge',
    'fight', 'stand against', 'denounce', 'condemn', 'criticize',
    'dispute', 'undermine', 'revolt', 'boycott', 'return down', 'dismiss',
    'anti-mask', 'anti-lockdown', 'anti-vaccine', 'push back',
    'backlash', 'non-adherence', 'break rules',
    'neglect', 'non-cooperation', 'rebellion', 'defiant', 'disobeying',
    'contravene', 'rebuff', 'object to', 'confront', 'outcry',
    'question', 'resistance', 'flouting'
]




def classify_statement(text):
    # compliance_keywords = ['comply', 'adherence', 'follow', 'guidelines', 'support']
    # defiance_keywords = ['defy', 'protest', 'violate', 'ignore', 'resist']




    if any(re.search(rf'\b{word}\b', text, re.IGNORECASE) for word in defiance_keywords):
        return 'defiance'
    elif any(re.search(rf'\b{word}\b', text, re.IGNORECASE) for word in compliance_keywords):
        return 'compliance'
    else:
        return 'neutral'


# Combine 'pre', 'verb', and 'post' to classify statements
filtered_df['full_text'] = filtered_df['pre'] + ' ' + filtered_df['verb'] + ' ' + filtered_df['post']
filtered_df['statement_type'] = filtered_df['full_text'].apply(classify_statement)


# 3. Tagging Topics (Lockdown, Social Distancing, etc.)


reopening_keywords = [
    # Original Keywords
    'reopen', 'reopening', 'easing', 'unlock',


    # Expanded Keywords
    'resume operations', 'back to business', 'restart', 'lifting restrictions',
    'open doors', 'back to normal', 'resumption', 'return to work',
    'business as usual', 'phased reopening', 'economic restart', 'recovery plan',
    'reopening plan', 'gradual reopening', 'business reopening',
    'back in action', 'normalcy', 'reboot', 'kickstart', 'revive economy',
    'relaunch', 'reinstating services', 'restoring operations', 'open up again',
    'end of lockdown', 'exit strategy', 'back to office', 'restart activities',
    'school reopening', 'return to school', 'tourism reopening', 'travel resumes',
    'lifting lockdown', 'easing measures', 'relaxing restrictions',
    'stores reopening', 'mall reopening', 'restaurant reopening',
    'back to dining', 'public spaces reopen', 'event venues reopen',
    'resuming flights', 'border reopening', 'resuming services',
    'hotel reopening', 'gym reopening', 'fitness center reopening',
    'cinema reopening', 'salon reopening', 'parks reopening',
    'end of quarantine', 'post-lockdown', 'get back on track',
    'reopen economy', 'open for business', 'reopen society'
]


vaccination_keywords = [
    # Original Keywords
    'vaccination', 'vaccine', 'immunization', 'vaccinated',


    # Expanded Keywords
    'jab', 'shot', 'inoculation',
    'booster shot', 'dose', 'first dose', 'second dose',
    'herd immunity', 'anti-vaccine',
    'immune', 'covid shot', 'booster dose', 'roll up your sleeve'
]




def tag_topic(text):
    topic_keywords = {
    'lockdown': ['lockdown', 'shutdown', 'curfew'],
    'social_distancing': ['distancing', 'social distancing', 'six feet'],
    'reopening': reopening_keywords,  # Using the expanded list
    'guidelines': ['policy', 'guideline', 'regulation', 'advisory'],
    'vaccination': vaccination_keywords  # Using the expanded list
    }


    for topic, keywords in topic_keywords.items():
        if any(re.search(rf'\b{word}\b', text, re.IGNORECASE) for word in keywords):
            return topic
    return 'general'


filtered_df['topic'] = filtered_df['full_text'].apply(tag_topic)


# 4. Inferring Public Behavior from Statements
def infer_behavior(statement_type):
    if statement_type == 'compliance':
        return 'Public Compliance'
    elif statement_type == 'defiance':
        return 'Public Defiance'
    else:
        return 'Neutral Behavior'


filtered_df['public_behavior'] = filtered_df['statement_type'].apply(infer_behavior)


# Final Enriched DataFrame
enriched_df = filtered_df[['date', 'media_source', 'full_text', 'statement_type', 'topic', 'public_behavior']]
print(enriched_df.head())

# -----------------------------------

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
