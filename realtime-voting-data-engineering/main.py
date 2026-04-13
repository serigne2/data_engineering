import random
import psycopg2
import requests
import simplejson as json
from confluent_kafka import SerializingProducer
import time

BASE_URL = 'https://randomuser.me/api/?nat=gb'
PARTIES = ["Management Party", "Savior Party", "Tech Republic Party"]
random.seed(42)

RETRIES = 3  # nombre de tentatives API

def generate_voter_data(retries=5, delay=1):
    for attempt in range(retries):
        response = requests.get(BASE_URL)
        if response.status_code != 200:
            print(f"Erreur HTTP {response.status_code}, retry {attempt+1}/{retries}")
            time.sleep(delay)
            continue

        results = response.json().get('results', [])
        if results:
            user_data = results[0]
            return {
                "voter_id": user_data['login']['uuid'],
                "voter_name": f"{user_data['name']['first']} {user_data['name']['last']}",
                "date_of_birth": user_data['dob']['date'],
                "gender": user_data['gender'],
                "nationality": user_data['nat'],
                "registration_number": user_data['login']['username'],
                "address": {
                    "street": f"{user_data['location']['street']['number']} {user_data['location']['street']['name']}",
                    "city": user_data['location']['city'],
                    "state": user_data['location']['state'],
                    "country": user_data['location']['country'],
                    "postcode": user_data['location']['postcode']
                },
                "email": user_data['email'],
                "phone_number": user_data['phone'],
                "cell_number": user_data['cell'],
                "picture": user_data['picture']['large'],
                "registered_age": user_data['registered']['age']
            }
        else:
            print(f"Erreur : pas de votant retourné par l'API, retry {attempt+1}/{retries}")
            time.sleep(delay)

    # Si après tous les retries on n’a toujours rien
    raise Exception("Impossible de récupérer un votant après plusieurs tentatives")


def generate_candidate_data(candidate_number, total_parties, retries=RETRIES):
    gender_param = 'female' if candidate_number % 2 == 1 else 'male'
    for attempt in range(retries):
        try:
            response = requests.get(BASE_URL + f'&gender={gender_param}', timeout=5)
            if response.status_code == 200:
                results = response.json().get('results', [])
                if results:
                    user_data = results[0]
                    return {
                        "candidate_id": user_data['login']['uuid'],
                        "candidate_name": f"{user_data['name']['first']} {user_data['name']['last']}",
                        "party_affiliation": PARTIES[candidate_number % total_parties],
                        "biography": "A brief bio of the candidate.",
                        "campaign_platform": "Key campaign promises or platform.",
                        "photo_url": user_data['picture']['large']
                    }
                else:
                    print(f"Erreur API vide pour candidat, tentative {attempt+1}/{retries}")
            else:
                print(f"Erreur HTTP candidat {response.status_code}, tentative {attempt+1}/{retries}")
        except Exception as e:
            print(f"Exception API candidat: {e}, tentative {attempt+1}/{retries}")
        time.sleep(1)
    return None

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# Kafka Topics
voters_topic = 'voters_topic'
candidates_topic = 'candidates_topic'

def create_tables(conn, cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS candidates (
            candidate_id VARCHAR(255) PRIMARY KEY,
            candidate_name VARCHAR(255),
            party_affiliation VARCHAR(255),
            biography TEXT,
            campaign_platform TEXT,
            photo_url TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS voters (
            voter_id VARCHAR(255) PRIMARY KEY,
            voter_name VARCHAR(255),
            date_of_birth VARCHAR(255),
            gender VARCHAR(255),
            nationality VARCHAR(255),
            registration_number VARCHAR(255),
            address_street VARCHAR(255),
            address_city VARCHAR(255),
            address_state VARCHAR(255),
            address_country VARCHAR(255),
            address_postcode VARCHAR(255),
            email VARCHAR(255),
            phone_number VARCHAR(255),
            cell_number VARCHAR(255),
            picture TEXT,
            registered_age INTEGER
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS votes (
            voter_id VARCHAR(255) UNIQUE,
            candidate_id VARCHAR(255),
            voting_time TIMESTAMP,
            vote int DEFAULT 1,
            PRIMARY KEY (voter_id, candidate_id)
        )
    """)
    conn.commit()

def insert_voters(conn, cur, voter):
    cur.execute("""
        INSERT INTO voters (voter_id, voter_name, date_of_birth, gender, nationality, registration_number, 
                            address_street, address_city, address_state, address_country, address_postcode, 
                            email, phone_number, cell_number, picture, registered_age)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """,
    (voter["voter_id"], voter['voter_name'], voter['date_of_birth'], voter['gender'],
     voter['nationality'], voter['registration_number'], voter['address']['street'],
     voter['address']['city'], voter['address']['state'], voter['address']['country'],
     voter['address']['postcode'], voter['email'], voter['phone_number'],
     voter['cell_number'], voter['picture'], voter['registered_age'])
    )
    conn.commit()

if __name__ == "__main__":
    conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
    cur = conn.cursor()
    producer = SerializingProducer({'bootstrap.servers': 'localhost:9092', })
    create_tables(conn, cur)

    # Create candidates if not exists
    cur.execute("SELECT * FROM candidates")
    candidates = cur.fetchall()
    if len(candidates) == 0:
        for i in range(3):
            candidate = generate_candidate_data(i, 3)
            if candidate:
                cur.execute("""
                    INSERT INTO candidates (candidate_id, candidate_name, party_affiliation, biography, campaign_platform, photo_url)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    candidate['candidate_id'], candidate['candidate_name'], candidate['party_affiliation'], 
                    candidate['biography'], candidate['campaign_platform'], candidate['photo_url']
                ))
                conn.commit()

    # Generate exactly 1000 voters
    i = 0
    while i < 1000:
        voter_data = generate_voter_data()
        if voter_data is None:
            continue  # retry si API a échoué
        insert_voters(conn, cur, voter_data)
        producer.produce(
            voters_topic,
            key=voter_data["voter_id"],
            value=json.dumps(voter_data),
            on_delivery=delivery_report
        )
        producer.flush()
        print(f'Produced voter {i}, data: {voter_data}')
        i += 1
