import tweepy
import json
import pandas as pd
import time

# === API-Zugang ===
api_token = "<<BEARER TOKEN>>"
client = tweepy.Client(bearer_token=api_token, wait_on_rate_limit=True)

# === Suchparameter ===
search_term = "munich -is:retweet lang:en"
max_results_per_call = 100
output_file = "fetched_tweets.json"
min_tweets = 3000
max_tweets = 3500

# === Ergebnis-Container ===
collected_data = []
all_raw_tweets = []

# === Paginierung vorbereiten ===
paginator = tweepy.Paginator(
    client.search_recent_tweets,
    query=search_term,
    tweet_fields=["entities", "context_annotations"],
    user_fields=["id", "username", "name"],
    expansions=["author_id"],
    max_results=max_results_per_call
)

print("Starte Tweet-Sammlung...")

# === Tweets sammeln ===
for page in paginator:
    user_lookup = {}

    # Benutzer-Infos auflösen
    if page.includes and "users" in page.includes:
        user_lookup = {user.id: user.username for user in page.includes["users"]}

    # Tweets verarbeiten
    if page.data is None:
        print(" Keine weiteren Tweets gefunden.")
        break

    for tweet in page.data:
        all_raw_tweets.append(tweet)

        user = user_lookup.get(tweet.author_id, "unknown")

        collected_data.append({
            "id": tweet.id,
            "username": user,
            "author_id": tweet.author_id,
            "text": tweet.text,
            "entities": tweet.entities,
            "context_annotations": tweet.context_annotations,
            "created_at": tweet.created_at.isoformat() if tweet.created_at else None
        })

    print(f"Zwischenstand: {len(collected_data)} Tweets gesammelt...")

    # Abbruchbedingung bei Erreichen von max_tweets
    if len(collected_data) >= max_tweets:
        print(f" Maximale Grenze von {max_tweets} Tweets erreicht.")
        break

    time.sleep(1)

# === Überprüfen ob Mindestgrenze erreicht wurde ===
if len(collected_data) < min_tweets:
    print(f" Nur {len(collected_data)} Tweets gefunden (weniger als {min_tweets}).")
else:
    # Als JSON speichern
    df = pd.DataFrame(collected_data)
    df.to_json(output_file, orient="records", indent=2, force_ascii=False)
    print(f"\n {len(df)} Tweets erfolgreich gespeichert in '{output_file}'.")
