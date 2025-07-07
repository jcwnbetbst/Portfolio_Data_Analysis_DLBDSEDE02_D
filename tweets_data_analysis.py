# -----------------------------
# Analyse von Twitter-Daten
# -----------------------------

# Bibliotheken für Datenverarbeitung und Analyse
import tweepy
import json
import collections
import re

# NLP-Bibliotheken
import nltk
from nltk.tokenize import TweetTokenizer
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
import string

# Datenstrukturierung
import numpy as np
import pandas as pd

# Gensim für LSA und Tf-idf
import gensim
from gensim.models import TfidfModel
from gensim.corpora import Dictionary
from gensim import models
from gensim.utils import simple_preprocess
from gensim.models.coherencemodel import CoherenceModel

# Visualisierung
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt
import plotly.graph_objs as go
import plotly.io as pio
pio.renderers.default = "notebook_connected"

# Farbige Konsolenausgabe
from termcolor import colored


# NLTK Bibliotheken Check

print("Haben Sie bereits die NLTK Bibliotheken heruntergeladen? j/n")
while True:
    answer_nltk = input("Ihre Antwort: ").strip().lower()

    if answer_nltk == "n":
        print("\nBitte laden sie die Bibliotheken über das GUI-Fenster runter.")
        nltk.download()
        print("\nDie Analyse fährt nun fort.")
        break
    
    elif answer_nltk == "j":
        print("\nVielen Dank für die Bestätigung. Die Analyse fährt nun fort.")
        break

    else:
        print("Ungültige Eingabe. Bitte geben Sie 'j' oder 'n' eingeben.")
        

# Konsolenfarben definieren
class color:
    CYAN = '\033[96m'
    END = '\033[0m'

# Funktion zur Entfernung von Emojis
def remove_emoji(text):
    emoji_pattern = re.compile("["
        u"\U00002700-\U000027BF"
        u"\U0001F600-\U0001F64F"
        u"\U00002600-\U000026FF"
        u"\U0001F300-\U0001F5FF"
        u"\U0001F900-\U0001F9FF"
        u"\U0001FA70-\U0001FAFF"
        u"\U0001F680-\U0001F6FF"
        "]+", re.UNICODE)
    return re.sub(emoji_pattern, '', text)

# Funktion zum Extrahieren von Hashtags
def extract_hashtags(text):
    regex = r"#(\w+)"
    return re.findall(regex, text)

# Funktion zur Tokenisierung
def tokenize(texts):
    for text in texts:
        yield tokenizer.tokenize(str(text))


# Funktion zur Lemmatisierung einer Liste von Strings
def lemmatize_text(text_list):
    lemmatized_texts = []
    for text in text_list:
        tokens = tokenizer.tokenize(text)
        lemmatized = [lemmatizer.lemmatize(token) for token in tokens]
        lemmatized_texts.append(" ".join(lemmatized))
    return lemmatized_texts

# Funktion zur Entfernung von Stoppwörtern
def remove_stopwords(texts):
    return [[word for word in simple_preprocess(str(doc)) if word not in stop_words] for doc in texts]

# Funktion zur Berechnung der uMass-Coherence
def coherence_umass(corpus, dictionary, k):
    model = models.LsiModel(corpus=corpus_tfidf, id2word=dictionary, num_topics=k)
    cm = CoherenceModel(model=model, corpus=corpus, dictionary=dictionary, coherence='u_mass')
    return cm.get_coherence()

# -----------------------------
# Hauptlogik
# -----------------------------

# Datensatz laden
df = pd.read_json('fetched_tweets.json')

# Tweets bereinigen
df['text'] = df['text'].str.lower()
df['text'] = df['text'].apply(lambda x: re.sub(r'http\S+', '', x))
df['text'] = df['text'].apply(remove_emoji)

# Hashtag-Analyse
print(color.CYAN + "\nTop 10 Hashtags:\n" + color.END)
all_hashtags = []
for text in df['text']:
    all_hashtags.extend(extract_hashtags(text))
hashtag_counts = collections.Counter(all_hashtags)
for tag, count in hashtag_counts.most_common(10):
    print(f"{tag}: {count}")


# Aktivste Nutzer
print(color.CYAN + "\nTop 10 Nutzer:\n" + color.END)
top_users = df['username'].value_counts().head(10)
for user, count in top_users.items():
    print(f"{user}: {count}")


# Stopwords definieren
stop_words = stopwords.words('english') + list(string.punctuation)
stop_words += [str(i) for i in range(10)] + ['rt', 'via', 'munich']

# NLP-Vorverarbeitung
tokenizer = TweetTokenizer(preserve_case=False, strip_handles=True, reduce_len=True)
lemmatizer = WordNetLemmatizer()
data = df['text'].tolist()
data_lemmatized = lemmatize_text(data)
tokenized = list(tokenize(data_lemmatized))
filtered = remove_stopwords(tokenized)

# Tf-idf und LSA vorbereiten
dictionary = Dictionary(filtered)
corpus = [dictionary.doc2bow(text) for text in filtered]
tfidf_model = TfidfModel(corpus)
corpus_tfidf = tfidf_model[corpus]

# Topic Coherence analysieren
print(color.CYAN + "\nThemenkohärenz (uMass):\n" + color.END)
topic_range = np.arange(1, 101)
umass_values = []
for k in topic_range:
    score = coherence_umass(corpus, dictionary, k)
    umass_values.append(score)
    print(f"{k}: {score}")

# Beste Topic-Anzahl ermitteln (oder auf 5 setzen)
df_umass = pd.DataFrame({'topics': topic_range, 'umass': umass_values})
best_k = df_umass.loc[df_umass['umass'].idxmax(), 'topics']
best_k = 5  # Feste Anzahl laut Aufgabenstellung

# LSA-Modell erstellen
lsi = models.LsiModel(corpus_tfidf, id2word=dictionary, num_topics=best_k)
print(color.CYAN + f"\nTop {best_k} Themen:\n" + color.END)
for topic in lsi.print_topics(num_topics=best_k, num_words=5):
    print(topic)

# UMass-Graph anzeigen
print(color.CYAN + "\nUMass-Kurve:" + color.END)
plt.plot(df_umass['topics'], df_umass['umass'], marker='o')
plt.xlabel("Anzahl Themen")
plt.ylabel("uMass-Wert")
plt.title("Topic Coherence")
plt.grid(True)
plt.show()

# Word2Vec-Modell trainieren
print(color.CYAN + "\nWord Embedding (Word2Vec):\n" + color.END)

# Erstelle und trainiere das Modell (nur Wörter mit min. 5 Vorkommen werden berücksichtigt)
w2v_model = gensim.models.Word2Vec(data, min_count=5, vector_size=200)
w2v_model.build_vocab(data)
w2v_model.train(data, total_examples=w2v_model.corpus_count, epochs=30, report_delay=1)

# Vokabular extrahieren und in 2D mit PCA transformieren
words = list(w2v_model.wv.key_to_index)
X = [w2v_model.wv[word] for i, word in enumerate(words)]
pca = PCA(n_components=2)
result = pca.fit_transform(X)

# Daten in ein DataFrame packen
pca_df = pd.DataFrame(result, columns=['x', 'y'])
pca_df['word'] = words
pca_df.head()

# Interaktive Visualisierung mit Plotly erstellen
fig = go.Figure(data=go.Scattergl(
    x=pca_df['x'],
    y=pca_df['y'],
    mode='markers',
    marker=dict(
        color=np.random.randn(len(words)),  # zufällige Farben
        colorscale='Viridis',
        line_width=1
    ),
    text=pca_df['word'],
    textposition="bottom center"
))

fig.update_layout(
    autosize=False,
    width=800,
    height=800,
    title="Word2Vec Wort-Embedding Visualisierung"
)

# Optional: als HTML speichern
fig.write_html("word2vec.html")

fig.show()
