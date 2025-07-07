# Portfolio_Data_Analysis_DLBDSEDE02_D

## Top 10 Hashtags

```
fifacwc: 174
gobetaverse: 88
gamblingx: 88
prizepicks: 88
gamblingtwitter: 88
fanduel: 85
mufc: 69
dfs: 69
fifaclubworldcup: 66
bayernmunich: 62
```

## Top 10 Nutzer

```
grok: 295
goBetaverse: 62
xavier_aleix: 33
Mayor_ttt: 28
BavarianFBWorks: 12
elpatrue1: 11
joelharkolawole: 11
CR_74_: 11
BayernNewsw: 11
thesoftfootball: 9
```

## Themenkohärenz (uMass)

```
1: -2.587921601969864
2: -9.985504487945546
3: -7.3791742911616325
4: -2.7003934027451963
5: -7.959821372499104
6: -7.818719608932278
7: -9.200915814637264
8: -9.816468161561843
9: -7.852534465554874
10: -6.453484727144284
11: -7.199990016298815
12: -5.80030710788613
13: -5.195923488287938
14: -7.635402898381784
15: -5.581500156411219
16: -6.864123508884084
17: -7.952249827366039
18: -7.356826556994355
19: -6.651436051278336
20: -7.927977257861014
21: -6.945699886425563
22: -8.24085744400635
23: -9.548706140145072
24: -7.138720926911038
25: -7.016261341481757
26: -8.361165940327616
27: -7.984493676036034
28: -9.055192249862422
29: -9.603534875598772
30: -8.825403727915386
31: -7.7676846726361575
32: -8.451227969231116
33: -9.037739334140399
34: -7.963754829821594
35: -8.516030211204429
36: -9.600423819570594
37: -8.038078256559317
38: -9.262851995140293
39: -8.914622101832961
40: -8.756898780601224
41: -8.017192314319262
42: -8.887729489914307
43: -8.998976833503336
44: -9.050411512201656
45: -9.142093074209138
46: -9.485871237056964
47: -9.50299777001587
48: -9.416725524195316
49: -9.096218369405083
50: -8.810896052400448
51: -8.906302415290435
52: -8.740972130200639
53: -10.061589713754083
54: -9.056151015704225
55: -9.687167007134105
56: -9.450379274836378
57: -9.216858631658068
58: -9.528461899657257
59: -9.081694673161499
60: -8.707769070758514
61: -9.063139525177396
62: -9.09749050162505
63: -9.52921123507141
64: -9.323262188046227
65: -9.256088575295072
66: -9.111471545263727
67: -9.34401273140072
68: -9.179709590061293
69: -9.704734825116534
70: -9.754089578158865
71: -9.778149019337702
72: -9.707308283746544
73: -9.583324771846309
74: -9.924015675505796
75: -9.547713020232555
76: -9.431913344813333
77: -9.697228395670084
78: -9.420212073787724
79: -9.803893472736213
80: -9.768437166655925
81: -9.929799558406046
82: -9.74002590382475
83: -9.731353454191552
84: -9.656725309017254
85: -9.877994930952823
86: -10.15413925787444
87: -10.424602505366186
88: -10.263566639150566
89: -10.17253053917431
90: -9.896460641306266
91: -10.713149016079605
92: -10.226947697649887
93: -9.820621620911327
94: -10.14158747655926
95: -10.437733263089875
96: -10.096926628768115
97: -10.035486744137112
98: -10.427301357598637
99: -10.132672644553322
100: -10.674680458248252
```

## Top 5 Themen

```
(0) 0.295*"cup" + 0.290*"world" + 0.270*"club" + 0.240*"fifa" + 0.231*"flamengo"
(1) 0.319*"free" + 0.252*"pick" + 0.236*"bet" + -0.188*"liverpool" + -0.188*"diaz"
(2) -0.271*"diaz" + -0.244*"luis" + -0.240*"liverpool" + -0.223*"approach" + -0.218*"free"
(3) 0.439*"rashford" + 0.394*"marcus" + 0.300*"move" + 0.280*"considering" + 0.254*"mufc"
(4) -0.330*"kane" + 0.307*"chelsea" + -0.305*"harry" + 0.272*"madrid" + -0.234*"flamengo"
```

## Visualisierungen

- **uMass-Kurve**: [Topic_Coherence.png](Topic_Coherence.png)
- **Word2Vec-Modell**: [`word2vec.html`](word2vec.html)


---

## Anleitung zur Verwendung der Skripte

### Voraussetzungen

- **Python-Version 3.10 oder höher**
- Ein gültiger Twitter API-Zugang (Basic Tier)
- Die in der `requirements.txt` aufgelisteten Bibliotheken:
  - `tweepy`, `pandas`, `nltk`, `gensim`, `matplotlib`, `numpy`, `scikit-learn`, `plotly`, `termcolor`
- Skripte und JSON-Datei müssen im selben Ordner liegen

### Installation der Bibliotheken

Installiere die notwendigen Pakete mit:

```
pip install -r requirements.txt
```

Optional kann dies auch manuell durchgeführt werden:

```
pip install tweepy pandas nltk gensim matplotlib numpy scikit-learn plotly termcolor
```

### Schritt 1: Tweets abrufen oder vorhandene Daten nutzen

**Möglichkeit 1: Eigene Tweets abrufen**

1. Öffne die Datei `fetch_tweets.py`.
2. Ersetze den Platzhalter `<<BEARER TOKEN>>` durch deinen Twitter API Token.
3. Du kannst im Skript außerdem die gewünschte Region, die Sprache sowie die minimale und maximale Anzahl der Tweets anpassen.
4. Führe das Skript aus, um die Tweets abzurufen:

```
python fetch_tweets.py
```

Dadurch wird eine Datei namens `fetched_tweets.json` erstellt.

**Möglichkeit 2: Vorhandene Daten nutzen**

Falls du bereits eine `fetched_tweets.json`-Datei hast oder die im Repository liegende verwenden willst, kannst du direkt mit der Analyse weitermachen.

### Schritt 2: Tweets analysieren

1. Führe das Analyse-Skript aus:

```
python tweets_analyze.py
```

2. Beim Starten wird eine interaktive Abfrage gestartet, bei der du mit `j` (Ja) oder `n` (Nein) antworten kannst, um den Download zu steuern, anhand dessen werden die NLTK-Bibliotheken heruntergeladen. (Das Herunterladen ist nur beim ersten Start notwendig)

### Ergebnisse

Beim Ausführen des Skripts werden folgende konkrete Inhalte erzeugt oder angezeigt:

- **Top 10 Hashtags**: Liste der am häufigsten verwendeten Hashtags im Datensatz.
- **Top 10 Nutzer**: Liste der aktivsten Twitter-Accounts, sortiert nach Tweet-Anzahl.
- **uMass-Topic-Coherence-Werte**: Bewertung der Kohärenz über verschiedene Themenanzahlen (1–100).
- **Top 5 Themen**: Die fünf inhaltlich dominanten Themen im Datensatz, als gewichtete Schlüsselwörter dargestellt.
- **Word2Vec-Modell**: Darstellung von Wortähnlichkeiten in einem 2D-Raum.

  
- Die Topic-Coherence-Grafik ([Topic_Coherence.png](Topic_Coherence.png)) wird während der Ausführung angezeigt, aber nicht automatisch gespeichert. Du kannst sie jedoch manuell sichern.
  
- Alternativ kannst du auch das Jupyter Notebook verwenden, um die Analysen durchzuführen. In diesem Fall ist kein separater Download der Skripte nötig.
