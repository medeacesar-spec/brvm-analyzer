# Temps de réponse — revue et plan

Relevé le 9 septembre 2026, sur la base de production (Supabase,
`aws-1-eu-west-3`, Transaction Pooler port 6543).

---

## Le résultat en une ligne

**Trois quarts du temps d'attente après chaque clic sont passés à ouvrir des
connexions à la base de données — pas à interroger, pas à calculer, pas à
afficher. À ouvrir.**

| Mesure | Durée | Connexions | Dont poignées de main |
|---|---|---|---|
| Démarrage à froid | **72,1 s** | 16 | 30,3 s — **42 %** |
| Ré-exécution (un clic quelconque) | **19,2 s** | 8 | 14,6 s — **76 %** |
| Ré-exécution (mesure répétée) | 19,3 s | 8 | 14,9 s — 77 % |

Mesuré avec `streamlit.testing.v1.AppTest` sur `app.py`, en instrumentant
`psycopg.connect`. Chaque chiffre est une exécution réelle de l'application,
pas une extrapolation.

---

## Le coût unitaire

| Opération | Durée | En allers-retours réseau |
|---|---|---|
| Aller-retour TCP nu vers la base | 0,29 s | 1 |
| **Ouvrir une connexion** | **1,94 s** | **≈ 6 à 7** |
| Une requête sur une connexion **déjà ouverte** | 0,37 s | ≈ 1,3 |

Une connexion coûte donc **cinq fois** ce que coûte la requête qu'on va y
faire passer. L'application ouvre une connexion, pose une question, ferme :
elle paie le trajet complet pour chaque phrase.

`data/storage.py` compte **106 appels** à `read_sql_df` ou `get_connection`,
et **63 appels** à `.close()`. `get_connection()` fait un
`psycopg.connect()` neuf à chaque fois — il n'y a aucune réutilisation.

---

## Les quatre causes, chiffrées

### 1. Aucune connexion n'est réutilisée — 14,6 s par clic

C'est la cause dominante, et de loin. Huit connexions par ré-exécution, à
1,94 s pièce. Les huit requêtes qu'elles portent coûteraient 3 s sur une
connexion maintenue ouverte.

### 2. `init_db()` tourne au moins deux fois par démarrage — 26 s

`init_db()` coûte **12,7 à 13,9 s** par appel : il rejoue toute la DDL
(29 `CREATE TABLE` / `CREATE INDEX` / `ALTER TABLE`) à chaque fois, sans
mémoriser qu'il vient de le faire.

Il est appelé :
- au niveau module, à l'import de `data/storage.py` (`storage.py:3286`) ;
- puis à l'ouverture de session (`app.py:424`).

Soit ≈ 26 s des 72 s du démarrage à froid, à créer des tables qui existent
déjà.

### 3. Presque rien n'est mis en cache — le reste

L'application entière compte **10** décorateurs `@st.cache_data` ou
`@st.cache_resource`, répartis sur sept fichiers de vue. `data/storage.py`,
qui porte les 106 accès à la base, n'en a **aucun**. `app.py` non plus.

Chaque ré-exécution du script — et Streamlit ré-exécute tout à chaque
interaction — repose donc les mêmes questions à la même base.

### 4. La distance

0,29 s d'aller-retour depuis le poste de mesure vers `eu-west-3`. Ce n'est
pas un défaut du code, mais c'est le multiplicateur de tout le reste : chaque
aller-retour évité vaut 0,29 s.

---

## Le plan

Par rapport entre le gain et le risque. Les gains ne s'additionnent pas
exactement — le second lève une part du premier — mais l'ordre est le bon.

### 1. Réutiliser la connexion — gain ≈ 14 s par clic · **FAIT le 09/09**

Faire porter la connexion par `@st.cache_resource` au lieu de la rouvrir.

**Le risque est réel et doit être traité, pas ignoré.** Une connexion psycopg
n'est pas faite pour être utilisée par deux fils d'exécution à la fois, et
`@st.cache_resource` partage l'objet entre *toutes* les sessions. Avec
plusieurs utilisateurs simultanés, une connexion unique partagée produirait
des erreurs de transaction croisée.

Deux formes acceptables :
- **un pool** (`psycopg[binary,pool]`, à ajouter aux dépendances — il n'est
  pas installé aujourd'hui), qui prête une connexion par appel et la reprend.
  C'est la forme correcte ;
- **une connexion par session**, via `st.session_state` plutôt que
  `cache_resource`. Plus simple, suffisante pour un usage à un seul
  utilisateur, mais elle ne survit pas à la mise en veille de Streamlit
  Cloud — il faut alors détecter la connexion morte et la rouvrir.

Dans les deux cas, le Transaction Pooler de Supabase impose déjà
`prepare_threshold=None` : cette contrainte ne change pas.

### 2. N'appeler `init_db()` qu'une fois — gain ≈ 26 s au démarrage · **FAIT le 09/09**

Deux corrections, indépendantes :

- **retirer l'appel au niveau module** (`storage.py:3286`). Importer un
  module ne devrait pas écrire dans une base ; c'est aussi ce qui rend le
  moindre script de diagnostic lent à démarrer ;
- **mémoriser le passage** : un drapeau de processus, ou `@st.cache_resource`,
  pour que la DDL ne soit rejouée qu'une fois par processus.

Le fond de la question est ailleurs : la réconciliation de schéma est une
**migration**, pas une étape de démarrage. Sa place est dans un script qu'on
lance quand le schéma change, pas dans le chemin de chaque ouverture de page.
Le commentaire d'`app.py:420` explique pourquoi elle est là — éviter
`UndefinedColumn` sur une installation ancienne — et c'est une bonne raison
d'avoir une migration, pas de la rejouer à chaque fois.

**Forme retenue** : une connexion **par session Streamlit**, tenue dans
`st.session_state`, et non un pool. La revue hésitait entre les deux ; la
mesure a tranché. Les huit connexions se paient **dans une seule exécution de
script** : les réutiliser à l'intérieur d'une session capture presque tout le
gain, sans jamais partager un objet psycopg entre deux sessions. La dépendance
`psycopg[pool]` n'a donc pas été ajoutée.

`close()` change de sens et non de signature : les soixante appelants restent
inchangés, mais la connexion est rendue au lieu d'être fermée. Un compteur de
profondeur protège les appels imbriqués, et un `ROLLBACK` la rend propre —
sauté quand elle est déjà `IDLE`, ce qui rend le retour gratuit après un
`commit`.

`BRVM_DB_REUSE=0` rétablit l'ancien comportement.

**Mesuré** — même instrument, `AppTest` sur `app.py` :

| | Avant | Après |
|---|---|---|
| Connexions par ré-exécution | 8 | **0** |
| Connexions par démarrage à froid | 16 | **1** |
| Ré-exécution | 19,2 s | **5,5 – 7,3 s** |

Et dos à dos sur trois lectures applicatives, réseau identique :
33,6 s → 12,3 s, puis 17,7 s → 9,0 s.

**Ce qu'il reste dans cette ligne** : le `ROLLBACK` de nettoyage coûte encore
un aller-retour complet (327 ms mesurés) après chaque lecture, puisqu'un
`SELECT` ouvre une transaction. Le supprimer demanderait de passer la
connexion en `autocommit` et d'encadrer les écritures explicitement — un
changement de sémantique qui ne se fait pas en passant. Le point 3 le rend de
toute façon secondaire : une lecture mise en cache ne coûte rien du tout.

### 3. Mettre en cache les lectures — gain sur toutes les ré-exécutions

Poser `@st.cache_data(ttl=...)` sur les lectures de `data/storage.py`, avec
un TTL choisi selon la nature de la donnée :

| Donnée | TTL raisonnable | Pourquoi |
|---|---|---|
| Profils de sociétés, secteurs | plusieurs heures | ne change quasiment jamais |
| Fondamentaux, trimestriels | plusieurs heures | change à chaque publication |
| Cotations du jour | quelques minutes | change en séance |
| Portefeuille, positions | pas de cache | l'utilisateur vient de l'écrire |

**Le piège** : une donnée qu'on vient d'écrire doit sortir du cache
immédiatement, sinon l'application ment. Toute écriture doit appeler
`.clear()` sur les lectures correspondantes. C'est exactement le défaut que
le bouton d'intégration vient de corriger d'une autre manière — un compte
rendu qui disparaissait avant d'être lu.

### 4. Regrouper les requêtes — gain modeste, à faire en dernier

Huit connexions par ré-exécution veut dire huit fonctions d'accès appelées
l'une après l'autre. Une fois les trois premiers points faits, chaque requête
ne coûtera plus que 0,37 s et le regroupement n'aura plus grand-chose à
gagner. À ne faire que si le compte de requêtes reste élevé après mesure.

### Hors code : la distance

Si l'application déployée et la base ne sont pas dans la même région, chaque
aller-retour est payé au prix fort. Vérifier la région de l'instance
Streamlit Cloud et, si l'écart est grand, rapprocher l'une de l'autre. Ce
levier ne demande aucune ligne de code et multiplie l'effet de tous les
autres.

---

## Cible

| | Avant | Estimé | **Constaté le 09/09** |
|---|---|---|---|
| Démarrage à froid | 72 s | ≈ 20 s | 53 s (voir réserve) |
| Ré-exécution | 19 s | ≈ 5 s | **5,5 – 7,3 s** |

La ré-exécution tient l'estimation. Le démarrage à froid, non : les points 1
et 2 en retirent bien les connexions et la moitié de la DDL, mais il reste
une centaine de requêtes à 0,37 s. C'est le point 3 qui les vise.

**Réserve sur le démarrage à froid** : mesuré entre 53 s et 266 s pour un
code identique, selon l'état du réseau au moment de la mesure. Le chiffre de
53 s est le meilleur relevé, pas une valeur typique ; le nombre de connexions
(16 → 1), lui, ne dépend pas du réseau. Sur le même réseau dégradé,
l'ancien code dépassait 400 s là où le nouveau finissait.

---

## Ce que cette revue ne dit pas

- **Les mesures viennent d'un seul poste**, avec 0,29 s d'aller-retour vers
  `eu-west-3`. L'instance Streamlit Cloud a sa propre latence, qui peut être
  meilleure ou pire. Les **proportions** (76 % du temps en poignées de main)
  tiennent quelle que soit la latence ; les **secondes absolues**, non. La
  mesure est à refaire depuis l'application déployée avant d'annoncer un
  chiffre.
- **Le rendu côté navigateur n'est pas mesuré.** Tout ce qui précède est du
  temps serveur. Si une page reste lente après ces corrections, il faudra
  regarder le poids du HTML produit et le nombre de graphiques Plotly.
- **L'appel réseau à `brvm.org` au démarrage n'est pas dans le chemin
  courant** : il est conditionné à la fraîcheur des données
  (`_check_data_status`). Il n'entre en jeu qu'une fois par jour, et la page
  de garde le couvre déjà.
