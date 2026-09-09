# Redesign v4 — écarts au canevas, onglet par onglet

Registre de travail. Référence : `design/BRVM Analyzer - Redesign v4.dc.html`.

**Quarante écarts.** Les deux qui étaient à arbitrer — D1 et S3 — l'ont
été le 08/09, et un troisième a été refusé sur le fond (A8). Le document a longtemps annoncé
« 25 » : le chiffre était faux dès la première rédaction — le décompte onglet
par onglet, lui, a toujours été juste. Recompté le 08/09.
Diagnostic établi le 8 septembre 2026 en ouvrant le canevas et l'application
**côte à côte**, page par page et onglet par onglet — 10 pages, 33 onglets.

Une ligne se coche quand la correction est **vue au rendu**, pas quand le code
compile. Voir « Comment refaire la comparaison » en fin de document.

---

## Comment refaire la comparaison

Sans cette procédure, on retombe dans le travers qui a coûté cher : juger le
design en lisant le code, et croire une page conforme parce que ses blocs
existent.

### 1. Remonter le canevas en page exécutable

Le canevas publié est un *bundle* : gabarit, runtime, React et polices
compressés en base64 dans la page. Il ne s'exécute pas tel quel.

```bash
python3 scripts/reconstruire_canevas.py <artefact.html> /tmp/canevas
python3 -m http.server 8777 --directory /tmp/canevas
```

L'artefact source s'obtient en lisant l'artefact publié depuis une session
Claude Code : l'outil enregistre le HTML complet dans un fichier local.

### 2. Ouvrir l'application avec une session administrateur

Trois pages — Portefeuille, Trajectoires, Historique Signaux — exigent une
session admin, et l'OAuth n'est pas installé en local :

```bash
BRVM_DEV_LOGIN=1 streamlit run app.py --server.port 8502
```

Le formulaire « Accès développeur » apparaît alors dans la barre latérale.
Sans la variable, il n'existe pas — Streamlit Cloud ne la définit pas.

La session de test s'ouvre par défaut sur **`dev@local`**, dont le portefeuille
a été rempli le 08/09 d'une copie des positions réelles : sans lui, les pages
sous connexion s'affichaient vides et ne montraient aucun des blocs à comparer.
Pour ouvrir un autre compte, `BRVM_DEV_EMAIL=...` à côté de `BRVM_DEV_LOGIN=1`
— le champ du formulaire ne se remplit pas depuis l'extérieur de Streamlit.

### 3. Comparer onglet par onglet, pas page par page

**C'est ici que l'erreur se commet.** Portefeuille et Analyse d'un Titre ont
des onglets aux designs entièrement différents : comparer la première vue
d'une page ne dit rien des cinq autres. Il faut ouvrir **chaque onglet des
deux côtés**.

Les onglets Streamlit sont rendus d'un bloc côté client : une fois la page
chargée, passer d'un onglet à l'autre est instantané. Le canevas aussi. Le
coût est le chargement de la page, pas les onglets — il n'y a donc aucune
économie à en sauter.

### 4. Deux pièges de mesure

- **Sous 800 px de large**, Streamlit passe la barre latérale en superposition
  et le canevas replie ses grilles : les deux se déforment différemment et la
  comparaison de mise en page devient trompeuse. Replier la barre latérale
  pour retrouver une largeur utile honnête.
- **Après un changement de taille de fenêtre, recharger** : Streamlit calcule
  la largeur de ses colonnes au chargement et ne la recalcule pas.

---

## Écarts par onglet

Légende : **à faire** · *en cours* · fait (vu au rendu)

### Dashboard — Jour · Semaine · Mois

| # | Écart | État |
|---|---|---|
| D1 | Le bulletin BOC et le bouton « Revue de presse » s'intercalent entre les KPI et les onglets ; le canevas enchaîne KPI → onglets | **arbitré le 08/09 : les deux vont dans Infos Marché.** Fait — le tableau de bord enchaîne KPI → onglets. Le BOC ouvre désormais la Revue de presse ; le bouton, qui ne faisait que renvoyer à cette page, est simplement retiré |
| D2 | Les lignes des Top 5 portent le ticker seul ; le canevas y met **ticker · secteur** | à faire |
| D3 | La bande « Ouvrir l'analyse d'un titre » est sous la carte ; le canevas la met **en pied de carte** | à faire |

### Infos Marché — Revue de presse · Fil d'actualités

| # | Écart | État |
|---|---|---|
| I1 | Le sélecteur de période est sous les onglets ; le canevas le place **au-dessus**, en barre d'outils de page | **fait** — vu au rendu le 08/09. Il gouverne désormais les deux onglets, comme dans le canevas ; ce que la fenêtre écarte du fil brut est compté, avec ce qui y reste à intégrer. Fenêtres ramenées à **1 j / 3 j / 7 j** (défaut 7) le 08/09 : 7, 15 et 30 jours ramenaient trop de lignes |
| I2 | Dates au format long (`2026-09-07`) ; le canevas les met en `02/09` | **fait** — vu au rendu le 08/09. Le fil brut les écrivait déjà ainsi ; la revue s'aligne |

### Analyse d'un Titre — 6 onglets

| # | Onglet | Écart | État |
|---|---|---|---|
| A1 | Cours | Pas de sélecteur **Exercice** dans la barre d'outils | **à trancher** — ce n'est pas un habillage : l'application choisit aujourd'hui *l'exercice le plus complet* toute seule (`_exercice_le_plus_complet`). Un sélecteur signifie rendre ce choix manuel, et il gouverne les ratios, les scores et le verdict de toute la page — pas seulement l'onglet Cours, où il n'a aucun effet sur les cours |
| A2 | Cours | Période affichée en menu déroulant ; le canevas met des **boutons segmentés** | **fait** — vu au rendu le 08/09, les huit fenêtres visibles d'un coup |
| A3 | Fondamentale | Les 4 sous-scores n'ont pas leur **sous-ligne explicative** (« ROE 28,4 % ») | **fait** — vu au rendu le 08/09. Chaque sous-ligne nomme les ratios qui alimentent réellement le barème, et dit « non applicable » pour l'endettement d'une banque |
| A4 | Fondamentale | Libellés coupés (« ENDETTEMEN/T ») : 5 colonnes fixes au lieu d'une grille repliable | **fait** — vu au rendu le 08/09, à 1 440 px (5 colonnes) et à 1 000 px (2 colonnes), sans libellé coupé |
| A5 | Technique | Période et surcouches en menu et cases à cocher ; le canevas met **segmentés et pastilles** | **fait** — vu au rendu le 08/09 (`st.segmented_control` et `st.pills`) |
| A6 | Technique | Pas de sélecteur **Périodicité** (Journalière / Mensuelle) | **à trancher** — l'application *détecte* la périodicité de la série (`_detect_frequency`) au lieu de la laisser choisir. Offrir le choix veut dire lire `price_monthly` au lieu de `price_cache`, et cette table n'a **ni ouverture, ni plus haut, ni plus bas** : les chandeliers de l'onglet devraient devenir une courbe en mensuel. Décision de fond, pas de forme |
| A7 | Recommandation | Manque les cartes **Prix actuel** et **Prix cible (modèle)** | **fait** — vu au rendu le 08/09, dans les deux cas : cible unique (BOAS.sn, « +9.5 % vs cours ») et méthodes divergentes (SNTS.sn, fourchette). Les deux tuiles qui répétaient ces chiffres plus bas ont été retirées |
| A8 | Recommandation | Composition du score en barre empilée ; le canevas montre des **barres pondérées 60/40** | **refusé le 08/09** — voir ci-dessous |
| A9 | Profil | Actionnariat en carte latérale ; le canevas en fait **3 cartes de KPI** | à faire |
| A10 | Profil | Le nom de la société est répété en titre de section | à faire |
| A11 | Risque | Un titre de section « Risque » en trop au-dessus de la note | à faire |
| A12 | Technique | Emoji résiduel « 📖 Comprendre les indicateurs techniques » | **fait** — vu au rendu le 08/09, avec A16 : l'emoji partait avec l'expander |
| A13 | Technique | Tendance affichée « Haussiere » sans accent (donnée) | **fait** — vu au rendu le 08/09. Le code interne reste sans accent : il sert de clé de comparaison dans `analysis/technical.py`. C'est le **libellé** qui s'accentue, la force aussi (« Modérée ») |
| A14 | Technique | **Niveaux clés incomplet** : le canevas donne 7 lignes — Résistance 2, Résistance 1, Cours, **SMA 50**, **SMA 200**, Support 1, Support 2 — chacune avec son écart au cours. L'application ne construit jamais les lignes de moyenne mobile, alors qu'elles sont tracées sur le graphique juste au-dessus. Sur ABJC.ci : 3 lignes au lieu de 7 | **fait** — vu au rendu le 08/09. MM50 et MM200 entrent dans le tableau, et l'ensemble est rangé par prix décroissant : c'est une **échelle**, pas trois listes. Sur ABJC.ci, cinq lignes au lieu de trois |
| A15 | Technique | **Signaux techniques en liste, pas en cartes.** Le canevas en fait des cartes à ton et étiquette (Info · Achat · Vigilance) avec une phrase qui explique le signal. L'application affiche « RSI en surachat — RSI = 77.5 (> 70) » : le constat sans la lecture. Même nature que P4, déjà corrigé côté portefeuille — `cartes_constats` existe | **fait** — vu au rendu le 08/09, par `cartes_constats`, le composant du diagnostic du portefeuille |
| A16 | Technique | **Une explication repliée dans une explication.** L'expander « 📖 Comprendre les indicateurs techniques » est ouvert **à l'intérieur** de « En savoir plus · RSI, MACD, Moyennes mobiles » : deux niveaux de repli pour le même sujet, là où le canevas n'en a qu'un. C'est la vraie forme de l'écart A12, dont l'emoji n'était que le symptôme visible | **fait** — vu au rendu le 08/09. Un seul niveau de repli : l'explication s'ouvre d'un geste |

### Screening — Filtres fondamentaux · Risque et liquidité · Résultats

| # | Écart | État |
|---|---|---|
| S1 | Onglet 1 : champs nus ; le canevas montre un **tableau des seuils** avec la colonne « effet sur l'univers » (« retire 22 titres ») | **fait** — vu au rendu le 08/09. La colonne distingue en plus ce qu'un seuil écarte de ce qu'une **donnée manquante** écarte |
| S2 | Onglet 2 : idem, avec la colonne « pourquoi » qui justifie chaque critère de marché | **fait** — vu au rendu le 08/09 |
| S3 | Un intitulé « Univers d'analyse » que le canevas n'a pas | **arbitré le 08/09 : le retirer.** Fait |

### Comparateur — Tableau · Profil · Performance

| # | Écart | État |
|---|---|---|
| C1 | Profil comparatif en barres groupées ; le canevas emploie des **axes parallèles**, où le croisement des lignes montre où le choix se joue | **fait** — vu au rendu le 08/09. Les croisements sont **calculés** et nommés, avec une lecture qui change selon qu'ils sont rares, majoritaires ou absents |

### Performance des Titres — Classement · Par secteur · Graphique · Tableau multi-périodes

Déclarée conforme le 08/09 sur une lecture d'ensemble. Relue bloc par bloc le
même soir, elle porte trois écarts.

| # | Onglet | Écart | État |
|---|---|---|---|
| F1 | Classement | **Les barres ignorent le signe.** La largeur vaut `abs(valeur) / max`, et la couleur est fixée par la liste, pas par la valeur. Sur le classement 1 an, **cinq des huit « pires performers » sont positifs** : SITAB (+13,5 %) reçoit une barre rouge de 24 % de largeur, plus longue que celle de la Loterie du Bénin (−5,1 %, 9 %). La barre lue comme une baisse n'en est pas une. La **barre signée** existe déjà dans l'application, sous « Toutes les cotations » | à faire |
| F2 | Par secteur | « Performance sectorielle » rendue en **graphique de la moyenne** ; le canevas met un **tableau à six colonnes** : Titres, Moyenne, **Médiane**, Position (barre signée), **Étendue**. La médiane et l'étendue disent si la moyenne est représentative ou tirée par un titre — l'application tient déjà ce raisonnement dans l'onglet Risque | à faire |
| F3 | — | Quatre blocs hors canevas : le **curseur** du nombre de titres, le **sélecteur de secteur** en descente, « Rendement rapporté au risque · 5 ans » et « Comparaison secteurs ». Le sélecteur de période offre par ailleurs 3M/6M/1A/2A/3A/Max là où le canevas propose 1 mois/3 mois/1 an/3 ans/5 ans | à arbitrer (ajouts de l'app) |

### Portefeuille — Performance · Recommandations · Risque et optimisation

| # | Onglet | Écart | État |
|---|---|---|---|
| P1 | Performance | Le tableau des positions n'a pas la **barre signée** de variation | à faire |
| P2 | Performance | L'intro omet « frais d'achat inclus dans le coût de revient » | à faire |
| P3 | Recommandations | Manque la carte **Concentration 3 lignes** (seuil de vigilance 50 %) | **fait** — vu au rendu le 08/09. La rangée passe de trois à quatre cartes et au gabarit v4 |
| P4 | Recommandations | Diagnostic en tableau ; le canevas en fait des **cartes** à filet et pastille | **fait** — vu au rendu le 08/09. Composant partagé `cartes_constats`, en grille repliable |
| P5 | Risque | Manque les **4 cartes de risque** : volatilité du portefeuille, **bêta agrégé**, perte maximale simulée, rendement par unité de risque | **fait** — vu au rendu le 08/09. Trois des quatre mesures n'existaient pas : elles sont calculées sur la série du portefeuille reconstituée aux poids d'aujourd'hui |

### Signaux — Synthèse par titre · Contradictions

Déclarée conforme le 08/09 sur une lecture d'ensemble. Les cartes de
contradiction sont bien plus riches que le canevas (ticker, verdict retenu,
signaux de chaque côté) — écart assumé. La relecture bloc par bloc en trouve
trois autres.

| # | Écart | État |
|---|---|---|
| G1 | **Le titre « Assistant Signaux » s'affiche deux fois** : `section_heading` puis `st.subheader`, l'un sous l'autre. Même défaut qu'A10 | **sans objet** — le bloc a été retiré le 08/09 |
| G2 | « Aucun désaccord : le bilan et le cours disent la même chose » passe par `st.info` — **la boîte bleue d'alerte que `note()` a précisément été écrit pour remplacer**. Un état normal se lit comme un avertissement | **fait** — vu au rendu le 08/09, note verte |
| G3 | Le bloc **Assistant Signaux** n'est pas au canevas. Le titre de page est « Signaux d'achat / vente » là où le canevas dit « Signaux » | **arbitré le 08/09 : retirer l'assistant.** Fait, sur les deux pages. Le titre « Signaux d'achat / vente » reste — non arbitré |

### Trajectoires Recommandations — 3 onglets

| # | Écart | État |
|---|---|---|
| T1 | Badge **ADMIN** absent à côté du titre | à faire |
| T2 | Backtest : note « Lecture du backtest » absente | à faire |

### Historique Signaux — 5 onglets

| # | Écart | État |
|---|---|---|
| H1 | Badge **ADMIN** absent à côté du titre | à faire |
| H2 | Bouton **Exporter CSV** en tête de page absent | à faire |

---

## Un écart refusé : A8

Le canevas montre la composition du score hybride en trois barres annotées
« pondéré 60 % » et « pondéré 40 % », le technique étant noté sur 25.

**Le modèle ne fait rien de tel.** `analysis/scoring.py` calcule
`hybrid_score = fund_score + tech_score`, deux barèmes sur cinquante : une
somme simple, 50/50, sans pondération d'aucune sorte. Le « 60/40 » du canevas
est une invention du design.

Adopter la forme du canevas reviendrait donc à AFFICHER une pondération que le
code n'applique pas — un chiffre faux, sur la carte même qui explique le
score. Arbitrage du donneur d'ordre, le 8 septembre : *« il n'y a aucune raison
de changer la pondération. Design ne peut pas prendre cette décision. »*

La barre empilée de l'application reste donc en place. Elle dit ce que le
modèle fait : deux composantes sur cinquante, et ce qui manque pour cent.

Si la forme en trois barres est un jour reprise, ce sera **sans** l'annotation
de pondération, et avec chaque score rapporté à son propre maximum — 38/50 et
20/50, pas 38/50 et 20/25.

## Causes récurrentes

Les quarante écarts se ramènent à cinq causes. Corriger la cause vaut mieux
que corriger les symptômes un à un.

1. **Rangées de KPI manquantes** — 7 cartes (P5 ×4, P3, A7 ×2). Le canevas
   ouvre chaque onglet par une rangée ; l'application en saute trois.
2. **Sous-lignes explicatives omises** — un score sans son « pourquoi » (A3).
3. **Contrôles non segmentés** — menus déroulants et cases à cocher là où le
   canevas met des boutons segmentés et des pastilles (A2, A5, A6, I1).
4. **Colonnes fixes au lieu de grilles repliables** — `st.columns` impose N
   colonnes quelle que soit la largeur, d'où les libellés coupés (A4). Le
   canevas emploie `repeat(auto-fit, minmax(...))`. `kpi_grille` le fait —
   le composant était annoncé par la passation mais n'avait jamais été
   écrit ; il l'est depuis le 08/09, et les trois rangées corrigées
   ci-dessus passent par lui.
5. **Blocs rendus autrement** — tableau au lieu de cartes (P4, A15), graphique au lieu de tableau (F2), barre empilée
   au lieu de barres pondérées (A8), barres au lieu d'axes parallèles (C1),
   champs nus au lieu de tableaux pédagogiques (S1, S2).

## Ce que le registre avait manqué

Le diagnostic du 8 septembre a parcouru les 33 onglets, mais il a lu l'onglet
**Technique** de trop loin. Trois écarts s'y sont ajoutés le 8 au soir, en le
comparant bloc par bloc au canevas plutôt qu'en survolant sa forme générale :

- **A14** — le tableau des niveaux clés perd les deux moyennes mobiles, qui
  sont pourtant l'essentiel de ce qu'un lecteur y cherche : le cours est-il
  au-dessus ou en dessous, et de combien.
- **A15** — les signaux sont une liste, pas des cartes ; le constat y est,
  la lecture non.
- **A16** — deux expanders emboîtés sur le même sujet. L'écart A12 ne notait
  que l'emoji, qui n'en était que la partie visible.

La leçon rejoint celle de la méthode : **un onglet ne se juge pas à sa
silhouette.** Il se compare bloc par bloc, comme les pages se comparent onglet
par onglet.

**La relecture a confirmé la leçon.** « Performance des Titres » et
« Signaux », déclarées conformes sur une lecture d'ensemble, portent six
écarts de plus (F1 à F3, G1 à G3), dont un défaut de lecture réel : une barre
rouge de vingt-quatre pour cent pour un titre qui a **gagné** treize pour
cent. Aucune page n'est donc conforme pour avoir été regardée : elle l'est
quand ses blocs ont été comparés un par un.

**Un piège de la relecture elle-même.** Le tableau multi-périodes a d'abord
paru vide, puis a paru porter des performances impossibles — « −118,8 % »,
« −922,0 % ». Les deux étaient faux : la première fois le tableau n'avait pas
fini de se rendre, la seconde le signe « + » se lisait comme un « − » à
l'échelle de la capture. Vérification faite en rejouant le formateur sur les
valeurs de la base, tout était juste. **Une capture d'écran ne tranche pas un
signe** : le calcul, lui, tranche.

## Ordre de correction proposé

1. ~~Les 7 cartes de KPI manquantes (P5, P3, A7)~~ — **fait le 08/09**.
2. ~~Les sous-lignes des scores (A3) et la grille (A4)~~ — **fait le 08/09**.
3. ~~Les contrôles segmentés (A2, A5)~~ — **fait le 08/09** ; ~~I1 fait~~. A1 et A6 renvoyés à un arbitrage : ce sont des fonctions nouvelles, pas des habillages.
4. ~~Les tableaux pédagogiques de Screening (S1, S2)~~ — **fait le 08/09**.
5. ~~Les blocs à re-rendre : P4 et C1~~ — **fait le 08/09** ; ~~A8 refusé~~.
6. La barre signée : les positions du portefeuille (P1) **et le classement de Performance des Titres (F1)** — le même composant répond aux deux.
7. Les détails : T1, T2, H1, H2, D2, D3, A9-A13, P2 — ~~I2 fait le 08/09~~.
