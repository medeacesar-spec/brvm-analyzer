# Redesign v4 — écarts au canevas, onglet par onglet

Registre de travail. Référence : `design/BRVM Analyzer - Redesign v4.dc.html`.
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
| D1 | Le bulletin BOC et le bouton « Revue de presse » s'intercalent entre les KPI et les onglets ; le canevas enchaîne KPI → onglets | à arbitrer (ajouts de l'app) |
| D2 | Les lignes des Top 5 portent le ticker seul ; le canevas y met **ticker · secteur** | à faire |
| D3 | La bande « Ouvrir l'analyse d'un titre » est sous la carte ; le canevas la met **en pied de carte** | à faire |

### Infos Marché — Revue de presse · Fil d'actualités

| # | Écart | État |
|---|---|---|
| I1 | Le sélecteur de période est sous les onglets ; le canevas le place **au-dessus**, en barre d'outils de page | à faire |
| I2 | Dates au format long (`2026-09-07`) ; le canevas les met en `02/09` | à faire |

### Analyse d'un Titre — 6 onglets

| # | Onglet | Écart | État |
|---|---|---|---|
| A1 | Cours | Pas de sélecteur **Exercice** dans la barre d'outils | à faire |
| A2 | Cours | Période affichée en menu déroulant ; le canevas met des **boutons segmentés** | à faire |
| A3 | Fondamentale | Les 4 sous-scores n'ont pas leur **sous-ligne explicative** (« ROE 28,4 % ») | à faire |
| A4 | Fondamentale | Libellés coupés (« ENDETTEMEN/T ») : 5 colonnes fixes au lieu d'une grille repliable | à faire |
| A5 | Technique | Période et surcouches en menu et cases à cocher ; le canevas met **segmentés et pastilles** | à faire |
| A6 | Technique | Pas de sélecteur **Périodicité** (Journalière / Mensuelle) | à faire |
| A7 | Recommandation | Manque les cartes **Prix actuel** et **Prix cible (modèle)** | à faire |
| A8 | Recommandation | Composition du score en barre empilée ; le canevas montre des **barres pondérées 60/40** | à faire |
| A9 | Profil | Actionnariat en carte latérale ; le canevas en fait **3 cartes de KPI** | à faire |
| A10 | Profil | Le nom de la société est répété en titre de section | à faire |
| A11 | Risque | Un titre de section « Risque » en trop au-dessus de la note | à faire |
| A12 | Technique | Emoji résiduel « 📖 Comprendre les indicateurs techniques » | à faire |
| A13 | Technique | Tendance affichée « Haussiere » sans accent (donnée) | à faire |

### Screening — Filtres fondamentaux · Risque et liquidité · Résultats

| # | Écart | État |
|---|---|---|
| S1 | Onglet 1 : champs nus ; le canevas montre un **tableau des seuils** avec la colonne « effet sur l'univers » (« retire 22 titres ») | à faire |
| S2 | Onglet 2 : idem, avec la colonne « pourquoi » qui justifie chaque critère de marché | à faire |
| S3 | Un intitulé « Univers d'analyse » que le canevas n'a pas | à arbitrer |

### Comparateur — Tableau · Profil · Performance

| # | Écart | État |
|---|---|---|
| C1 | Profil comparatif en barres groupées ; le canevas emploie des **axes parallèles**, où le croisement des lignes montre où le choix se joue | à faire |

### Performance des Titres — 4 onglets

Conforme sur les quatre onglets.

### Portefeuille — Performance · Recommandations · Risque et optimisation

| # | Onglet | Écart | État |
|---|---|---|---|
| P1 | Performance | Le tableau des positions n'a pas la **barre signée** de variation | à faire |
| P2 | Performance | L'intro omet « frais d'achat inclus dans le coût de revient » | à faire |
| P3 | Recommandations | Manque la carte **Concentration 3 lignes** (seuil de vigilance 50 %) | à faire |
| P4 | Recommandations | Diagnostic en tableau ; le canevas en fait des **cartes** à filet et pastille | à faire |
| P5 | Risque | Manque les **4 cartes de risque** : volatilité du portefeuille, **bêta agrégé**, perte maximale simulée, rendement par unité de risque | à faire |

### Signaux — Synthèse · Contradictions

Conforme. Les cartes de contradiction sont plus riches que le canevas
(ticker, verdict retenu, signaux de chaque côté) — écart assumé.

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

## Causes récurrentes

Les vingt-cinq écarts se ramènent à cinq causes. Corriger la cause vaut mieux
que corriger les symptômes un à un.

1. **Rangées de KPI manquantes** — 7 cartes (P5 ×4, P3, A7 ×2). Le canevas
   ouvre chaque onglet par une rangée ; l'application en saute trois.
2. **Sous-lignes explicatives omises** — un score sans son « pourquoi » (A3).
3. **Contrôles non segmentés** — menus déroulants et cases à cocher là où le
   canevas met des boutons segmentés et des pastilles (A2, A5, A6, I1).
4. **Colonnes fixes au lieu de grilles repliables** — `st.columns` impose N
   colonnes quelle que soit la largeur, d'où les libellés coupés (A4). Le
   canevas emploie `repeat(auto-fit, minmax(...))` ; `kpi_grille` le fait déjà.
5. **Blocs rendus autrement** — tableau au lieu de cartes (P4), barre empilée
   au lieu de barres pondérées (A8), barres au lieu d'axes parallèles (C1),
   champs nus au lieu de tableaux pédagogiques (S1, S2).

## Ordre de correction proposé

1. Les 7 cartes de KPI manquantes (P5, P3, A7) — le plus visible.
2. Les sous-lignes des scores (A3) et la grille (A4).
3. Les contrôles segmentés (A2, A5, A6, I1).
4. Les tableaux pédagogiques de Screening (S1, S2).
5. Les trois blocs à re-rendre (P4, A8, C1).
6. La barre signée des positions (P1).
7. Les détails : T1, T2, H1, H2, D2, D3, I2, A9-A13, P2.
