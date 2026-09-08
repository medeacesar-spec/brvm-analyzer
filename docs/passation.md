# BRVM Analyzer — passation

**Dépôt** `medeacesar-spec/brvm-analyzer` · `/Users/mdegbe/brvm-analyzer` · Streamlit + Supabase
**État au 9 septembre 2026, après la nuit du 8 au 9.**

---

## Où en est le chantier

Le **redesign v4** est posé sur les dix pages. Le canevas de référence est
dans le dépôt : `design/BRVM Analyzer - Redesign v4.dc.html`.

Trois documents tiennent le chantier, et ils se lisent dans cet ordre :

| Document | Ce qu'il contient |
|---|---|
| `docs/redesign_v4_ecarts.md` | **Le registre de travail** : 31 écarts au canevas, onglet par onglet, avec l'ordre de correction — et la **méthode pour refaire la comparaison** |
| `docs/redesign_v4_inventaire.md` | Ce que le canevas contient (10 pages, 33 onglets, 95 cartes de KPI) et les écarts assumés |
| `docs/cahier_des_requetes.md` | Les demandes de fond, hors design |

**Neuf PR fusionnées** (#118 à #126). Le déploiement Streamlit Cloud suit
`main` automatiquement et a été vérifié en séance réelle.

---

## Règles de travail (à respecter d'emblée)

- **Branche isolée depuis `origin/main`, PR par l'API GitHub.** Le jeton est
  dans l'URL du remote. **La fusion par l'API est refusée** par le garde-fou
  de session : c'est le donneur d'ordre qui fusionne.
- **Vérifier l'état de la PR avant chaque push.** Elle est souvent fusionnée
  entre-temps, et le push part alors sur une branche morte sans rien signaler.
  C'est arrivé cinq fois. `curl .../pulls/N` puis branche neuve si close.
- **Vérifier par le rendu réel, jamais par la lecture du code.** Tous les
  défauts marquants de la journée n'étaient visibles qu'à l'écran : une carte
  de KPI en double, un score à −154 sur une échelle 0-100, une valeur coupée
  en « +… », une échelle de couleur délavée par une seule case, une première
  étape étiquetée « ensuite ».
- **Comparer ONGLET par onglet, pas page par page.** Portefeuille et Analyse
  d'un Titre ont des onglets aux designs entièrement différents. La procédure
  est dans `docs/redesign_v4_ecarts.md`.
- **`docs/cahier_des_requetes.md` ne se modifie pas dans une PR de code.**
- **Ne rien inventer** : une case vide vaut mieux qu'un chiffre fabriqué. Le
  canevas demande parfois une donnée que l'application ne calcule pas
  (confiance du modèle, impact de la 3ᵉ étape du plan) — on ne l'affiche pas.

---

## Ce que la journée a ajouté

**Composants partagés** dans `utils/ui_helpers.py` : `status_strip`,
`kpi_v4`, `kpi_grille`, `breadth_bar`, `heatmap`, `donut`, `note`,
`plan_etapes`. Ils portent le gabarit du canevas ; toute nouvelle carte
passe par eux.

**Bêta et RSI calculés** — `scripts/calculer_beta_rsi.py`. Les colonnes
`market_data.beta` et `market_data.rsi` étaient vides depuis l'origine.
45 bêtas, 47 RSI. Le bêta se mesure au MOIS contre le BRVM Composite
(au jour, un titre qui ne cote pas fausse tout) ; le RSI au JOUR, formule de
Wilder. **La corrélation au marché est faible sur cette place** — 18 titres
sur 45 sous 0,30 — et un bêta dont la corrélation tombe sous ce seuil
s'affiche en gris : la valeur est juste, sa portée est nulle.

**Accès développeur** — `utils/auth.py`. Le mode dev était lu partout et
n'avait jamais eu d'interface : impossible de voir les trois pages sous
connexion. Il en a une, fermée par `BRVM_DEV_LOGIN=1`, qui ne crée aucun
compte et ne vérifie aucun mot de passe.

**Outil de comparaison** — `scripts/reconstruire_canevas.py` remonte le
canevas en site statique local.

---

## SEPT PULL REQUESTS ATTENDENT LA FUSION

Rien n'a été fusionné depuis la #138. **À fusionner d'abord, avant tout
nouveau travail** — plusieurs se touchent, et l'ordre compte :

| PR | Contenu | Note |
|---|---|---|
| #135 | Screening : sans information sur un seuil, un titre est écarté | décision de fond |
| #139 | Contrôles segmentés A2, A5 | `p2_stock_analysis.py` |
| #140 | Onglet Technique A12-A16 | `p2_stock_analysis.py` — après #139 |
| #141 | Barre signée P1, P2, F1 | débloque F2 |
| #142 | Pages sous connexion T1, T2, H1 | |
| #143 | Dashboard D2-D5 et finitions A9-A11 | |
| #144 | Cahier des requêtes · Total Return | doc seule |

Chaque PR décrit ce qu'elle corrige et ce qu'elle a trouvé en route. Toutes
ont été vérifiées au rendu.

## Ce qui attend

1. **F2 — le tableau sectoriel de Performance des Titres.** Seul écart du
   registre encore à faire. Il veut une colonne en barre signée : `barre_signee`
   arrive avec la #141, d'où l'attente.
2. **Cinq arbitrages**, tous écrits dans le registre avec leur raisonnement :
   **A1** (sélecteur d'exercice — il gouvernerait toute la page, pas le seul
   onglet Cours), **A6** (périodicité — `price_monthly` n'a pas d'OHLC, les
   chandeliers deviendraient une courbe), **F3** (quatre blocs hors canevas de
   Performance des Titres), **H3** (bouton « Colonnes » désactivé), et la
   question du **Composite Total Return** portée au cahier (#26).
3. **Introduction d'une banque le 14 septembre** — nom à confirmer. L'ajouter
   à `data/brvm_tickers.json` **et** à `TICKER_TO_BRVM_SLUG`, puis lancer
   `collecter_historique_cours.py` en M et T. Attention : depuis la #135, un
   titre sans historique est écarté du screening dès qu'un critère de marché
   est réglé — la banque le sera pendant deux ans, comme BICI Bénin et la
   Loterie du Bénin. La page le dit, ce n'est pas silencieux.
4. **Neuf dividendes en contradiction** avec l'avis BRVM — SOLIBRA 2020/2021
   au dixième, Filtisac 2024 à 145 contre 1 320.
5. **SODECI affiche une marge nette de 24,62**, soit 2 462 %. Le ratio est
   faux, ce n'est pas un défaut d'affichage. À porter au cahier.
6. **`market_data.dps` est vide** sur les 48 lignes, comme l'étaient beta et
   rsi. Même traitement possible.

---

## Ce que la nuit du 8 au 9 a appris

Trois leçons de méthode, chacune payée par une erreur.

**Un onglet ne se juge pas à sa silhouette.** Le diagnostic du 8 septembre
avait parcouru les 33 onglets et déclaré « Technique », « Performance des
Titres » et « Signaux » conformes parce que leurs blocs existaient tous. Relus
bloc par bloc, ils portaient **neuf écarts de plus** — dont un tableau de
niveaux clés amputé de ses moyennes mobiles, et une barre rouge de 24 % pour
un titre qui avait gagné 13,5 %.

**Une capture d'écran ne tranche pas un signe.** Le tableau multi-périodes a
paru vide, puis a paru porter des performances impossibles — « −118,8 % ». Les
deux fois j'avais tort : il n'avait pas fini de se rendre, puis le « + » se
lisait comme un « − » à l'échelle de l'image. **Ce qui se mesure se vérifie
par le calcul**, en rejouant le formateur sur les valeurs de la base ou en
lisant les styles calculés dans le DOM — pas à l'œil.

**Une rangée pleine peut cacher une carte tombée.** Le Composite Total Return
était collecté chaque jour, stocké, et affiché nulle part : la rangée des
indices tenait quatre colonnes pour cinq cartes. Rien ne pouvait le laisser
voir à l'écran. **Comparer ce que la base contient à ce que la page affiche**
est un contrôle que le rendu seul ne remplace pas.

---

## Composants partagés — l'état au 9/09

Dans `utils/ui_helpers.py`, tous nés d'un besoin répété : `status_strip`,
`kpi_v4`, **`kpi_grille`** (la grille repliable — elle a corrigé la cause n° 4
partout), `breadth_bar`, `heatmap`, `donut`, `note`, `plan_etapes`,
**`cartes_constats`** (les constats en cartes : diagnostic du portefeuille,
signaux techniques), **`barre_signee`** (positions, classement de performance,
toutes les cotations), **`titre_admin`** (les deux pages réservées).

La règle qui les a tous produits : **quand un motif apparaît une troisième
fois, il devient un composant** — trois copies d'une même échelle finissent
toujours par diverger.

---

## Environnement

- Python **3.13** en production (`runtime.txt`), mais le Streamlit local
  tourne sous 3.9 : `Authlib` y manque, d'où l'accès développeur.
- `numpy<2`, `pandas<2.3` — des SIGSEGV ont été rapportés au-delà.
- Une configuration de lancement `brvm-dev-admin` (port 8502, avec
  `BRVM_DEV_LOGIN=1`) est dans le `.claude/launch.json` local.
- La session de test s'ouvre sur **`dev@local`**, dont le portefeuille a été
  rempli le 08/09 d'une copie des 16 lots réels, des 5 dividendes et du cash :
  sans lui, les pages sous connexion s'affichaient vides. Pour ouvrir un autre
  compte, `BRVM_DEV_EMAIL=...` — le champ du formulaire ne se remplit pas
  depuis l'extérieur de Streamlit.
- La synchronisation quotidienne se déclenche au premier chargement du jour
  et prend plusieurs minutes : ne pas la confondre avec un blocage.
