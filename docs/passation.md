# BRVM Analyzer — passation

**Dépôt** `medeacesar-spec/brvm-analyzer` · `/Users/mdegbe/brvm-analyzer` · Streamlit + Supabase
**État au 8 septembre 2026, fin de journée.**

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

## Ce qui attend

1. **Les écarts au canevas** — 31 au total, dans l'ordre donné par
   `docs/redesign_v4_ecarts.md`. Commencer par les 7 cartes de KPI absentes.
2. **Introduction d'une banque le 14 septembre** — nom à confirmer. L'ajouter
   à `data/brvm_tickers.json` **et** à `TICKER_TO_BRVM_SLUG`, puis lancer
   `collecter_historique_cours.py` en M et T.
3. **Neuf dividendes en contradiction** avec l'avis BRVM — SOLIBRA 2020/2021
   au dixième, Filtisac 2024 à 145 contre 1 320. Les avis moissonnés et les
   états financiers des exercices contestés ont été mis en cache le 8/09.
4. **SODECI affiche une marge nette de 24,62**, soit 2 462 %. Le ratio est
   faux, ce n'est pas un défaut d'affichage. À porter au cahier.
5. **`market_data.dps` est vide** sur les 48 lignes, comme l'étaient beta et
   rsi. Même traitement possible.

---

## Environnement

- Python **3.13** en production (`runtime.txt`), mais le Streamlit local
  tourne sous 3.9 : `Authlib` y manque, d'où l'accès développeur.
- `numpy<2`, `pandas<2.3` — des SIGSEGV ont été rapportés au-delà.
- Une configuration de lancement `brvm-dev-admin` (port 8502, avec
  `BRVM_DEV_LOGIN=1`) est dans le `.claude/launch.json` local.
- La synchronisation quotidienne se déclenche au premier chargement du jour
  et prend plusieurs minutes : ne pas la confondre avec un blocage.
