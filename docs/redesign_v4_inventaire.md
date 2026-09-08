# Redesign v4 — inventaire et avancement

Référence : `design/BRVM Analyzer - Redesign v4.dc.html` (canevas Claude Design).
Ce fichier liste, page par page et onglet par onglet, **ce que le canevas
contient**, et l'état de la reprise. Il se met à jour à chaque page traitée.

Le canevas compte **95 cartes de KPI**, 10 pages, 33 onglets. Les blocs qu'il
emploie sont d'un vocabulaire fermé : tableau, listes, cartes KPI, courbe,
nuage de points, carte de chaleur, plan en étapes, cartes, note, anneau,
barres groupées, bloc dépliable.

## Composants partagés

| Composant | Où il vit | État |
|---|---|---|
| `status_strip` — bandeau de séance | `utils/ui_helpers.py` | fait, dans la coquille |
| `kpi_v4` — carte à filet coloré | `utils/ui_helpers.py` | fait |
| `breadth_bar` — barre de largeur | `utils/ui_helpers.py` | fait |
| `heatmap` — carte de chaleur | `utils/ui_helpers.py` | fait |
| `note` — encart à filet latéral | `utils/ui_helpers.py` | fait |
| `donut` — anneau à figure centrale | `utils/ui_helpers.py` | fait |
| `plan_etapes` — plan d'action numéroté | `utils/ui_helpers.py` | fait |
| Châssis de tableau (rayon, filets, en-têtes) | `style.css` + vues | fait |
| Boutons, segments, champs | `style.css` | fait |

## Pages

| # | Page | Onglets | Blocs du canevas | État |
|---|---|---|---|---|
| 1 | Dashboard | Jour · Semaine · Mois | barre de largeur, listes, cartes KPI, carte de chaleur, tableau | **fait** |
| 2 | Infos Marché | Revue de presse · Fil d'actualités | cartes×3, tableau | **fait** |
| 3 | Analyse d'un Titre | Cours · Fondamentale · Technique · Risque · Recommandation · Profil | 32 KPI, 12 tableaux, 3 courbes, nuage, 4 notes, 3 dépliables | partiel |
| 4 | Screening | Filtres fondamentaux · Risque et liquidité · Résultats | 4 KPI, 3 tableaux, note, nuage | **fait** |
| 5 | Comparateur | Tableau · Profil · Performance | 1 tableau, 2 courbes, carte de chaleur | **fait** |
| 6 | Performance des Titres | Classement · Par secteur · Graphique · Multi-périodes | 4 KPI, listes, 2 tableaux, carte de chaleur, courbe | **fait** |
| 7 | Portefeuille | Performance · Recommandations · Risque et optimisation | 16 KPI, 9 tableaux, carte de chaleur, anneau, barres, plan en étapes, nuage, 5 notes | **fait** (vérifié isolément) |
| 8 | Signaux | Synthèse · Contradictions | 4 KPI, tableau, cartes | **fait** |
| 9 | Trajectoires Recommandations | Cohorte · Trajectoires · Backtest | 12 KPI, 2 tableaux, note | **fait** (non vérifiable ici) |
| 10 | Historique Signaux | Vue d'ensemble · Signaux · Recommandations · Calibration · Données brutes | 23 KPI, 8 tableaux, carte de chaleur, note | **fait** (vérifié sur les données) |

## Méthode

Une page est « faite » quand ses onglets, sa barre d'outils, sa rangée de KPI
et chacun de ses blocs correspondent au canevas, **et** que le rendu réel a
été vérifié dans le navigateur. Pas quand le code compile.

Les écarts assumés au canevas sont notés dans la PR qui les introduit, jamais
laissés implicites.

## Ce qui n'est pas vérifiable ici

Trois pages ne s'ouvrent pas dans le navigateur de développement :

| Page | Pourquoi |
|---|---|
| Portefeuille | exige une connexion Google ; `Authlib` n'est pas installé en local |
| Trajectoires Recommandations | réservée à l'administrateur, donc invisible sans connexion |
| Historique Signaux | idem |

Pour ces trois pages, la vérification a pris deux formes, toutes deux réelles
mais partielles :

1. **La forme** — les composants sont rendus isolément dans un navigateur,
   avec la feuille de style réelle et des valeurs plausibles. On voit le bloc,
   pas son branchement.
2. **La donnée** — les calculs qui les alimentent sont exécutés contre la base
   de production et leurs résultats lus. On voit les chiffres, pas leur mise
   en page.

Ce qui n'est PAS vérifié : que le bloc s'affiche au bon endroit de la page,
avec les bonnes données, dans le vrai flux Streamlit. Seul un utilisateur
connecté peut le confirmer.
