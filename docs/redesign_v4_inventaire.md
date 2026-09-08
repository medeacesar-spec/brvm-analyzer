# Redesign v4 — inventaire et avancement

> Le registre des écarts onglet par onglet, et la **méthode de comparaison**,
> vivent dans `docs/redesign_v4_ecarts.md`. Ce fichier-ci ne tient que
> l'inventaire de ce que le canevas contient.

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

## Vérifier les pages sous connexion

Portefeuille, Trajectoires et Historique Signaux exigent une session
administrateur. En développement, il n'y avait aucun moyen d'en ouvrir une :
`Authlib` n'est pas installé en local, et le mode dev de `utils/auth.py`
était **lu partout mais n'avait jamais eu d'interface**.

Il en a une désormais, fermée par défaut :

```bash
BRVM_DEV_LOGIN=1 streamlit run app.py
```

Le formulaire « Accès développeur » apparaît alors dans la barre latérale et
pose une identité de test dans la session. Aucun compte n'est créé, aucun mot
de passe n'est vérifié, et fermer l'onglet efface tout. Sans la variable, le
formulaire n'existe pas — Streamlit Cloud ne la définit pas.

**Les trois pages ont été vérifiées ainsi, en conditions réelles**, sur un
portefeuille de huit positions. Quatre défauts n'apparaissaient qu'à ce
moment : lots répétés dans la carte de chaleur et dans l'anneau, valeur
totale coupée en deux, première étape du plan étiquetée « ensuite ».

## Écarts assumés au canevas

Ce que le canevas demande et que l'application ne fait pas, avec la raison.
Aucun n'est un oubli ; chacun a été décidé.

| Ce que le canevas montre | Ce qui est livré | Pourquoi |
|---|---|---|
| Colonnes **Bêta** et **RSI** au tableau des cotations | Les deux, **plus la corrélation** — un bêta dont la corrélation tombe sous 0,30 s'affiche en gris | Le calcul a révélé que la corrélation au marché est faible sur cette place (0,05 à 0,69). Un bêta seul aurait fait croire à une sensibilité qu'il ne mesure pas. |
| Carte de chaleur de calibration sur **4 horizons** | 1 et 3 mois | L'historique des signaux ne couvre pas encore six ni douze mois. Une colonne vide n'est pas une information. |
| **Impact chiffré** sur chacune des 3 étapes du plan | Sur deux étapes | « Ouvrir de nouvelles lignes » n'a pas d'impact calculable avant de choisir les montants. Case vide plutôt que nombre inventé. |
| Graphique en **axes parallèles** (Profil comparatif) | Barres horizontales groupées, déjà en place | Les deux répondent à « quelle est la forme de ce titre ». Le second est au moins aussi lisible et existait ; le remplacer n'aurait rien apporté. |
| KPI du Dashboard sous-titrés **« +2 vs veille »** | « 38 % de la cote » | Les comptes de la veille ne sont pas stockés. La part du marché est calculable et dit autant. |
| **Tri par clic sur l'en-tête** du tableau des cotations | Sélecteur « Trier par » explicite | Un tableau HTML ne se trie pas au clic. Le tri reste, il se voit. |
| Une seule **bande d'accès** sous les listes Top 5 | Idem | Les cinq boutons par liste ont disparu ; le saut vers un titre précis reste au sélecteur du tableau des cotations. |
| Icône « B » en pastille | Emoji de la barre d'onglet inchangé | `st.set_page_config` prend un emoji ou une image ; changer demanderait de livrer un fichier d'icône. Non fait. |
