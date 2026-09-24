# BRVM Analyzer — passation

**Dépôt** `medeacesar-spec/brvm-analyzer` · `/Users/mdegbe/brvm-analyzer`
**État au 24 septembre 2026.** Aucune PR ouverte : tout est fusionné dans `main`.

Ce document est fait pour être lu en entier avant la première modification.
`docs/chantiers.md` dit quoi faire ensuite ; celui-ci dit comment, et surtout
ce qu'il ne faut pas refaire.

---

## 1. Trois règles de survie

**L'application déployée peut être en retard sur `main`.**
`brvm-analyzer.streamlit.app` ne recharge pas le code toute seule : après une
fusion, il faut la redémarrer (*Manage app* → *Reboot*). Trois fois ce mois-ci,
un correctif présent dans `main` n'était pas visible pour le donneur d'ordre, et
la première réponse a été de lui décrire un rendu local — qui ne prouve rien.
Dans l'iframe du site déployé, `get_page_text` renvoie du vide : seule la
capture d'écran dit la vérité.

**La base est commune au local et à l'application publique.**
Les comptes `local` et `dev@local` y portent de vrais portefeuilles. Un visiteur
non connecté reçoit l'identifiant `anonyme`, jamais `local` — corrigé le 16/09
après qu'il ait vu les lignes d'un autre.

**Le workflow git.** Une branche par sujet, partie de `origin/main`, PR ouverte
par l'API GitHub, jamais de push direct. Ne jamais empiler deux branches. Si une
PR passe en conflit, c'est presque toujours qu'elle a pris du retard : la
remettre à jour avec `main` plutôt que la refaire.

---

## 2. L'architecture

**Hébergement.** Streamlit Cloud (Python 3.13 épinglé, `numpy<2`), base Supabase
Postgres via le *Transaction Pooler* port 6543 — d'où `prepare_threshold=None`
obligatoire dans `psycopg.connect`, sans quoi toute deuxième requête échoue.

**Onze pages** dans `views/` : tableau de bord (p1), fiche titre (p2), screening
(p3), comparateur (p4), signaux (p5), portefeuille (p6), infos marché (p8),
performance des titres (p9), calibration (p10), analyses (p11).

**Vingt modules d'analyse** dans `analysis/`. Les plus structurants :

| module | ce qu'il porte |
|---|---|
| `risque.py` | volatilité, bêta, perte maximale, fenêtre réglable (3, 5, 8, 10 ans, tout) |
| `portefeuille_risque.py` | risque d'ensemble, candidats d'amélioration, répartition du cash |
| `fundamental.py` | ratios et score fondamental /50, grille sectorielle |
| `technical.py` | indicateurs et score technique /50, avec décomposition par sous-critère |
| `scoring.py` | score hybride et verdict consolidé |
| `backtest.py` | banc de mesure commun aux backtests de signaux |
| `indices.py` | **registre unique** des 17 indices — y ajouter un indice, pas ailleurs |
| `cotation.py` | depuis quand un titre est coté (distingue une introduction d'un titre éteint) |
| `lecture_syscohada.py` | lecteur d'états financiers, **ne sert pas encore à écrire** |
| `revue.py`, `pertinence.py` | revue de presse, 20 dépêches par page, pages disjointes |

**Couche données** : `data/db.py` (abstraction SQLite/Postgres, traduit `?` en
`%s`, réutilise la connexion par session), `data/storage.py` (CRUD et requêtes),
`data/pdf_extractor.py` (extraction PDF avec OCR), `data/scraper.py` (brvm.org et
sikafinance), `data/indices_marche.py` (indices du jour).

**Trente-quatre tables.** Les principales : `price_cache` (journalier),
`price_monthly`, `fundamentals` (+ `fundamentals_journal`, journal de toutes les
écritures), `quarterly_data`, `market_data`, `report_links`, `publications`,
`news_articles`, `portfolio`, `dividends`, `indices_cache`, `scoring_snapshot`.

**Soixante-quatre scripts** dans `scripts/`. Beaucoup sont des migrations jouées
une fois ; les vivants sont listés au §6.

**Six workflows GitHub Actions** : `intraday_refresh` (9h, 11h, 13h, 15h UTC en
semaine — cotations + indices), `daily_snapshot` (16h UTC — collecte des
publications, extraction, instantanés), `keepalive` (toutes les 2 h — session
WebSocket réelle, un ping HTTP ne suffit pas), plus trois de rattrapage.

---

## 3. Ce que la BRVM impose de savoir

**L'exercice N est publié au printemps N+1.** Au 31 janvier 2026, la dernière
liasse connue est celle de 2024. Tout backtest qui l'ignore se félicite de
prédire le passé.

**Un trimestre n'est pas un exercice**, et une ligne « exercice » peut en porter
un. Un document annuel référencé ne prouve rien sur les montants de la ligne :
le recoupement se fait avec `quarterly_data`, en excluant les périodes `S2` et
`T4` — chez BOA Burkina, la ligne S2 porte le montant de l'année entière.

**Les trimestres ne sont pas tous cumulés.** SITAB et CFAO publient des
trimestres autonomes : la monotonie T1 ≤ S1 ≤ T3 n'est pas universelle.

**Les cours sont ajustés** des divisions et attributions gratuites, pas des
dividendes. `analysis/risque.py` ajoute les dividendes lui-même, au mois réel de
versement (`dps_paiement`), pas en juillet par défaut.

**L'IRVM** vaut 12 % en Côte d'Ivoire et 10 % au Sénégal. La base porte le
dividende **brut** — sauf Sonatel, qui publie le net et dont le brut est
reconstitué (÷ 0,90).

**La nomenclature sectorielle a changé.** Sept indices historiques s'arrêtent au
31/12/2025, six nouveaux commencent au 02/01/2025 : un an de chevauchement, des
paniers différents. Ils ne se recollent pas.

---

## 4. L'état de la donnée

| donnée | couverture | source |
|---|---|---|
| cours journaliers | 218 898 séances depuis 1998 | import RichBourse + collecte |
| indices | 17 séries | registre `analysis/indices.py` |
| chiffre d'affaires, résultat net | **236 / 240** couples 2021-2025 | fiches société + documents |
| dividende par action | 89 % | 414 avis de paiement BRVM |
| dividende déclaré | 218 exercices | calculé : DPS × actions |
| résultat d'exploitation | 64 % | états financiers (PDF) |
| capitaux propres | 62 % | idem |
| total de bilan | 56 % | idem |
| dette totale | 40 % | idem |
| EBITDA | 34 % | idem |
| charges d'intérêts, flux, investissements | 13-14 % | idem |

Le compte de résultat est presque complet ; **le bilan et les flux ne le sont
pas**. C'est le chantier prioritaire.

---

## 5. Ce qui a été fait du 10 au 24 septembre

**Données.** Dix-sept exercices 2025 lus dans les documents officiels et vérifiés
un à un (#175, #176) ; 49 trimestres qui se faisaient passer pour des exercices,
corrigés par recoupement avec les fiches société (#181) ; neuf alertes
instruites, dont quatre bénéfices écrits à la place de pertes (#182) ; les
dividendes Sonatel passés du net au brut sur huit exercices (#172) ; le dividende
déclaré calculé pour 218 exercices (#179) ; Bridge Bank intégrée le jour de son
introduction, avec ses cinq exercices (#177).

**Affichage.** Le YTD des indices, faux parce que brvm.org publie une colonne
figée depuis janvier — le Composite affichait +1,70 % pour +53,4 % réels (#183).
La répartition suggérée du cash, qui ignorait la fenêtre de risque (#185). Les
messages d'un titre nouvellement coté, qui le traitaient comme un titre éteint
(#184). La revue de presse, 20 dépêches par page et pages disjointes (#168). Le
portefeuille d'un visiteur non connecté, qui montrait celui d'un autre (#169).

**Performance.** Démarrage à froid de 66 s à 14 s (#173) : l'application lisait
218 000 séances à chaque ouverture et interrogeait brvm.org avant le premier
affichage. Avant cela, la réutilisation des connexions avait déjà fait passer une
ré-exécution de 19 s à 6 s.

**Historique.** Import RichBourse : `price_cache` de 13 644 à 218 898 séances,
`price_monthly` de 2 847 à 11 645 mois, et les 17 indices (#164, #166).

**Outillage.** Backtests des signaux (#165), lecteur d'états financiers et son
étalon (#178, #186), outils de complétion et de recoupement.

---

## 6. Les outils, et quand s'en servir

Tous acceptent `--simuler` ou n'écrivent rien par défaut. **Toujours simuler
d'abord**, lire la sortie, puis écrire.

| script | à quoi il sert |
|---|---|
| `recouper_fiches_societe.py` | compare la base aux fiches société, corrige sur double confirmation, signale le reste |
| `dividendes_declares.py` | calcule le dividende déclaré depuis les avis BRVM |
| `collecter_avis_dividendes.py` | moissonne les 414 avis de paiement (option `--titre`) |
| `completer_fondamentaux.py` | remplit les champs manquants depuis les PDF (option `--sans-scans`) |
| `etalon_lecture_syscohada.py` | **mesure** le lecteur contre 14 documents vérifiés |
| `importer_richbourse.py` | importe les exports CSV de `csv/` (idempotent) |
| `integrer_bridge_bank.py` | modèle pour intégrer un nouveau titre |
| `backtest_technique.py`, `_fondamental`, `_croise`, `_ponderation`, `_souscriteres` | mesure des signaux |
| `refresh_intraday.py`, `build_daily_snapshot.py` | ce que font les workflows |

---

## 7. Ce que les backtests ont établi

Consigné dans `docs/backtest_signaux.md`. À ne pas refaire :

- les deux scores **rangent** les titres, mais faiblement (rho ≈ 0,12) ;
- **aucune pondération** entre fondamental et technique ne survit hors
  échantillon : le 50/50 reste, faute de mieux démontré ;
- **cinq sous-critères techniques sur six changent de signe** selon la période.
  Le RSI, le plus négatif avant 2014, est le plus positif depuis 2024. Les
  repondérer serait courir après du bruit ;
- le **volume** est la seule composante dont le signe tient sur toutes les
  fenêtres.

---

## 8. Comment on vérifie ici

**Rendu réel, jamais déduit du code.** `streamlit.testing.v1.AppTest` permet de
rendre une page sans navigateur et de lire ce qu'elle affiche. C'est ainsi qu'ont
été vérifiés les messages de Bridge Bank et les 20 dépêches par page.

**Mesure avant et après.** Le démarrage à froid, le YTD, le lecteur d'états
financiers : chaque changement est accompagné du chiffre qu'il déplace.

**Recoupement à trois sources** pour la donnée : la colonne comparative du
document, le cumul à neuf mois déjà en base, et l'exercice précédent.

**Attention aux fenêtres qui se chevauchent** dans les backtests : chaque titre
fournit une décision par mois et un rendement à douze mois, donc le `n` affiché
surestime largement le nombre d'observations indépendantes.

---

## 9. Ce qui reste fragile

**Les documents sont des scans.** Sur vingt-deux téléchargés, deux portaient du
texte. L'OCR fonctionne mais casse les chiffres (`4 2 454 158 321`).

**Le lecteur d'états financiers n'écrit rien.** 17 valeurs justes sur 36. Une
valeur fausse mais vraisemblable est pire qu'une valeur absente : pour NSIA, il
rendait le produit net bancaire de 2024 à la place de 2025 — plausible, donc
invisible.

**Deux dépendances extérieures.** sikafinance pour les fiches société (validée :
19 concordances sur 19 avec les documents officiels) et brvm.org pour les avis,
les rapports et les indices. Les deux ont déjà publié des chiffres faux.

**Les caches.** 5 minutes sur les données de marché, 10 minutes sur l'alerte des
titres dormants, 6 heures sur les indices. Un correctif peut sembler sans effet
pendant ce délai.

---

## 10. Les décisions en attente

1. **Les 52 écarts modérés** entre la base et les fiches société : le plus
   souvent une différence de définition — comptes sociaux contre consolidés,
   produit net bancaire contre produits totaux. Aucune règle automatique ne les
   tranche.
2. **Les données de test en production** : `local` (8 lignes, un profil
   investisseur) et `dev@local` (16 lignes). Les supprimer ?
3. **Deux séries d'indices manquantes** : Composite Total Return et Services
   financiers. Sans historique, leur YTD est effacé plutôt que faux. Un export
   RichBourse les réglerait.
4. **Deux corrections sur source unique** : Safca 2022 et Erium 2021, sans
   document publié. À revérifier au prochain dépôt.
5. **Deux correctifs de keepalive** proposés en septembre et jamais tranchés :
   resserrer le cron, et faire échouer le workflow quand aucune session WebSocket
   ne s'ouvre.

---

## 11. Les pièges qui ont coûté le plus cher

**Une valeur fausse mais vraisemblable est pire qu'une valeur absente.** C'est la
règle qui commande tout le travail sur la donnée.

**Recopier une source extérieure se paie.** Deux fois : les rendements collectés
sur le web, faux pour la plupart des titres, et le YTD de brvm.org, figé depuis
janvier. Quand l'historique est en base, calculer vaut mieux que recopier.

**Un chiffre improbable n'est pas toujours une erreur.** Sur sept PER aberrants
instruits, trois étaient exacts : Erium, AGL et BOA Niger gagnent réellement très
peu. Les corriger sans lire le document aurait remplacé un faux chiffre par un
autre.

**Un réglage doit se propager jusqu'au bout.** La fenêtre de risque était lue par
la mesure mais pas par la répartition du cash : le plan répondait à une question
que personne n'avait posée. Même défaut, plus tôt : un sélecteur placé après le
calcul n'agit qu'au rerun suivant.

**Une exception avalée cache un bug pendant des semaines.** `except Exception:
pass` sur une requête SQL invalide a fait passer un contrôle d'échelle pour
concluant alors qu'il ne tournait jamais.

---

## 12. Où reprendre

`docs/chantiers.md` fixe l'ordre : les états financiers sur cinq ans d'abord,
puis la robustesse du fil d'actualité, l'alerte « sans compte annuel », et le
projet de recherche sur les groupes de titres qui battent la BRVM.

Pour le premier, la suite immédiate est identifiée : dans
`analysis/lecture_syscohada.py`, faire gagner « TOTAL CAPITAUX PROPRES » sur
« Sous-Total part du groupe », puis lire les montants rangés dans des tableaux
plutôt que dans le texte — quatre documents de l'étalon en dépendent. Mesurer
après chaque changement avec `scripts/etalon_lecture_syscohada.py`.
