# Projet de recherche — étape 1 : les données de cours

Le chantier 4 de `docs/chantiers.md` pose la question : quels groupes de titres
ont battu le Composite, et sur quelle durée ? Tout ce qui se déduit des cours
se teste sur vingt-huit ans. Encore faut-il que les cours soient justes. Cette
note fait l'état des séries au 26/09/2026, dit ce qui a été corrigé et ce qui
limite encore l'étude.

## Ce que la base contient

`price_cache` porte une séance par ligne : 49 titres et 17 indices, du
16 septembre 1998 au 25 septembre 2026. Les cours sont **ajustés des divisions
et des attributions gratuites** (export RichBourse, recoupé sur sikafinance :
Sonatel 2010, BOA Sénégal 2024). Ils ne le sont **pas des dividendes**.

La liquidité change tout au long de la période. Part médiane des séances où un
titre a réellement échangé :

| Période | Titres | Séances avec échange (médiane) |
|---|---|---|
| 1998-2001 | 23 à 28 | 32 à 59 % |
| 2002-2005 | 27 à 29 | **17 à 24 %** |
| 2006-2014 | 29 à 35 | 31 à 64 % |
| 2015-2020 | 36 à 44 | 81 à 98 % |
| 2021-2026 | 44 à 48 | presque toutes |

Au début des années 2000, un titre moyen ne traitait qu'une séance sur cinq.
Une règle qui « achète » un titre ce jour-là n'aurait souvent pas trouvé de
vendeur. Les tests devront le dire, et raisonner sur des cours mensuels
plutôt que quotidiens avant 2006.

## Trois défauts trouvés et corrigés

**1. Deux séries recopiées d'un autre titre.** Les fichiers d'export
« sicor-ci(SICC).csv » et « unilever-ci(UNLC).csv » étaient, octet pour octet,
ceux de SAPH et de Sucrivoire : même empreinte MD5, deux téléchargements
mal nommés. Sicor portait 5 021 séances de SAPH (1998 à mars 2026), Unilever
2 160 séances de Sucrivoire (2016 à mars 2026). La fiche d'Unilever affichait
« +3 808 % sur un an ». Les séances copiées sont passées en quarantaine
(`price_cache_quarantaine`) et les vraies ont été recollectées sur
sikafinance (`scripts/reparer_cours_copies.py`). sikafinance confirmait SAPH
et Sucrivoire sur 11 séances témoins sur 12, et ni Sicor ni Unilever (0 sur 12).

**2. Des points mensuels glissés dans le quotidien, d'avril 2021 à avril 2026.**
Avant l'import RichBourse, un collecteur avait écrit au premier jour de chaque
mois l'ouverture du mois, son plus haut, son plus bas, sa clôture de fin de
mois et son volume cumulé, et **sans ajustement**. SAFCA sautait chaque début
de mois de 470 à 800 FCFA, BOA CI de 2 125 à 2 575. L'import n'écrasant jamais
une séance existante, ces lignes étaient restées : environ quarante par titre.
S'y ajoutaient des lignes du robot quotidien datées d'un jour férié (Pâques,
Ascension, Tabaski, Korité, 7 et 15 août) ou d'un week-end : la dernière séance
recopiée sous la date de son passage. `scripts/aligner_cours_richbourse.py`
reprend cours et volume de l'export pour les premières, et met les secondes en
quarantaine.

**3. L'historique des indices s'arrêtait au 9 septembre 2026.** Il venait de
l'export seul ; aucun collecteur ne l'alimentait. Le rafraîchissement intraday
écrit désormais la clôture du jour des dix indices publiés par brvm.org
(`data/indices_marche.py`).

## Résultat

| Mesure | Avant | Après |
|---|---|---|
| Sauts de plus de 35 % en une séance | 310 | 5, tous réels (Filtisac −42 % le 26/09/2025 : distribution exceptionnelle de 28,2 Md) |
| Séances en désaccord avec l'export RichBourse | 1 876 | 0 |
| Lignes mises en quarantaine | — | 10 700 (copies 7 181, jours sans séance 1 187, contredites par sikafinance 122, anciennes versions des lignes reprises 2 210) |

Les exports de Sicor et d'Unilever ont été retéléchargés le 26/09/2026 : Sicor
couvre de nouveau 1998-2026, Unilever juin 2001-2026 (RichBourse ne remonte pas
plus loin). Les séances que sikafinance comptait en plus, au cours de la
veille et parfois un jour férié, sont passées en quarantaine comme pour les
autres titres.

**Le mensuel** (`price_monthly`, lu par les mesures de risque et les tests)
portait lui aussi les copies de SAPH et de Sucrivoire, et 86 mois en double :
le collecteur sikafinance datait le mois de sa première séance, l'import du
1er. SAFCA y suivait un autre ajustement de sa division (+1 %, 55 mois). Le
mensuel est aligné sur l'export : un mois, une ligne, clôture de la dernière
séance et volume cumulé. 530 lignes en `price_monthly_quarantaine`.
`price_quarterly`, que rien ne lit, n'a pas été revu.

## Ce qui limite encore l'étude

- **Le biais du survivant.** Seuls les titres cotés aujourd'hui ont un
  historique, plus Movis CI (SVOC, retiré de la cote). Les sociétés radiées depuis 1998
  manquent. Une règle « acheter les plus petites capitalisations » testée sur
  les seuls survivants ignore précisément celles qui ont disparu. Il faut
  retrouver la liste des radiations et leurs cours avant de publier un
  classement sur vingt-huit ans. Ni brvm.org (sa rubrique « Radiation »
  recense surtout des obligations) ni sikafinance (historique mensuel limité à
  cinq ans, aucun ancien code ne répond) ne donnent cette liste : il faudra la
  reconstituer à partir des rapports annuels de la BRVM, puis demander les
  exports à RichBourse.
- **Les dividendes.** Le Composite est un indice de prix, et les cours ajustés
  ne réinvestissent pas les dividendes. Comparer des prix à des prix reste
  juste. Mais un groupe à fort rendement serait pénalisé face à un groupe de
  croissance si l'on s'arrêtait là. Les avis de dividende de la BRVM couvrent
  2015-2025 ; avant, les montants sont à reconstituer.
- **La taille.** Le nombre d'actions n'est connu qu'à partir de 2021 : avant,
  la capitalisation serait une reconstitution. Le volume échangé peut servir de
  substitut, à condition de le dire.
- **L'indice « BRVM - Services financiers »** (nouvelle nomenclature, 2025)
  n'est pas au registre : aucun export n'en a été fait.

## Suite

1. Liste des radiations depuis 1998 et collecte de leurs cours.
2. Dividendes antérieurs à 2015.
3. Le banc de test : chaque groupe est une règle de sélection annuelle,
   comparée au Composite sur 5, 10, 20 et 28 ans, en médiane, en rendement
   relatif, sans période écartée, avec vérification hors échantillon
   (`docs/backtest_signaux.md`).
