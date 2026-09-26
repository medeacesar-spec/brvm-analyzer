# Projet de recherche — étape 2 : quels groupes de titres battent le Composite ?

Première mesure du banc (`scripts/recherche_groupes.py`), au 26/09/2026, sur
les groupes qui ne demandent que les cours. Les données sont celles de
l'étape 1 (`docs/recherche_donnees_cours.md`).

## Le banc

Chaque 1er janvier, une règle choisit des titres avec ce qui était connu au
31 décembre. On les détient l'année entière, à poids égaux, et on compare au
BRVM Composite. Vingt-sept années complètes, 1999 à 2025. Les seuils (un
cinquième de la cote, trois ans d'historique) sont fixés d'avance : rien n'est
ajusté sur les résultats.

Chaque groupe est mesuré de trois façons, parce que la première trompe :

- **Moyenne** : poids égaux, toute la cote.
- **Médiane** : le rendement du titre médian du groupe. Un titre multiplié par
  quatorze en un an (Nestlé CI en 2021 : 320 à 4 605 FCFA, confirmé sur
  sikafinance) ne fait plus à lui seul la performance d'un groupe de sept.
- **Liquides** : poids égaux, parmi la moitié de la cote la plus échangée
  l'année précédente — les titres qu'on aurait réellement pu acheter.

« Écart » : rendement annualisé du groupe moins celui du Composite, en points
par an. « Stable » : l'écart a le même signe sur 1999-2011 et sur 2012-2025.

## Ce qui tient

| Groupe | Moyenne 27 ans | Médiane 27 ans | Liquides 27 ans | Stable (3 variantes) | Années battues |
|---|---|---|---|---|---|
| **Momentum 1 an : le cinquième le plus fort** | +9,0 | +7,6 | +8,2 | oui, oui, oui | 58 à 69 % |
| **Secteur bancaire** | +3,8 | +3,5 | +3,1 | non, oui, oui | 56 à 63 % |
| **La plus grosse capitalisation, à éviter** (1 titre) | −4,7 | −4,7 | −4,3 | oui, oui, oui | 52 % |
| **Le cinquième le plus gros, à éviter** | −2,0 | −4,2 | −0,4 | oui, oui, non | 37 à 52 % |

**Acheter en janvier le cinquième des titres qui ont le plus monté l'année
précédente** est le seul groupe qui bat le Composite dans les trois mesures,
sur les deux moitiés de la période. L'effet survit au retrait de sa meilleure
année (+5 à +6,5 points restent). Deux réserves : il renouvelle les trois
quarts de ses titres chaque année, soit de l'ordre de 1 à 1,5 point de frais
par an ; et parmi les titres liquides, il s'affaiblit depuis 2012 (+3,6 points,
contre +13,7 avant).

**Les banques**, en panier, battent l'indice d'environ trois points par an, avec
une rotation presque nulle. C'est modeste, mais c'est le plus régulier.

**Les plus grosses capitalisations font moins bien que l'indice.** Le titre le
plus capitalisé (Sonatel, la plupart des années) perd 4 à 5 points par an
face au Composite dans les trois mesures, sur les deux moitiés. Le cinquième
le plus gros perd aussi, moins nettement. Le Composite étant pondéré par la
capitalisation, cela veut dire que le reste de la cote a fait mieux que ses
géants. La capitalisation est ici une **reconstitution** : cours ajusté ×
nombre d'actions actuel, juste tant que le capital n'a changé que par
division ; une augmentation de capital en numéraire gonfle la taille des
années d'avant. Pour un classement du plus gros au plus petit, l'erreur
change peu les rangs.

**Les plus petites capitalisations ne battent l'indice qu'en moyenne** (+8,7) :
en médiane (+1,3) et parmi les titres liquides (−0,5), l'avantage disparaît.
Même mécanisme que pour les perdants : quelques rebonds géants, et le biais
du survivant.

## Le mois d'achat

Le banc achète en janvier. Refait avec un achat dans chacun des douze mois
(détention de douze mois, 1999-2024), il répond à deux questions : un
résultat tient-il à janvier, et le mois compte-t-il ?

| Groupe | Écart selon le mois d'achat (moyenne / médiane / liquides) | Signe |
|---|---|---|
| Momentum 1 an, le cinquième le plus fort | +5,6 à +10,1 / +0,9 à +8,0 / +1,7 à +7,7 | positif les 12 mois, les 3 mesures |
| Secteur bancaire | +2,6 à +4,3 / +1,1 à +3,3 / +2,0 à +4,0 | positif les 12 mois, les 3 mesures |
| Le cinquième le plus gros | −5,0 à −2,1 / −7,7 à −3,7 / −5,4 à −0,2 | négatif les 12 mois, les 3 mesures |
| Le cinquième le plus petit | +2,7 à +8,3 / −4,4 à −1,1 / −1,5 à +5,9 | dépend de la mesure |
| Toute la cote | +3,8 à +6,1 / −3,3 à −1,3 / +0,0 à +1,8 | dépend de la mesure |

**Aucun des résultats solides ne tient au choix de janvier** : ils gardent leur
signe quel que soit le mois d'achat. Le mois change l'ampleur. Pour le
momentum, **janvier et février sont les meilleurs départs** dans les trois
mesures (+7 à +10 points), **juillet le plus faible** (+1,3 à +6,4). Choisir
le meilleur des douze mois après coup serait toutefois s'ajuster aux
données : c'est une indication, pas une règle.

## La saison dans l'année

Rendement du Composite par mois civil, 1999-2025 (vingt-sept observations par
mois).

| Mois | Moyenne | Médiane | Mois en hausse | Titre moyen hors Sonatel |
|---|---|---|---|---|
| janv. | −0,9 | −2,0 | 33 % | |
| févr. | +2,3 | +3,1 | 67 % | |
| juil. | −1,4 | −2,4 | 26 % | |
| **déc.** | **+4,5** | **+3,5** | **81 %** | **+5,2** (+0,6 les autres mois) |
| les huit autres mois | −0,5 à +1,0 | | 41 à 67 % | |

**Décembre est le mois le plus net de la cote.** Le Composite y gagne 4,5 %
en moyenne, en hausse 22 années sur 27, avec la même ampleur sur 1999-2011
(+4,4) et sur 2012-2025 (+4,5). L'écart est trop grand pour le hasard (t =
4,8, qui résiste au fait d'avoir testé douze mois). Les clôtures sont
confirmées sur sikafinance, et le mouvement est large : le titre moyen, hors
Sonatel, gagne 5,2 % en décembre contre 0,6 % les autres mois. Une
explication plausible, non démontrée ici : les institutionnels de la zone
valorisent leurs portefeuilles au 31 décembre.

**Janvier suit décembre en baisse** (un tiers seulement de mois en hausse) :
acheter en décembre plutôt qu'en janvier capte le mouvement plutôt que de le
payer. **Février** est fort, mais seulement depuis 2012 (+3,9, contre +0,6
avant) : à confirmer. **Juillet** est le mois des dividendes (86 des 302
paiements connus) : un indice de prix y perd le montant détaché sans que
l'actionnaire ait rien perdu. Sa faiblesse est au moins en partie
comptable.

## Ce qui ne tient pas

| Groupe | Moyenne 27 ans | Médiane 27 ans | Liquides 27 ans |
|---|---|---|---|
| Momentum 1 an : le cinquième le plus faible | +12,6 | −0,1 | −5,0 |
| Momentum 3 ans : le cinquième le plus faible | +14,6 | −0,4 | −1,7 |
| Liquidité : le cinquième le moins échangé | +8,8 | −0,9 | −1,7 |
| Forte volatilité | +9,5 | −4,5 | −2,1 |
| Toute la cote, poids égaux | +5,5 | −2,1 | +1,2 |

Les résultats les plus spectaculaires — acheter les perdants, les titres les
moins échangés, les plus agités — **viennent de quelques rebonds géants de
titres qu'on ne pouvait guère acheter**. En médiane ou parmi les titres
liquides, ils disparaissent ou s'inversent. Ils sont en outre flattés par le
biais du survivant : les perdants qui ont fini radiés ne sont pas dans la base.

**Le momentum à trois ans perd** : le cinquième le plus fort sur trois ans fait
moins bien que l'indice dans les trois mesures (−5,9 en médiane, −3,7 parmi
les liquides), sur les deux moitiés. Un titre qui a monté un an continue ; un
titre qui a monté trois ans s'essouffle. C'est ce qu'on observe sur les autres
marchés.

La faible volatilité, la liquidité et le titre unique (le plus fort, le plus
faible, le plus échangé) ne donnent rien de stable.

## Ce que ce banc ne mesure pas encore

- **Les dividendes.** Cours contre cours : le Composite est un indice de prix.
  Les groupes à fort rendement (banques, services publics) sont désavantagés ;
  l'écart des banques est donc plutôt sous-estimé.
- **Les radiés d'avant 2016.** Voir l'étape 1. Le momentum à un an y est peu
  sensible — il achète ce qui monte — ; les groupes de perdants le sont
  beaucoup.
- **Les frais.** Aucun n'est déduit.
- **La capitalisation réelle.** Elle est reconstituée (voir plus haut) ; une
  vérification sur les capitalisations publiées par la BRVM reste à faire.

## Suite

1. Dividendes : les ajouter au rendement des titres (avis de la BRVM
   2015-2025), puis les comparer à un Composite dividendes réinvestis
   reconstitué.
2. Frais : déduire un coût par rotation, pour dire ce qui reste du momentum.
3. Les groupes fondamentaux (PER, rendement, ROE) sur 2021-2025.
