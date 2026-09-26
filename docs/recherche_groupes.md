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

**Acheter en janvier le cinquième des titres qui ont le plus monté l'année
précédente** est le seul groupe qui bat le Composite dans les trois mesures,
sur les deux moitiés de la période. L'effet survit au retrait de sa meilleure
année (+5 à +6,5 points restent). Deux réserves : il renouvelle les trois
quarts de ses titres chaque année, soit de l'ordre de 1 à 1,5 point de frais
par an ; et parmi les titres liquides, il s'affaiblit depuis 2012 (+3,6 points,
contre +13,7 avant).

**Les banques**, en panier, battent l'indice d'environ trois points par an, avec
une rotation presque nulle. C'est modeste, mais c'est le plus régulier.

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
- **La taille réelle.** La capitalisation n'est connue qu'à partir de 2021 ; le
  montant échangé en tient lieu, et ce n'est qu'un substitut.

## Suite

1. Dividendes : les ajouter au rendement des titres (avis de la BRVM
   2015-2025), puis les comparer à un Composite dividendes réinvestis
   reconstitué.
2. Frais : déduire un coût par rotation, pour dire ce qui reste du momentum.
3. Les groupes fondamentaux (PER, rendement, ROE) sur 2021-2025.
