# Chantiers

Ce que l'application doit devenir, dans l'ordre. Les chantiers termines sont
retires ; ce qui reste ici est ce qui reste a faire.

---

## 1. Les etats financiers des cinq dernieres annees — PRIORITE

**L'etat des lieux, mesure le 24/09/2026** sur 48 titres et les exercices
2021-2025, soit 240 couples et **1 783 trous** :

| champ | couverture | | champ | couverture |
|---|---|---|---|---|
| chiffre d'affaires | 98 % | | EBITDA | 34 % |
| resultat net | 97 % | | cout du risque | 22 % |
| dividende par action | 89 % | | depots | 19 % |
| nombre d'actions | 89 % | | charges d'interets | 14 % |
| resultat d'exploitation | 64 % | | investissements | 14 % |
| capitaux propres | 62 % | | flux de tresorerie | 13 % |
| total de bilan | 56 % | | dividendes verses | 6 % |
| dette totale | 40 % | | | |

**Ce qui est fait.** Le dividende declare se calcule depuis les 414 avis de
paiement de la BRVM, sans ouvrir un document : 218 exercices (PR #179). Les
exercices 2025 de dix-sept titres ont ete lus dans les documents officiels
et verifies un a un (PR #175, #176).

**Ce qui bloque.** Sur vingt-deux documents telecharges, DEUX portaient du
texte : le reste est scanne. L'extraction automatique rend 10 valeurs justes
sur 36 (PR #178), et surtout des valeurs fausses mais PLAUSIBLES — pour NSIA
elle rend le produit net bancaire de 2024 a la place de celui de 2025.

**La voie retenue.** Recouper avec les fiches societe, qui publient cinq
exercices d'un coup, puis lire a la main les ecarts signales. Le volume doit
rester verifiable : une liste de trente ecarts se controle, une liste de
trois cents ne se controle pas.

---

## 2. Robustesse de l'integration dans le fil d'actualite

L'integration des rapports et etats financiers depuis le fil n'est pas assez
sure. A reprendre : ce qui est propose a l'integration, ce qui est ecrit, et
ce qui doit etre signale plutot qu'ecrit.

---

## 3. Alerte « titre sans compte annuel » et « titre dormant »

Definition a valider avant de coder :

- **sans compte annuel** : aucun etat financier annuel publie pour le dernier
  exercice clos, alors que la cadence UEMOA le voulait au printemps suivant ;
- **dormant** : peu ou pas de seances cotees sur une periode a definir.

Movis CI montre le besoin : elle est retiree de la cote et porte le drapeau
`retire`, mais rien ne signale un titre qui s'eteint doucement.

---

## 4. Projet de recherche — quels groupes de titres battent la BRVM ?

**La question.** Quels groupes de titres ont battu le Composite de facon
constante, et sur quelle duree ? En achetant chaque debut d'annee la plus
grosse capitalisation, bat-on l'indice sur 10, 20 ou 30 ans ? Et la plus
petite ? La mediane ? Le titre qui a le plus progresse l'annee precedente ?

**Ce qui rend la question mesurable aujourd'hui.** `price_cache` porte
169 996 seances depuis septembre 1998, cours ajustes des divisions et des
attributions gratuites, et le Composite remonte a la meme date.

**Ce qui se teste sur vingt-huit ans** — tout ce qui se deduit des cours :
momentum a un an et a trois ans, retour a la moyenne, volatilite, liquidite,
paniers sectoriels.

**Ce qui ne se teste que sur cinq ans** — tout ce qui demande les
fondamentaux : PER, rendement, rentabilite des capitaux propres, croissance
des benefices, et la capitalisation REELLE. Le nombre d'actions par exercice
n'est connu qu'a partir de 2021 ; avant, une capitalisation historique
serait une reconstitution, pas une mesure. La taille peut s'approcher par le
volume echange, mais c'est un substitut et il faudra le dire.

**La methode.** Un banc unique, ou chaque groupe est une regle de selection
annuelle, comparee au Composite sur 5, 10, 20 et 28 ans. Les pieges des
backtests de signaux valent ici : mediane plutot que moyenne, rendement
RELATIF a l'indice, aucune periode ecartee, et verification hors
echantillon. Voir `docs/backtest_signaux.md`.

**Pourquoi ce chantier vient apres le premier.** La moitie des groupes a
tester demande les fondamentaux. Les mesurer sur une base a 60 % de trous
donnerait des classements qui parlent de la donnee manquante, pas du marche.
