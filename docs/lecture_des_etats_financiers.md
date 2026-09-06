# Lire les états financiers de la BRVM — ce que le cycle 2025 a appris

Ce document existe pour que le cycle suivant ne recommence pas à zéro. Il ne
décrit pas le code : il décrit **les documents**, ce qu'ils contiennent
vraiment, et les recoupements qui tranchent quand deux lectures s'opposent.

---

## 1. Le calendrier et les périodes

- L'exercice N est publié au printemps N+1. Un document daté d'avril 2026 porte
  l'exercice 2025.
- Les rapports périodiques sont **cumulatifs** : T1 = 3 mois, S1 et T2 = 6 mois,
  T3 = 9 mois, T4 et S2 = 12 mois. Un trimestre n'est jamais un exercice.
- Les **flux** (résultat, chiffre d'affaires, trésorerie générée) ne se lisent
  que dans un rapport annuel. Les **stocks** (bilan) se lisent aussi dans un
  rapport de période.
- Un document intitulé « note de recherche » ou « analyse » n'est pas un état
  financier. Signe distinctif : plus de six colonnes de montants, dont des
  exercices futurs. Il ne doit jamais alimenter la base.

## 2. Les deux référentiels

SYSCOHADA et IFRS coexistent, et plusieurs sociétés publient les deux jeux.
**Ne jamais mélanger les deux dans une même ligne.** La SODECI porte 27 898 M
de capitaux propres en IFRS et 19 716 M en SYSCOHADA : écrire l'un en face d'un
actif issu de l'autre fabrique un ratio faux et invisible.

Le recoupement qui tranche : le **total du bilan**. Si le total général du
document ne correspond pas à l'actif déjà en base, les deux lignes viennent de
référentiels différents.

## 3. Où se trouve la dette

C'est la découverte la plus coûteuse du cycle.

**En SYSCOHADA, le crédit bancaire à court terme n'est pas dans les « dettes
financières ».** Il est tout en bas du bilan, sous **TRÉSORERIE PASSIF** :
« Banques, crédits d'escompte » et « Banques, établissements financiers et
crédits de trésorerie ». Chez BNBC ce poste pèse 18 536 M quand la ligne
« Emprunts et dettes financières diverses » n'en porte que 209 — 99 % de
l'endettement réel ignoré, un ratio de 0,01 au lieu de 1,06.

Corollaires :

- **La dette financière = emprunts + dettes de location + trésorerie-passif.**
- Le total imprimé « TOTAL DETTES FINANCIÈRES ET RESSOURCES ASSIMILÉES » n'est
  **pas** la dette financière : il inclut les provisions pour risques et
  charges. Chez NEI-CEDA il vaut 85 823 448 alors que la dette est nulle.
- Une dette **nulle** est une information, pas une absence. NEI-CEDA et SITAB
  affichent zéro (ou 1,4 M) et cela doit s'écrire.
- Ce qui porte le mot « dette » sans être de la dette financière :
  fournisseurs, dettes fiscales et sociales, créditeurs divers, dettes
  circulantes HAO, et — pour une banque — les **dépôts de la clientèle**, qui
  sont sa matière première.
- Le tableau de flux emploie le même vocabulaire pour des **mouvements** :
  « Remboursement des emprunts », « Augmentation autres dettes financières ».
  Ce ne sont pas des encours de clôture.

## 4. Les capitaux propres

Le total est presque toujours imprimé (« TOTAL CAPITAUX PROPRES ET RESSOURCES
ASSIMILÉES ») : c'est lui qui fait foi. Reconstituer par les composantes
(capital, primes, écarts de réévaluation, réserves, report à nouveau, résultat)
n'est légitime que si elles forment un **bloc contigu** et que leur somme
retombe sur le total déclaré.

## 4 bis. Un bilan consolidé porte TROIS lignes de capitaux propres

En IFRS consolidé, le passif en aligne trois d'affilée :

- **Capitaux propres attribuables aux propriétaires de la société mère** — la
  part du groupe ;
- **Capitaux propres attribuables aux participations ne donnant pas le
  contrôle** — les minoritaires ;
- **Total capitaux propres**, qui est la somme des deux.

Se tromper de ligne ne se voit pas : les trois sont plausibles. Chez Sonatel,
la base portait 224 291 millions pour l'exercice 2024 — les minoritaires — et
1 160 715 pour 2025 — la part du groupe. Deux lignes différentes, deux
exercices, et des capitaux propres qui semblaient quintupler en un an. Le
total, lui, vaut 1 274 638 puis 1 399 263.

**C'est le TOTAL qui s'écrit**, parce que c'est lui qui referme le bilan :
total des capitaux propres + passifs non courants + passifs courants = total
du passif. La part du groupe ne referme rien.

## 5. Les recoupements qui tranchent

À utiliser systématiquement avant d'écrire une valeur :

1. Somme des composantes = total déclaré des capitaux propres.
2. Ressources stables + passif circulant + trésorerie-passif + écart de
   conversion = **total général**.
3. Total actif = total passif.
4. Le résultat net du bilan = le résultat net du compte de résultat.
5. Le total du bilan d'un exercice se retrouve en colonne N-1 du suivant.

Exemple : chez SOLIBRA, 169 443 + 58 381 + 60 888 + 18 832 + 2 = 307 546, le
total déclaré au million près. Cette égalité a confirmé une trésorerie-passif
de 18 832 contre les 188 322 que produisait une lecture fautive.

## 6. Les formes que prennent les documents

Le moteur doit reconnaître **la forme avant le contenu**. Recensées à ce jour :

| Forme | Signe | Traitement |
|---|---|---|
| Texte normal, tableau régulier | — | lecture par coordonnées |
| **Une espace entre chaque glyphe** — « D e tte s fin an c iè re s » | plus de 15 % des jetons font une seule lettre | recoller les fragments dont l'écart est dix fois plus étroit qu'un vrai blanc (0,19 pt contre 1,9) — SOLIBRA, SUCRIVOIRE, SONATEL |
| **Glyphe surchargé** — un glyphe se décode en deux chiffres | le mot est trop étroit pour ses caractères | ne garder que ce que la largeur autorise, **en comparant des chiffres de même corps** |
| **Encodage cassé** (pas de table ToUnicode) | proportion de lettres effondrée dans la page | écarter la page et passer par la lecture optique |
| **Image / scan** | la page ne rend aucun texte | lecture optique à 300 points par pouce |
| Diapositive de présentation | montants centrés, pas de colonnes | lecture par centrage |
| Bilan à deux panneaux (actif à gauche, passif à droite) | deux libellés par ligne | candidats en milieu de ligne |
| Deux colonnes collées | plus de quinze chiffres d'un bloc, nombre pair | scinder, **en conservant le signe lu avant le premier chiffre** |

## 7. Les pièges de lecture, un par un

- **Colonne BRUT et colonne NET.** Un bilan SYSCOHADA porte les deux à l'actif.
  Chez NEI-CEDA, lire la mauvaise donne 9,93 Md au lieu de 7,47.
- **Colonne de renvoi de note.** « Emprunts | 13 | 66 | 79 » : le 13 est un
  numéro de note. Il s'écarte par sa nature — un entier inférieur à cent suivi
  d'un nombre plus grand — et non par sa taille.
- **L'ordre des colonnes n'est pas garanti.** Certaines sociétés impriment
  N puis N-1, d'autres l'inverse, d'autres écrivent « N » et « N-1 » sans
  millésime. Trancher par une valeur déjà connue en base.
- **Le signe se lit avant le premier chiffre**, pas après. L'oublier a
  transformé la perte de SETAO en bénéfice du même montant.
- **Le multiplicateur est dans l'en-tête**, parfois deux lignes plus haut, et
  peut différer d'un tableau à l'autre dans la même page.
- **Les séparateurs de milliers** : l'espace en français, la virgule en anglais.
  La convention doit être **prouvée par le document** (un nombre à trois
  groupes, ou un en-tête bilingue) avant d'être appliquée.
- **Les montants en devise étrangère** ne sont pas des francs CFA. Un montant
  suivi de « $ », « USD » ou « euros » se rejette.
- **« Total » et « totaux »** ne s'écrivent pas pareil : un motif écrit
  `totaux?` exige « totau » et ne reconnaît jamais le mot « total ». Cette
  seule lettre a fait échouer Servair, la SODECI et la CIE.

## 8. Règles de conduite

- **Une valeur fournie s'écrit d'abord et se diagnostique ensuite.** Faire
  refaire deux fois le même travail au donneur d'ordre est la faute la plus
  coûteuse.
- **Ne jamais écrire « absent » sans avoir ouvert le document.** Quatre titres
  ont été classés à tort « bilan absent » ou « aucun libellé de dette » —
  NEI-CEDA, SOLIBRA, SITAB, TotalEnergies Sénégal — alors que le bilan était là
  et parfois déjà fourni.
- **Mesurer avant de coder.** Compter combien de titres partagent un symptôme
  avant d'écrire une règle : trois documents sur quarante-huit écrits glyphe
  par glyphe justifient une règle, un cas isolé justifie une saisie.
- **Une méthode qui a produit de l'historique ne se remplace pas en silence.**
