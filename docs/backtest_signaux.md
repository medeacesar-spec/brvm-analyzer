# Jusqu'où nos signaux sont-ils justes ?

Ce document rassemble ce que les backtests ont mesuré, et surtout ce qu'ils
n'autorisent pas à conclure. Il se lit avant de toucher à un barème.

Quatre scripts, quatre questions, du général au particulier :

| script | question |
|---|---|
| `backtest_technique.py` | le score technique range-t-il les titres ? |
| `backtest_fondamental.py` | le score fondamental range-t-il les titres ? |
| `backtest_croise.py` | les deux disent-ils la même chose ? |
| `backtest_ponderation.py` | quel partage entre les deux ? |
| `backtest_souscriteres.py` | **à l'intérieur de chaque score, les poids tiennent-ils ?** |

---

## Les trois règles du banc de mesure

1. **Aucun regard vers l'avenir.** Une décision au 30 juin 2019 ne lit que ce
   qui était connu ce jour-là. Pour les cours c'est trivial ; pour les
   fondamentaux, l'exercice N n'est publié qu'au printemps N+1 et
   `exercice_connu_le` applique ce décalage. Sans lui, tous les chiffres
   seraient faux et flatteurs.
2. **Le rendement se mesure contre le Composite.** Gagner 8 % quand la place
   en fait 12 est une perte de 4 points.
3. **On ne jette pas les périodes qui dérangent.** Tout l'historique
   disponible, y compris les années de recul.

## La quatrième règle, apprise à ses dépens

**Un optimum trouvé sur toutes les données ne prouve rien.** Chaque barème
est appris avant 2024, puis vérifié sur 2024-2025, des années que la
recherche n'a jamais vues. Deux fois déjà, un barème brillant à
l'apprentissage s'est effondré à la vérification. C'est la mesure qui
tranche, pas celle de l'apprentissage.

---

## 1. Le partage fondamental / technique : aucun ne tient

Horizon 12 mois, 2 164 décisions communes.

| poids fondamental | rho apprentissage (< 2024) | rho vérification (≥ 2024) |
|---|---|---|
| 0 % (tout technique) | 0,169 | **+0,035** |
| 40 % | **0,243** *(optimum appris)* | −0,061 |
| 50 % (barème actuel) | 0,241 | −0,077 |
| 100 % (tout fondamental) | 0,163 | −0,100 |

L'apprentissage désigne 40 % de fondamental. Hors échantillon, ce poids est
le **pire** de la table sauf deux. Aucune repondération n'est justifiée : le
50/50 actuel reste, faute de mieux démontré.

## 2. Les sous-critères : un seul est stable

C'est le résultat le plus important du chantier, et c'est un résultat
négatif.

### Technique — 10 325 décisions, 1999-2025

| sous-critère | poids | rho global | < 2014 | ≥ 2014 | ≥ 2024 | stable |
|---|---|---|---|---|---|---|
| tendance | 15 | +0,120 | +0,191 | +0,065 | −0,003 | **non** |
| MACD | 10 | +0,093 | +0,151 | +0,043 | −0,013 | **non** |
| momentum | 5 | +0,052 | +0,086 | +0,030 | −0,021 | **non** |
| volume | 5 | +0,038 | +0,031 | +0,045 | +0,025 | **oui** |
| RSI | 10 | −0,056 | −0,142 | +0,013 | +0,084 | **non** |
| Bollinger | 5 | −0,100 | −0,163 | −0,050 | +0,015 | **non** |

Lu vite, ce tableau dit « RSI et Bollinger rangent à l'envers, retirons-les ».
Lu correctement, il dit autre chose : **cinq sous-critères sur six changent de
signe selon la période**. Leur rho d'ensemble est un accident de fenêtre, pas
une propriété. Retirer le RSI parce qu'il était négatif avant 2014 revient à
retirer ce qui, depuis 2024, est devenu le plus positif des six.

La vérification hors échantillon le confirme : « sans RSI ni Bollinger » perd
0,041 de rho par rapport au barème en vigueur.

**Le volume est la seule composante dont le signe tient sur les trois
fenêtres.** Il est aussi la plus faible. C'est peu, et c'est honnête.

### Fondamental — 2 185 décisions, 2020-2025

| sous-critère | poids | rho global | < 2022 | ≥ 2022 | ≥ 2024 | stable |
|---|---|---|---|---|---|---|
| endettement | 10 | +0,137 | −0,126 | +0,288 | +0,054 | non |
| valorisation | 15 | +0,072 | −0,045 | +0,246 | −0,013 | non |
| rentabilité | 15 | +0,064 | −0,017 | +0,255 | −0,130 | non |
| dividendes | 10 | +0,035 | −0,072 | +0,136 | −0,011 | non |

Aucun n'est stable, et les quatre bougent **ensemble** : négatifs avant 2022,
fortement positifs en 2022-2023, incertains depuis. Ce n'est pas quatre
mesures indépendantes qui se contredisent, c'est une seule période — 2022-2023
— où acheter la qualité a payé.

Le score fondamental complet a un rho **négatif** (−0,081) sur 2024-2025.
Avec quatre exercices d'historique, il n'y a pas de quoi conclure autrement
que : on ne sait pas encore.

### Ce qu'il faut donc faire des barèmes

**Rien.** Les poids en vigueur ne sont pas démontrés, mais aucune alternative
ne fait mieux hors échantillon. Les changer serait remplacer une convention
par une convention plus récente et moins éprouvée.

---

## Deux pièges rencontrés, et corrigés

### Le volume ne comptait pas

`backtest_technique.py` passait une colonne de volumes à zéro.
`compute_technical_score` exige `volume_sma20 > 0` : le sous-critère volume ne
marquait **jamais**, et le score mesuré valait 45 points, pas 50. Les volumes
existent pourtant dans `price_monthly` (97 % des mois).

Corrigé. Effet mesuré, à observations identiques :

| période | rho avec volume | rho sans volume |
|---|---|---|
| 1999-2025 | 0,1228 | 0,1184 |
| avant 2014 | 0,1690 | 0,1666 |
| 2014-2023 | 0,0834 | 0,0768 |
| 2024-2025 | 0,0350 | 0,0290 |

Le volume améliore le score sur **chaque** période. L'écart est faible, mais
c'est la seule contribution qui ne change jamais de signe.

Conséquence de lecture : le score médian passe de 26 à 29. Les tranches fixes
du tableau (`0-20`, `20-25`, …) ne découpent donc plus la même population
qu'avant la correction. La comparaison des tranches d'un tableau à l'autre
n'a pas de sens ; celle des rho, si.

### Le cinquième supérieur était choisi sur la réponse

`sorted(zip(scores, rendements))` départage les ex aequo **par le rendement**.
Un sous-critère ne prend que quatre à neuf valeurs distinctes : des centaines
de décisions partagent le même score, et le « cinquième supérieur » était donc
rempli avec les meilleurs rendements de la tranche à égalité — c'est-à-dire
avec la réponse qu'il prétendait mesurer.

Effet : un écart haut-bas annoncé à **+92,1 %**, retombé à **+9,2 %** une fois
les ex aequo départagés par tirage fixe. Le même défaut existait dans
`backtest_ponderation.py`, où il gonflait toute la colonne « écart ».

---

## Ce que ces mesures ne couvrent pas

- **Les fenêtres se chevauchent.** Chaque titre fournit une décision par mois
  et un rendement à 12 mois : deux décisions voisines partagent onze mois de
  rendement. Le `n` affiché surestime donc largement le nombre d'observations
  indépendantes — sur 2024-2025, 922 décisions valent peut-être quarante
  observations libres. Les colonnes « ≥ 2024 » sont à lire comme une
  indication, pas comme une preuve.
- **Le score est mesuré en mensuel.** `MONTHLY_PARAMS` n'est pas le paramétrage
  journalier affiché dans l'onglet Technique.
- **Les fondamentaux ne remontent qu'à 2020-2021.** Chiffre d'affaires et
  résultat net sont complets à partir de l'exercice 2021, les capitaux propres
  en 2025. Quatre ans utiles contre vingt-huit pour le technique.
- **Les frais et l'illiquidité ne sont pas déduits.** Un écart de quelques
  points sur un titre qui traite trois fois par mois n'est pas réalisable.
