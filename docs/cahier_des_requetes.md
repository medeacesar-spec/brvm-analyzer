# Cahier des requêtes

Registre des demandes en attente. Une ligne par demande, dans l'ordre décidé
par le donneur d'ordre. Rien n'en sort sans être fait ou explicitement retiré.

Mis à jour le 2026-09-06.

## En cours

| # | Requête | Origine | État |
|---|---|---|---|
| 1 | ~~Collecte des avis de dividendes de la BRVM~~ | 2026-09-05 | **livré 2026-09-06** — 414 avis, 43 sociétés, exercices 2015-2025 |
| 2 | ~~Les deux sondes manquantes~~ | 2026-09-05 | **livré 2026-09-06** — `distribution` et `rendement` dans `coherence_interne.py` |
| 3 | ~~Correctifs des repères sectoriels~~ | 2026-09-05 | **livré 2026-09-06** — 5 ratios construits, seuil porté à 3 observations, portée nommée |
| 4 | ~~Routine quinzomadaire~~ | demandée de longue date | **livré 2026-09-06** — `routine_quinzaine.py` + atelier, billet GitHub en cas d'échec |
| 15 | **Trente-cinq dividendes en contradiction avec l'avis BRVM** — ce ne sont pas des retenues à la source. Les Bank of Africa et la SIB tombent sur des rapports de 0,50 · 0,6667 · 0,625 (acompte enregistré seul ?), SOLIBRA 2020 et 2021 sont au dixième de l'avis, Filtisac 2024 porte 145 contre 1 320. | 2026-09-06 | à instruire |
| 16 | **ETI hors collecte** — la BRVM publie ses dividendes tantôt en dollars, tantôt en cents, tantôt étiquetés « FCFA » alors qu'ils sont en dollars. Deux lignes contradictoires pour le seul exercice 2025. | 2026-09-06 | à trancher |
| 20 | **Dividendes recopiés d'une année sur l'autre.** Bernabé n'a d'avis que pour l'exercice 2022 (150 FCFA), et la base porte 150 sur 2020, 2021, 2022, 2023 et 2024 ; ses comptes 2025 déclarent « Total dividendes bruts à distribuer : 0 ». La sonde `distribution` le voit (×27,6 en 2023, ×135,9 en 2024). Combien d'autres titres portent ainsi un dividende reconduit sans source ? | 2026-09-06 | à instruire |
| 19 | **BOABF.bf — coût du risque figé.** +4,303 Md en 2024 et −4,303 Md en 2025 : même valeur, signe opposé. Le stock de provisions du bilan (5 650 328 968 fin 2025) n'est pas le coût du risque, qui est un flux du compte de résultat. | 2026-09-06 | à relire |
| 17 | **BOAB.bj — la page parle de deux exercices.** La grille bancaire recule à 2023 (7 indicateurs sur 10) pendant que le tableau des pairs lit 2025 (4 sur 10). C'est une lacune de données, pas un défaut d'affichage : il manque dépôts, crédits, coût du risque, RBE et résultat avant impôt sur l'exercice 2025. | 2026-09-06 | à combler |
| 18 | **La sonde « vocabulaire » du diagnostic classe à tort.** Elle range en « vocabulaire » tout champ manquant dans un document qui contient du texte, sans vérifier que le poste y figure : 6 des 8 constats sont en réalité des bilans absents ou en image. | 2026-09-06 | à corriger |
| 5 | **Bilans en image** — NEI-CEDA, TotalEnergies Sénégal et d'autres publient un bilan qui n'existe qu'en image ; la lecture optique ne les traite pas encore. | 2026-09-06 | à instruire |

## Défauts de données connus

| # | Constat | État |
|---|---|---|
| 21 | SNTS.sn — capitaux propres de 224,3 à 1 160,7 Mds entre 2024 et 2025, relevé par la sonde `saut`. | à relire |
| 22 | ORGT.tg 2026 — crédits négatifs (−188,75 Mds), relevé par la sonde `signe`. | à relire |
| 6 | Le total du bilan est faux ou absent chez quelques titres. LNBB affiche 14,8 Md d'actif pour 21,95 Md de capitaux propres — impossible. | à corriger |
| 7 | SHEC porte 18,4 Md de capitaux propres pour 605 M de chiffre d'affaires, sur deux exercices. | à vérifier |
| 8 | Le plafond de dette ne borne que par le haut : une dette trop petite n'est jamais détectée. | sonde à écrire |
| 9 | La sonde « figé » ne voit que les doublons exactement égaux, pas les quasi-doublons. | sonde à affiner |
| 10 | 34 constats « saut » et 14 constats « période » relevés par la cohérence interne, jamais instruits. | à instruire |
| 11 | `scripts/relire_signales.py` n'est pas versionné. | à livrer |
| 14 | Bernabé : la dette lue vaut 208 527 219 d'emprunts **plus 16 688 923 003**, qui est la « Trésorerie nette » du tableau de flux et non la trésorerie-passif du bilan (18 535 854 210 en 2025). Suspecte avant comme après la #89. | à corriger |

## Décisions à confirmer

| # | Question | Position actuelle |
|---|---|---|
| 12 | Dette des banques : les dépôts sont leur matière première, pas leur endettement. Faut-il publier un ratio d'endettement bancaire, et sur quelle assiette ? | non calculé — 14 banques sans ratio |
| 13 | SAFCA ne porte aucune dette financière ; CFAO et SEMC n'en publient pas. | acté |

## Fait

| Requête | Livré |
|---|---|
| Retrait de SVOC de la cote, diffusé partout | 2026-09-05 |
| Suppression des quatre pages (Suivi des données, Panorama, Calendrier, Assistant IA) | 2026-09-05 |
| Campagne capitaux propres — 47 titres sur 48 | 2026-09-05 |
| Campagne dette — tous les titres non bancaires sauf SAFCA (nulle) et SEMC (non publiée) | 2026-09-06 |
| Routine de quinzaine — recense, traite, collecte les avis, contrôle, et ouvre un billet en cas d'échec | 2026-09-06 |
| Sonatel — capitaux propres consolidés corrigés (1 274,6 et 1 399,3 Md), actif et dette renseignés | 2026-09-06 |
| Collecte des avis de dividendes — 168 valeurs écrites, exercice 2025 de 23 à 35 titres sur 44 | 2026-09-06 |
