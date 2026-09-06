# Cahier des requêtes

Registre des demandes en attente. Une ligne par demande, dans l'ordre décidé
par le donneur d'ordre. Rien n'en sort sans être fait ou explicitement retiré.

Mis à jour le 2026-09-06.

## En cours

| # | Requête | Origine | État |
|---|---|---|---|
| 1 | **Collecte des avis de dividendes de la BRVM** — une source courte et non ambiguë pour tout le marché. L'avis n°211 (LNBB) a prouvé la qualité : « 164,1709 FCFA net par action, au titre de l'exercice 2025 ». | 2026-09-05 | prochain chantier |
| 2 | **Les deux sondes manquantes** — une distribution n'excède pas durablement le résultat net ; un rendement au-dessus de 25 % n'existe pas sur cette place. | 2026-09-05 | à faire |
| 3 | **Correctifs des repères sectoriels** — libellé « Secteur » quand le repère est le marché entier ; médiane calculée sur deux sociétés ; cinq ratios annoncés jamais calculés (dette/CP, taux de distribution, marge de trésorerie libre, couverture des intérêts, rendement de l'actif). Correctifs écrits, non livrés. | 2026-09-05 | correctifs prêts |
| 4 | **Routine quinzomadaire** — lire toutes les deux semaines les états financiers nouvellement publiés et les avis de la BRVM, et signaler ce qui entre en base. | demandée de longue date | jamais construite |
| 5 | **Bilans en image** — NEI-CEDA, TotalEnergies Sénégal et d'autres publient un bilan qui n'existe qu'en image ; la lecture optique ne les traite pas encore. | 2026-09-06 | à instruire |

## Défauts de données connus

| # | Constat | État |
|---|---|---|
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
