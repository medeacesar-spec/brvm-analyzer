# Cahier des requêtes

Registre des demandes en attente. Une ligne par demande, dans l'ordre décidé
par le donneur d'ordre. Rien n'en sort sans être fait ou explicitement retiré.

Mis à jour le 2026-09-25.

## En cours

| # | Requête | Origine | État |
|---|---|---|---|
| 1 | ~~Collecte des avis de dividendes de la BRVM~~ | 2026-09-05 | **livré 2026-09-06** — 414 avis, 43 sociétés, exercices 2015-2025 |
| 2 | ~~Les deux sondes manquantes~~ | 2026-09-05 | **livré 2026-09-06** — `distribution` et `rendement` dans `coherence_interne.py` |
| 3 | ~~Correctifs des repères sectoriels~~ | 2026-09-05 | **livré 2026-09-06** — 5 ratios construits, seuil porté à 3 observations, portée nommée |
| 4 | ~~Routine quinzomadaire~~ | demandée de longue date | **livré 2026-09-06** — `routine_quinzaine.py` + atelier, billet GitHub en cas d'échec |
| 15 | **Neuf dividendes en contradiction avec l'avis BRVM**, après élimination des opérations sur titre. SOLIBRA 2020 et 2021 sont au dixième de l'avis ; Filtisac 2024 porte 145 contre 1 320. Restent aussi CFAO 2022, Onatel 2024, Vivo 2021, SITAB 2022, TotalEnergies Sénégal 2022, Filtisac 2023. | 2026-09-06 | à instruire |
| 26 | ~~Le Composite Total Return porte les variations du Composite~~ | 2026-09-09 | **retiré le 09/09 sur décision.** La question n'est plus instruite. Le tableau de bord continue d'afficher la valeur de l'indice et de taire ses deux variations — les montrer reviendrait à publier celles du Composite sous un autre nom |
| 23 | **Aucune valeur ne dit si elle est retraitée ou telle que payée.** Les deux conventions cohabitent dans `dps` sans être distinguées. | 2026-09-06 | à trancher |
| 16 | **ETI hors collecte** — la BRVM publie ses dividendes tantôt en dollars, tantôt en cents, tantôt étiquetés « FCFA » alors qu'ils sont en dollars. Deux lignes contradictoires pour le seul exercice 2025. | 2026-09-06 | à trancher |
| 20 | **Dividendes recopiés d'une année sur l'autre.** Bernabé n'a d'avis que pour l'exercice 2022 (150 FCFA), et la base porte 150 sur 2020, 2021, 2022, 2023 et 2024 ; ses comptes 2025 déclarent « Total dividendes bruts à distribuer : 0 ». La sonde `distribution` le voit (×27,6 en 2023, ×135,9 en 2024). Combien d'autres titres portent ainsi un dividende reconduit sans source ? | 2026-09-06 | à instruire |
| 19 | **BOABF.bf — coût du risque figé.** +4,303 Md en 2024 et −4,303 Md en 2025 : même valeur, signe opposé. Le stock de provisions du bilan (5 650 328 968 fin 2025) n'est pas le coût du risque, qui est un flux du compte de résultat. | 2026-09-06 | à relire |
| 17 | **BOAB.bj — la page parle de deux exercices.** La grille bancaire recule à 2023 (7 indicateurs sur 10) pendant que le tableau des pairs lit 2025 (4 sur 10). C'est une lacune de données, pas un défaut d'affichage : il manque dépôts, crédits, coût du risque, RBE et résultat avant impôt sur l'exercice 2025. | 2026-09-06 | à combler |
| 18 | **La sonde « vocabulaire » du diagnostic classe à tort.** Elle range en « vocabulaire » tout champ manquant dans un document qui contient du texte, sans vérifier que le poste y figure : 6 des 8 constats sont en réalité des bilans absents ou en image. | 2026-09-06 | à corriger |
| 24 | **Introduction d'une banque le 14 septembre 2026.** Nom à confirmer. À ajouter à `data/brvm_tickers.json` avec son secteur, puis lancer les deux collectes d'historique. La page Risque affichera « pas assez d'historique » pendant deux ans, comme pour BICI Bénin et la Loterie du Bénin. | 2026-09-07 | à faire le 14/09 |
| 25 | **La routine de quinzaine ne peut pas découvrir un nouveau titre.** `scan_brvm_reports.py` parcourt une table `TICKER_TO_BRVM_SLUG` écrite à la main : une société absente de cette table n'est jamais visitée. Une introduction reste donc invisible tant que personne ne l'ajoute — ce que la routine était pourtant censée éviter. | 2026-09-07 | à corriger |
| 5 | **Bilans en image** — NEI-CEDA, TotalEnergies Sénégal et d'autres publient un bilan qui n'existe qu'en image ; la lecture optique ne les traite pas encore. | 2026-09-06 | à instruire |

## Défauts de données connus

| # | Constat | État |
|---|---|---|
| 21 | SNTS.sn — capitaux propres de 224,3 à 1 160,7 Mds entre 2024 et 2025, relevé par la sonde `saut`. | à relire |
| 27 | Le **Composite Total Return** n'était affiché nulle part avant le 09/09 : collecté chaque jour et stocké dans `indices_cache`, il tombait d'une rangée de quatre colonnes qui en contenait cinq. C'est le seul indice qui compte les dividendes. | **corrigé** — il s'affiche depuis le 09/09. La question de ses variations est retirée (#26) |
| 22 | ORGT.tg 2026 — crédits négatifs (−188,75 Mds), relevé par la sonde `signe`. | à relire |
| 6 | Le total du bilan est faux ou absent chez quelques titres. LNBB affiche 14,8 Md d'actif pour 21,95 Md de capitaux propres — impossible. | à corriger |
| 7 | SHEC porte 18,4 Md de capitaux propres pour 605 M de chiffre d'affaires, sur deux exercices. | à vérifier |
| 8 | Le plafond de dette ne borne que par le haut : une dette trop petite n'est jamais détectée. | sonde à écrire |
| 9 | La sonde « figé » ne voit que les doublons exactement égaux, pas les quasi-doublons. | sonde à affiner |
| 10 | 34 constats « saut » et 14 constats « période » relevés par la cohérence interne, jamais instruits. | à instruire |
| 11 | `scripts/relire_signales.py` n'est pas versionné. | à livrer |
| 28 | ~~**SHEC.ci (Vivo Energy) 2025 — total de bilan recopié du comparatif.** La base porte 207 064 380 555 pour 2024 ET 2025. Le document donne « TOTAL ACTIF 193 561 278 972 207 064 380 555 TOTAL PASSIF 193 561 278 972 207 064 380 555 » ; la première colonne est 2025, comme le chiffre d'affaires vérifié à la main (#175). Relevé par le lecteur d'états financiers (#189).~~ | **corrigé 2026-09-24** — 193 561 278 972 |
| 29 | ~~**SIVC.ci (Erium) 2025 — total de bilan.** La base porte 18 525 800 000. Le document : actif net = brut − amortissements = total du passif = 14 611 080 094. Relevé par le lecteur (#189).~~ | **corrigé 2026-09-24** — 14 611 080 094 ; la base portait l'actif immobilisé BRUT (18 525 784 452) |
| 30 | ~~**NSBC.ci (NSIA Banque) 2025 — total de bilan.** La base porte 2 562 Md ; le document donne « Total de l'actif 2 510 429 3 073 062 » en millions, et aucune des deux colonnes ne retombe sur la base. NSIA écrit le comparatif d'abord : 3 073 Md serait 2025.~~ | **corrigé 2026-09-24** — 3 073 062 M. Reste 2024 : la base porte 2 230 Md, le comparatif retraité 2 510 Md — à revoir avec le document 2024 |
| 31 | ~~**BICB.bj (BICI Bénin) 2025 — deux tableaux, deux unités.** « Total des capitaux propres 215 986 878 257 135 118 114 620 85 176 » mêle francs et millions ; « Total Passif et Capitaux propres 1 925 468 637 911 1 844 600 … ». La base (135 118 M, 1 844 600 M) suit le tableau en millions. À quel exercice ou périmètre correspond la colonne en francs ?~~ | **la base est juste** — la colonne en francs est une colonne de travail dont les totaux ne se recoupent pas ; les colonnes certifiées en millions et les chiffres clés donnent 1 844,6 et 135,1 |
| 32 | **Bilans à lire à la main**, relevés par `recouper_par_lecteur.py` : le document, lu en mode sûr, contredit la base sur les capitaux propres ou le total de bilan, que la fiche société ne publie pas. CIE 2024 (2 019 contre 1 975 Md) · NSIA 2024 (2 514 contre 2 230, comparatif retraité) · Onatel 2024 (capitaux propres 62,6 contre 61,9 ; total 301,5 contre 288,0) · PRSC 2023 (51,2 contre 20,5) · SGBCI 2023 (capitaux propres 404,0 contre 345,0) · Vivo 2023 (169,8 contre 168,5) · **SIB 2024 (1 685 contre 1 606, la base recopie 2023)** · SIB 2025 (capitaux propres 204,8 contre 205,0) · Sonatel 2023 et 2025 (total 2 574 et 3 270 contre 2 399 et 3 149) · Sonatel 2024 (social 1 382 contre consolidé 3 105 : définition) · TotalEnergies Sénégal 2024 (201,3 contre 116,8) · Unilever CI 2021 et 2023. | à lire |
| 33 | **Sonatel 2025 — capitaux propres réécrits depuis le 06/09.** Le 6 septembre, les capitaux propres consolidés ont été portés à 1 274,6 Md (2024) et 1 399,3 Md (2025) (voir « Fait »). Aujourd'hui, 2024 porte toujours 1 274,6, mais **2025 porte 1 001,9 Md** (ligne modifiée le 17/09) ; le document 2025, lu en mode sûr, donne 1 399,263. La ligne 2026 (1 398,9 Md de capitaux propres, 3 607 Md de total, 14/09) est probablement un bilan semestriel au 30/06/2026 — à confirmer, un exercice non clos n'ayant pas sa place dans `fundamentals`. Quel traitement a réécrit 2025 le 17/09 ? | à instruire |
| 34 | **SITAB 2024 — résultat net.** Le document écrit 44 730 358 142 sur trois lignes, le résultat 2023 retombant exactement sur la base ; la fiche et la base disent 44,174 Md. La base ayant été remplie depuis la fiche, c'est un document contre une fiche. | à lire |
| 35 | **Exercices recopiés, second document scanné.** SODECI 2022 (CA : base 37,1 Md, fiche 160,7), BOA Mali 2025 (CA : base 36,159 recopié, fiche 37,997), BOA Niger 2023 (résultat : base 10,134 recopié, fiche 10,077). Les documents sont des scans : la passe OCR du recoupement fournira la seconde source. | en cours |
| 36 | **SMB 2024 — total de bilan absent en base** : 163,113 Md lu en mode sûr. À proposer après lecture. | à proposer |
| 37 | **La sonde « figé » ne voit que les égalités exactes** (voir #9) : SIB 2024, SGBCI 2025, Onatel 2024 portaient des montants recopiés de l'année précédente à l'arrondi près. Une comparaison à 10⁻⁴ en trouve douze, dont Orange CI 2022, qui est une vraie quasi-égalité (17 M d'écart, confirmée par la fiche). | sonde à affiner |
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
| Historique de cours — `price_monthly` (5 ans) et `price_quarterly` (10 ans), 49 séries chacune, indices compris | 2026-09-07 |
| Colonne `dps_paiement` — date de versement du dividende, 303 couples remplis depuis les avis BRVM | 2026-09-07 |
| 41 dividendes reconduits sans avis marqués comme non sourcés — **conservés sur décision** | 2026-09-07 |
| Onglet Risque et carte « ce que le score ne dit pas » dans Recommandation | 2026-09-07 |
| Sonatel — capitaux propres consolidés corrigés (1 274,6 et 1 399,3 Md), actif et dette renseignés | 2026-09-06 |
| 41 dividendes reconduits sans avis marqués comme non sourcés (conservés sur décision) | 2026-09-06 |
| 26 des 35 contradictions de dividende expliquées par une opération sur titre | 2026-09-06 |
| Collecte des avis de dividendes — 168 valeurs écrites, exercice 2025 de 23 à 35 titres sur 44 | 2026-09-06 |
