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
| 32 | **Bilans à lire à la main**, relevés par `recouper_par_lecteur.py` : le document, lu en mode sûr, contredit la base sur les capitaux propres ou le total de bilan, que la fiche société ne publie pas. **Cinq sont aussi des copies de l'exercice précédent relevées par la sonde « figé »** — la base y a tort à coup sûr, seule la valeur du document reste à confirmer : CIE 2024 (total 2 019 contre 1 975 Md recopié), SGBCI 2023 (capitaux propres 404,0 contre 345,0 recopié), Onatel 2024 (total 301,5 contre 288,0 recopié), SIB 2024 (total 1 685 contre 1 606 recopié), Ecobank CI 2024 (capitaux propres recopiés). Les autres : NSIA 2024 (2 514 contre 2 230, comparatif retraité) · Onatel 2024 (capitaux propres 62,6 contre 61,9) · PRSC 2023 (51,2 contre 20,5) · Vivo 2023 (169,8 contre 168,5) · SIB 2025 (capitaux propres 204,8 contre 205,0) · Sonatel 2023 et 2025 (total 2 574 et 3 270 contre 2 399 et 3 149) · Sonatel 2024 (social 1 382 contre consolidé 3 105 : définition) · TotalEnergies Sénégal 2024 (201,3 contre 116,8) · Unilever CI 2021 et 2023. | à lire |
| 33 | **Sonatel 2025 — capitaux propres réécrits depuis le 06/09.** Le 6 septembre, les capitaux propres consolidés ont été portés à 1 274,6 Md (2024) et 1 399,3 Md (2025) (voir « Fait »). Aujourd'hui, 2024 porte toujours 1 274,6, mais **2025 porte 1 001,9 Md** (ligne modifiée le 17/09) ; le document 2025, lu en mode sûr, donne 1 399,263. La ligne 2026 (1 398,9 Md de capitaux propres, 3 607 Md de total, 14/09) est probablement un bilan semestriel au 30/06/2026 — à confirmer, un exercice non clos n'ayant pas sa place dans `fundamentals`. Quel traitement a réécrit 2025 le 17/09 ? | à instruire |
| 34 | **SITAB 2024 — résultat net.** Le document écrit 44 730 358 142 sur trois lignes, le résultat 2023 retombant exactement sur la base ; la fiche et la base disent 44,174 Md. La base ayant été remplie depuis la fiche, c'est un document contre une fiche. | à lire |
| 35 | ~~**Exercices recopiés, second document scanné.** SODECI 2022, BOA Mali 2025, BOA Niger 2023.~~ | **BOA Mali 2025 et BOA Niger 2023 confirmés par la passe OCR du 25/09** (#196). SODECI 2022 : le scan 2023 ne donne pas de lecture sûre — reste à lire. |
| 36 | **Trous que le lecteur comble en mode sûr** : SMB 2024 total de bilan 163,1 Md ; BICI CI 2024 capitaux propres 99,8 Md et total 1 015,6 Md ; PRSC 2025 total 47,8 Md ; SEMC 2021 total 22,4 Md ; Unilever CI 2024 total 34,2 Md. À proposer après lecture — la fiche ne publie pas ces champs. | **SMB 2024 et Unilever CI 2024 comblés le 25/09** par l'ancre inversée à deux sources (#203) ; le reste à proposer |
| 38 | **Lus dans les scans, à trancher** (passe OCR du 25/09) : Ecobank CI 2024 capitaux propres 199,4 Md contre 178,2 **recopié de 2023** · Oragroup 2024 capitaux propres : le scan donne 54,8 Md (part du groupe), la base 110,5, le comparatif du document 2025 96,7 — trois valeurs · Servair Abidjan 2025 résultat : document 0,351, fiche 1,351, base 1,331 · SEMC 2022 résultat : document 1 M, fiche et base 3,86 Md · SETAO 2025 résultat : document +0,497, fiche et base −0,097. | à lire |
| 39 | **Encours bancaires à lire — écarts modérés (5 à 15 %)** relevés par le chaînage des comparatifs (#204) : deux documents consécutifs donnent la même valeur, la base une autre, peut-être par périmètre (brut ou net, social ou consolidé). NSIA 2021-2024 (crédits et dépôts, base 8 à 12 % en dessous) · Oragroup 2021-2024 (crédits) · BOA Bénin 2023-2024 (dépôts) · BOA CI 2023 · Bicici 2023 (dépôts) · Ecobank CI 2024 · SIB 2022. **Deux sont des copies à coup sûr** : SIB 2024 crédits (1 037,0 recopié de 2023 ; documents 1 101,2) et SGBCI 2023 crédits (base 1 965,5 ; documents 2 394,9). SGBCI 2025 : un seul document (crédits 2 546,3, dépôts 2 907,7 contre 2 492,0 et 2 692,9 en base). | à lire |
| 40 | **Résultats d'exploitation et frais financiers à lire** — le chaînage (#205) contredit la base : CFAO 2023 EBIT 11,109 contre **41,109** (faute de frappe probable) et 2022 9,881 contre 1,068 · CIE 2024 frais financiers 3,291 contre 18,087 · CIE 2023 EBIT 16,460 contre 11,981 · Bolloré 2022 EBIT (document 0,723, base 21,858, supérieure à l'EBE de 7,3) · Sonatel 2024 EBIT 619,5 contre 155,0 (base trimestrielle ?) · TotalEnergies CI 2024 EBIT 10,491 contre 6,785 · Palm CI 2024 EBIT 19,394 contre 28,502 · Bernabé 2024 EBIT 1,193 contre 0,400 · NEI-CEDA 2023 EBIT 0,850 contre 1,850 · Filtisac 2021 EBIT (signe). | à lire |
| 41 | **Valeurs à une seule source**, jamais écrites : 87 postes bancaires lus par le lecteur bancaire (#202) dans un seul document aux soldes enchaînés (BOA Bénin 2022, BOA CI 2022 et 2024, BOA Sénégal 2021 et 2024, NSIA 2025, SGBCI 2025, SIB 2021 et 2024…) ; 52 valeurs lues par l'ancre inversée (#203). Coût du risque d'Oragroup 2022 (59,8 Md) : signe inconnu. | à lire |
| 42 | **Coût du risque renseigné pour des non-banques** : CIE, SAFCA, SODECI, Bolloré, Tractafric, Unilever CI. Sans effet sur l'écran (le ratio n'est calculé que pour les banques), mais la complétude le compte. | à nettoyer |
| 43 | **Signe du coût du risque** : depuis le 25/09, le lecteur bancaire écrit une charge en négatif et une reprise nette en positif, déduits de l'enchaînement RBE → résultat d'exploitation. Les valeurs plus anciennes mêlent les deux conventions (Ecobank 2022-2024 positives, SGBCI négatives). Le ratio utilise la valeur absolue. | acté |
| 44 | **Documents inutilisables** récupérés sur brvm.org : Servair Abidjan 2021 (PDF corrompu), Sicable 2021 (31 Ko, sans texte). | à rechercher |
| 45 | **Parc d'Orange CI** : le « Parc FMI » publié couvre le groupe (Côte d'Ivoire, Burkina Faso, Libéria), pas la seule Côte d'Ivoire. À signaler à l'écran à côté de Sonatel (groupe, cinq pays) et d'Onatel (un pays). | à décider |
| 37 | **Les quatorze montants « figés » de la sonde n'avaient jamais été instruits** (voir #10). La sonde les voyait bien — SIB 2024, SGBCI 2022, 2023 et 2025, Onatel 2024, TotalEnergies CI 2025, CIE 2024, Ecobank CI 2024 — et le recoupement par le lecteur en confirme neuf (#196). Restent : les **quasi-copies**, qui lui échappent (BOA Mali 2025 : 36,157 → 36,159 Md ; BOA Niger 2023), et les copies de bilan de #32. | sonde à affiner |
| 14 | Bernabé : la dette lue vaut 208 527 219 d'emprunts **plus 16 688 923 003**, qui est la « Trésorerie nette » du tableau de flux et non la trésorerie-passif du bilan (18 535 854 210 en 2025). Suspecte avant comme après la #89. | à corriger |

## Décisions à confirmer

| # | Question | Position actuelle |
|---|---|---|
| 12 | Dette des banques : les dépôts sont leur matière première, pas leur endettement. Faut-il publier un ratio d'endettement bancaire, et sur quelle assiette ? | non calculé — 14 banques sans ratio |
| 13 | SAFCA ne porte aucune dette financière ; CFAO et SEMC n'en publient pas. | acté |

## Fait

| Requête | Livré |
|---|---|
| Lecteur bancaire — compte de résultat lu en bloc, soldes enchaînés, deux documents : 40 trous comblés, 14 écarts corrigés (exercice précédent recopié) (#202) | 2026-09-25 |
| Ancre inversée en cascade — 24 trous comblés à deux sources (#203) | 2026-09-25 |
| Encours bancaires par chaînage des comparatifs — 16 trous, 5 écarts grossiers corrigés (SGBCI : dix fois l'exercice précédent) (#204) | 2026-09-25 |
| Chaînage des comparatifs non bancaires — EBITDA, EBIT, frais financiers, investissements : 18 trous (#205) | 2026-09-25 |
| Collecte brvm.org paginée — 667 liens, 17 exercices sans document retrouvés (#199) | 2026-09-25 |
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
