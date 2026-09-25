# Sources hors brvm.org

brvm.org reste la source de reference : c'est la que les emetteurs deposent leurs etats. Mais il arrive que le depot soit absent, incomplet, ou un scan que l'OCR ne sait pas lire. Il faut alors chercher ailleurs, et **noter ce qu'on a trouve** pour ne pas refaire la recherche.

## Quand chercher ailleurs

- **Pas de document annuel** pour un exercice clos : BOA Niger 2024 et 2025 (seulement le rapport du CA et celui des CAC).
- **Scan illisible** : les chiffres sortent faux ou incomplets, et les identites comptables (PNB − charges = RBE, EBE − dotations = resultat d'exploitation) ne tombent pas.
- **Grille sectorielle incomplete** : le document de brvm.org donne le PNB et le resultat, pas les charges ni le RBE, ou les donne en image. Le site de l'emetteur publie souvent les memes etats en texte natif (Oragroup 2025 sur orabank.net).
- **Introduction recente** : Bridge Bank Group CI, cotee le 24/09/2026, sans etats financiers sur brvm.org.

## Ou chercher, dans l'ordre

1. **Les autres documents de brvm.org** du meme exercice : rapport du conseil d'administration a l'AGO, rapport de gestion, rapport d'activites du 2e semestre, autre version des etats (IFRS et SYSCOHADA).
2. **Le site de l'emetteur**, page « communication financiere » ou « investisseurs » : comptes resumes, rapports annuels complets, souvent en texte natif.
3. **La note d'information** d'une introduction en bourse ou d'un emprunt obligataire : cinq exercices de comptes.
4. Les sites d'information financiere (richbourse, sikafinance) : seulement pour **retrouver** un document, jamais comme source des chiffres.

## Comment le noter

- Le document est telecharge dans `data/pdf_fondamentaux/`, nomme `TICKER_EXERCICE_DATE_-_intitule.pdf` comme les autres.
- Il est enregistre dans `report_links` avec sa **vraie source** (colonne `source` : `boaniger.com`, `bridgebankgroup.com`...), pour que les lecteurs et la fiche le retrouvent.
- Le titre est ajoute a `data/sources_emetteurs.json` : l'adresse de sa page de communication financiere, ce qu'on y trouve, **pourquoi** on y va, et la date de verification. Un scan brvm.org illisible et le document a lui preferer y figurent aussi.

## Ce qui ne change pas

Un chiffre ecrit en base reste recoupe : identites comptables, comparatif de l'exercice suivant, ou deux documents. Une source de meilleure qualite ne dispense pas du recoupement.
