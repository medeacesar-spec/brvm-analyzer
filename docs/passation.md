# BRVM Analyzer — passation

**Dépôt** `medeacesar-spec/brvm-analyzer` · `/Users/mdegbe/brvm-analyzer` · Streamlit + Supabase
**État au 24 septembre 2026.** Aucune PR ouverte : tout est fusionné dans `main`.

---

## Ce qu'il faut savoir avant de toucher quoi que ce soit

**L'application déployée peut être en retard sur `main`.** `brvm-analyzer.streamlit.app`
ne recharge pas le code toute seule : après une fusion, il faut la redémarrer
(*Manage app* → *Reboot*). Deux fois ce mois-ci, un correctif présent dans `main`
n'était pas visible côté utilisateur. Le rendu local ne prouve donc rien de ce
que voit le donneur d'ordre.

**La base est commune au local et à l'application publique.** Les comptes de
test `local` et `dev@local` y ont de vrais portefeuilles. Un visiteur non
connecté reçoit `anonyme`, jamais `local` (corrigé le 16/09).

**Le workflow git** : une branche par sujet, partie de `origin/main`, PR via
l'API GitHub, jamais de push direct. Ne jamais empiler deux branches ; si une
PR passe en conflit, la remettre à jour avec `main` plutôt que de la refaire.

---

## L'état de la donnée

| donnée | couverture | source |
|---|---|---|
| cours journaliers | 218 898 séances depuis 1998 | import RichBourse + collecte quotidienne |
| indices | 17 séries, dont 7 sectoriels historiques | registre `analysis/indices.py` |
| chiffre d'affaires, résultat net | **236 / 240** couples 2021-2025 | fiches société + documents officiels |
| dividende par action | 89 % | 414 avis de paiement BRVM |
| dividende déclaré | 218 exercices | calculé : DPS × actions |
| capitaux propres | 62 % | états financiers (PDF) |
| total de bilan | 56 % | idem |
| dette totale | 40 % | idem |
| charges d'intérêts, flux, investissements | 13-14 % | idem |

Le compte de résultat est presque complet ; **le bilan et les flux ne le sont
pas**, et c'est le chantier prioritaire (voir `docs/chantiers.md`).

---

## Ce qui a été fait du 10 au 24 septembre

**Données.** Dix-sept exercices 2025 lus dans les documents officiels et
vérifiés un à un ; 49 trimestres qui se faisaient passer pour des exercices,
corrigés par recoupement avec les fiches société ; neuf alertes instruites, dont
quatre bénéfices écrits à la place de pertes ; les dividendes Sonatel passés du
net au brut ; Bridge Bank intégrée le jour de son introduction.

**Corrections d'affichage.** Le YTD des indices, faux parce que brvm.org publie
une colonne figée depuis janvier — le Composite affichait +1,70 % pour +53,4 %
réels. La répartition suggérée du cash, qui ignorait la fenêtre de risque. Les
messages d'un titre nouvellement coté, qui le traitaient comme un titre éteint.

**Performance.** Démarrage à froid de 66 s à 14 s : l'application lisait
218 000 séances à chaque ouverture et interrogeait brvm.org avant le premier
affichage.

**Outillage.** Un lecteur d'états financiers SYSCOHADA (`analysis/lecture_syscohada.py`)
et son étalon (`scripts/etalon_lecture_syscohada.py`), mesuré contre quatorze
documents lus à la main : **17 valeurs justes sur 36**, contre 10 au départ.

---

## Les décisions en attente

1. **Les 52 écarts modérés** entre la base et les fiches société. Ils relèvent
   le plus souvent d'une différence de définition — comptes sociaux contre
   consolidés, produit net bancaire contre produits totaux. Aucune règle
   automatique ne les tranche.
2. **Les données de test en production** : le compte `local` (8 lignes, un
   profil investisseur) et `dev@local` (16 lignes). Faut-il les supprimer ?
3. **Deux séries d'indices manquantes** : Composite Total Return et Services
   financiers. Sans historique, leur YTD est effacé plutôt que faux. Un export
   RichBourse les réglerait.
4. **Deux corrections faites sur une source unique** — Safca 2022 et Erium 2021,
   sans document publié. À revérifier au prochain dépôt.

---

## Les pièges rencontrés, et ce qu'ils ont appris

**Une valeur fausse mais vraisemblable est pire qu'une valeur absente.** Le
lecteur automatique rendait, pour NSIA, le produit net bancaire de 2024 à la
place de celui de 2025 : plausible, et donc invisible. C'est la raison pour
laquelle il n'écrit rien tant que l'étalon ne s'est pas inversé.

**Recopier une source extérieure se paie.** Deux fois : les rendements
collectés sur le web (faux pour la plupart des titres) et le YTD de brvm.org
(figé depuis janvier). Chaque fois que l'historique est en base, mieux vaut
calculer que recopier.

**Un chiffre improbable n'est pas toujours une erreur.** Sur sept PER aberrants
instruits, trois étaient exacts : Erium, AGL et BOA Niger gagnent réellement
très peu. Les corriger sans lire le document aurait remplacé un faux chiffre
par un autre.

**Une ligne « exercice » peut porter un trimestre.** Un document annuel
référencé ne prouve rien sur les montants de la ligne. Le recoupement se fait
avec `quarterly_data`, en excluant les périodes `S2` et `T4` — chez BOA Burkina,
la ligne S2 porte le montant de l'exercice entier.

---

## Où reprendre

`docs/chantiers.md` fixe l'ordre : les états financiers sur cinq ans d'abord,
puis la robustesse du fil d'actualité, l'alerte « sans compte annuel », et le
projet de recherche sur les groupes de titres qui battent la BRVM.

Pour le premier, la suite immédiate est identifiée : dans le lecteur, faire
gagner « TOTAL CAPITAUX PROPRES » sur « Sous-Total part du groupe », puis lire
les montants rangés dans des tableaux plutôt que dans le texte — quatre
documents de l'étalon en dépendent.
