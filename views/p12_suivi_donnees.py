"""
Suivi de l'acquisition des donnees.

Trois questions, trois blocs : ou en est la cote, ou en est chaque societe,
et quels documents restent a lire. Une quatrieme section suit a part les
quinze banques, dont la grille exige cinq postes que les autres secteurs
n'ont pas.
"""

import streamlit as st

from utils.ui_helpers import section_heading


def _barre(part: float, largeur: int = 120) -> str:
    """Petite jauge horizontale, lisible sans couleur."""
    couleur = ("var(--up)" if part >= 0.8
               else "var(--ocre)" if part >= 0.5 else "var(--down)")
    return (
        f"<div style='display:inline-block;width:{largeur}px;height:6px;"
        f"background:var(--border);border-radius:3px;overflow:hidden;"
        f"vertical-align:middle;'>"
        f"<div style='width:{part*100:.0f}%;height:100%;background:{couleur};'>"
        f"</div></div>"
    )


def _bloc_chiffre(libelle: str, valeur: str, precision: str = "") -> str:
    return (
        f"<div style='flex:1;min-width:130px;border:1px solid var(--border);"
        f"border-radius:10px;padding:12px 14px;background:var(--bg-elev);'>"
        f"<div style='font-size:11px;color:var(--ink-3);text-transform:uppercase;"
        f"letter-spacing:.04em;'>{libelle}</div>"
        f"<div style='font-size:22px;font-weight:600;margin-top:2px;'>{valeur}</div>"
        f"<div style='font-size:11.5px;color:var(--ink-3);'>{precision}</div>"
        f"</div>"
    )


def render():
    from analysis.completude import etat_cote, reste_a_lire

    st.markdown("## Suivi des données")
    st.caption(
        "Une donnée n'est comptée comme acquise que si elle est **utilisable**. "
        "Un total de bilan présent mais incohérent — Bank of Africa Bénin porte "
        "21,8 M FCFA d'actif pour 112,8 Mds de fonds propres — est compté à part, "
        "en « à corriger » : il ne manque pas, il est faux, et ce n'est pas le "
        "même travail."
    )

    with st.spinner("Inventaire en cours…"):
        lignes = etat_cote()

    if not lignes:
        st.info("Aucune société à inventorier.")
        return

    total_champs = sum(e["total"] for e in lignes)
    total_acquis = sum(len(e["acquis"]) for e in lignes)
    total_douteux = sum(len(e["douteux"]) for e in lignes)
    completes = [e for e in lignes if not e["manquants"] and not e["douteux"]]

    st.markdown(
        "<div style='display:flex;gap:10px;flex-wrap:wrap;margin:10px 0 4px;'>"
        + _bloc_chiffre("Sociétés", f"{len(lignes)}", "cotées et suivies")
        + _bloc_chiffre("Données acquises",
                        f"{total_acquis*100//total_champs} %",
                        f"{total_acquis} sur {total_champs} attendues")
        + _bloc_chiffre("Dossiers complets", f"{len(completes)}",
                        f"sur {len(lignes)} sociétés")
        + _bloc_chiffre("À corriger", f"{total_douteux}",
                        "valeurs présentes mais fausses")
        + "</div>",
        unsafe_allow_html=True,
    )

    # ── Par secteur ──
    section_heading("Avancement par secteur", spacing="loose")
    par_secteur = {}
    for e in lignes:
        s = par_secteur.setdefault(e["secteur"], {"n": 0, "acquis": 0, "total": 0})
        s["n"] += 1
        s["acquis"] += len(e["acquis"])
        s["total"] += e["total"]

    rangs = ""
    for secteur, s in sorted(par_secteur.items(),
                             key=lambda x: -(x[1]["acquis"] / max(x[1]["total"], 1))):
        part = s["acquis"] / s["total"] if s["total"] else 0
        rangs += (
            f"<tr style='border-top:1px solid var(--border);'>"
            f"<td style='padding:7px 12px 7px 0;font-size:12.5px;'>{secteur}</td>"
            f"<td style='padding:7px 12px 7px 0;font-size:12px;color:var(--ink-3);"
            f"text-align:right;'>{s['n']}</td>"
            f"<td style='padding:7px 12px 7px 0;'>{_barre(part)}</td>"
            f"<td style='padding:7px 0;font-size:12.5px;font-weight:600;"
            f"text-align:right;font-variant-numeric:tabular-nums;'>"
            f"{part*100:.0f} %</td></tr>"
        )
    st.markdown(
        f"<table style='width:100%;border-collapse:collapse;'>"
        f"<thead><tr>"
        f"<th style='text-align:left;font-size:11px;font-weight:500;"
        f"color:var(--ink-3);padding-bottom:4px;'>Secteur</th>"
        f"<th style='text-align:right;font-size:11px;font-weight:500;"
        f"color:var(--ink-3);'>Sociétés</th><th></th>"
        f"<th style='text-align:right;font-size:11px;font-weight:500;"
        f"color:var(--ink-3);'>Acquis</th>"
        f"</tr></thead><tbody>{rangs}</tbody></table>",
        unsafe_allow_html=True)

    # ── Societe par societe ──
    section_heading("Société par société", spacing="loose")
    st.caption("Les moins renseignées en tête : c'est là qu'il reste du travail.")

    filtre = st.selectbox("Secteur", ["Tous"] + sorted(par_secteur.keys()),
                          key="suivi_filtre_secteur")
    visibles = [e for e in lignes if filtre == "Tous" or e["secteur"] == filtre]

    corps = ""
    for e in visibles:
        manque = ", ".join(l for l, _ in e["manquants"]) or "—"
        douteux = ", ".join(l for l, _ in e["douteux"])
        detail = (f"<div style='font-size:11px;color:var(--ink-3);'>manque : "
                  f"{manque}</div>")
        if douteux:
            detail += (f"<div style='font-size:11px;color:var(--down);'>"
                       f"à corriger : {douteux}</div>")
        corps += (
            f"<tr style='border-top:1px solid var(--border);'>"
            f"<td style='padding:7px 12px 7px 0;font-size:12.5px;"
            f"white-space:nowrap;'><b>{e['ticker']}</b>"
            f"<div style='font-size:11px;color:var(--ink-3);'>"
            f"{str(e['nom'])[:28]}</div></td>"
            f"<td style='padding:7px 12px 7px 0;font-size:11.5px;"
            f"color:var(--ink-3);'>{e['secteur']}</td>"
            f"<td style='padding:7px 12px 7px 0;font-size:12px;"
            f"text-align:center;'>{e['exercice'] or '—'}</td>"
            f"<td style='padding:7px 12px 7px 0;'>{_barre(e['part'], 90)}</td>"
            f"<td style='padding:7px 12px 7px 0;font-size:12.5px;"
            f"text-align:right;font-variant-numeric:tabular-nums;'>"
            f"{len(e['acquis'])}/{e['total']}</td>"
            f"<td style='padding:7px 0;'>{detail}</td></tr>"
        )
    st.markdown(
        f"<div style='overflow-x:auto;'>"
        f"<table style='width:100%;border-collapse:collapse;'>"
        f"<thead><tr>"
        + "".join(f"<th style='text-align:left;font-size:11px;font-weight:500;"
                  f"color:var(--ink-3);padding-bottom:4px;'>{t}</th>"
                  for t in ["Titre", "Secteur", "Exercice", "", "Acquis",
                            "Ce qui manque"])
        + f"</tr></thead><tbody>{corps}</tbody></table></div>",
        unsafe_allow_html=True)

    # ── Focus bancaire ──
    banques = [e for e in lignes
               if "banque" in e["secteur"].lower() or "bank" in e["secteur"].lower()]
    if banques:
        section_heading("Suivi de l'analyse bancaire", spacing="loose")
        st.caption(
            "La grille bancaire exige cinq postes que les autres secteurs "
            "n'ont pas : frais généraux, résultat brut d'exploitation, coût du "
            "risque, dépôts et crédits. Sans eux, ni coefficient d'exploitation "
            "ni analyse en ciseau."
        )
        postes = ["Frais généraux", "Résultat brut d'exploitation",
                  "Coût du risque", "Dépôts clientèle", "Crédits à la clientèle"]
        entetes = "".join(
            f"<th style='font-size:10.5px;font-weight:500;color:var(--ink-3);"
            f"padding:0 6px 4px;text-align:center;'>{p.split()[0]}</th>"
            for p in postes)
        corps_b = ""
        for e in sorted(banques, key=lambda x: x["part"]):
            acquis = {l for l, _ in e["acquis"]}
            cases = ""
            for p in postes:
                ok = p in acquis
                cases += (
                    f"<td style='text-align:center;padding:6px;font-size:13px;"
                    f"color:{'var(--up)' if ok else 'var(--ink-3)'};'>"
                    f"{'●' if ok else '○'}</td>")
            corps_b += (
                f"<tr style='border-top:1px solid var(--border);'>"
                f"<td style='padding:6px 10px 6px 0;font-size:12.5px;"
                f"white-space:nowrap;'><b>{e['ticker']}</b></td>"
                f"<td style='padding:6px 10px 6px 0;font-size:12px;"
                f"text-align:center;color:var(--ink-3);'>{e['exercice'] or '—'}</td>"
                f"{cases}</tr>")
        st.markdown(
            f"<div style='overflow-x:auto;'>"
            f"<table style='width:100%;border-collapse:collapse;'>"
            f"<thead><tr>"
            f"<th style='text-align:left;font-size:10.5px;font-weight:500;"
            f"color:var(--ink-3);padding-bottom:4px;'>Banque</th>"
            f"<th style='font-size:10.5px;font-weight:500;color:var(--ink-3);"
            f"padding:0 6px 4px;'>Exercice</th>{entetes}"
            f"</tr></thead><tbody>{corps_b}</tbody></table></div>",
            unsafe_allow_html=True)
        pleins = sum(1 for e in banques
                     if all(p in {l for l, _ in e["acquis"]} for p in postes))
        st.caption(f"{pleins} banque(s) sur {len(banques)} disposent des cinq "
                   f"postes nécessaires à l'analyse du coût du risque.")

    # ── Ce qui reste a lire ──
    section_heading("Ce qui reste à lire", spacing="loose")
    with st.spinner("Recensement des documents…"):
        try:
            documents = reste_a_lire()
        except Exception as exc:
            documents = []
            st.caption(f"Recensement indisponible : {exc}")

    if not documents:
        st.caption("Aucun rapport annuel référencé ne laisse de trou.")
        return

    st.caption(
        f"{len(documents)} rapport(s) annuel(s) déjà référencé(s) dont les "
        f"données ne sont pas encore en base. Le classement met en tête ceux "
        f"qui combleraient le plus de trous."
    )
    for d in documents[:40]:
        st.markdown(
            f"<div style='border-top:1px solid var(--border);padding:7px 0;'>"
            f"<span style='font-size:12.5px;font-weight:600;'>{d['ticker']}</span>"
            f"<span style='font-size:12px;color:var(--ink-3);'> · exercice "
            f"{d['exercice']} · {d['type'].replace('_', ' ')}</span>"
            f"<div style='font-size:11px;color:var(--ink-3);'>"
            f"{d['nb_manque']} poste(s) manquant(s) : "
            f"{', '.join(d['manque'])}</div>"
            f"<a href='{d['url']}' target='_blank' "
            f"style='font-size:11px;'>ouvrir le document</a></div>",
            unsafe_allow_html=True)
    if len(documents) > 40:
        st.caption(f"… et {len(documents) - 40} autres.")
