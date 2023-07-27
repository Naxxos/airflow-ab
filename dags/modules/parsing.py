import copy
import pandas as pd

from . import config

CHAMPS_BUDG = config.CHAMPS_LIGNE_BUDGET


def parsing_infos_coll(dict_from_xml: dict):
    infos_dict = dict()
    dict_entete_doc = dict_from_xml["DocumentBudgetaire"]["EnTeteDocBudgetaire"]
    infos_dict["siret_coll"] = dict_entete_doc["IdColl"]["@V"]
    infos_dict["libelle_collectivite"] = dict_entete_doc["LibelleColl"]["@V"]
    infos_dict["nature_collectivite"] = dict_entete_doc["NatCEPL"]["@V"]
    infos_dict["departement"] = dict_entete_doc.get("Departement", {}).get("@V", None)

    return infos_dict


def parsing_infos_etablissement(dict_from_xml: dict, id_doc_budg):
    infos_dict = dict()
    dict_entete_budget = dict_from_xml["DocumentBudgetaire"]["Budget"]["EnTeteBudget"]
    dict_bloc_budget = dict_from_xml["DocumentBudgetaire"]["Budget"]["BlocBudget"]

    infos_dict["id_doc_budg"] = id_doc_budg

    # Parsing entête budgétaire
    infos_dict["siret_etablissement"] = dict_entete_budget["IdEtab"]["@V"]
    infos_dict["libelle"] = dict_entete_budget["LibelleEtab"]["@V"]
    infos_dict["code_insee"] = dict_entete_budget.get("CodInseeColl", {}).get(
        "@V", None
    )
    infos_dict["nomenclature"] = dict_entete_budget["Nomenclature"]["@V"]

    # Parsing bloc budget
    infos_dict["exercice"] = int(dict_bloc_budget["Exer"]["@V"])
    infos_dict["nature_dec"] = dict_bloc_budget["NatDec"]["@V"]
    infos_dict["num_dec"] = int(dict_bloc_budget.get("NumDec", {}).get("@V", None) or 0)
    infos_dict["nature_vote"] = dict_bloc_budget["NatFonc"]["@V"]
    infos_dict["type_budget"] = dict_bloc_budget["CodTypBud"]["@V"]
    infos_dict["id_etabl_princ"] = dict_bloc_budget.get("IdEtabPal", {}).get("@V", None)

    # Parsing des lignes de budget -> json
    infos_dict["json_budget"] = generate_dict_budget(dict_from_xml)
    # duplique certaines données et non nécessaire pour l'instant donc je le commente
    # if isinstance(dict_from_xml["DocumentBudgetaire"]["Budget"]["Annexes"], dict):
    #    infos_dict["list_annexes"] = list(dict_from_xml["DocumentBudgetaire"]["Budget"]["Annexes"].keys())

    infos_dict["fk_siret_collectivite"] = dict_from_xml["DocumentBudgetaire"][
        "EnTeteDocBudgetaire"
    ]["IdColl"]["@V"]

    return infos_dict


def generate_dict_budget(dict_from_xml: dict) -> dict:
    budget_dict = copy.deepcopy(
        dict_from_xml["DocumentBudgetaire"]["Budget"]["LigneBudget"]
    )

    if isinstance(budget_dict, dict):
        for field in config.CHAMPS_LIGNE_BUDGET:
            if field in budget_dict:
                if "@V" in budget_dict[field]:
                    budget_dict[field] = budget_dict[field]["@V"]
        # for field in ["MtSup", "CaracSup"]:
        #     if field in budget_dict:
        #         if "@V" in budget_dict[field]:
        #             budget_dict[field] = { budget_dict[field]['@Code']: budget_dict[field]['@V'] }
        return budget_dict
    for idx, row in enumerate(budget_dict):
        for field in config.CHAMPS_LIGNE_BUDGET:
            if field in row:
                if "@V" in row[field]:
                    budget_dict[idx][field] = row[field]["@V"]
        for field in ["MtSup", "CaracSup"]:
            if field in row:
                budget_dict[idx].pop(field)
        # for field in ["MtSup", "CaracSup"]:
        # if field in row:
        #     if "@V" in row[field]:
        #         budget_dict[idx][field] = { row[field]['@Code']: row[field]['@V'] }
    return budget_dict


def _all_annexe_columns(df, fields):
    all_columns = copy.deepcopy(fields)
    columns = list(df.columns)
    for i in all_columns:
        columns.append(i)
    return columns


def explode_annexe_json_into_rows_first_way(df, fields):
    all_columns = _all_annexe_columns(df, fields)
    all_columns.pop(all_columns.index("json_budget"))
    df["json_budget"] = df.json_budget.apply(lambda x: eval(str(x)))
    temp = df.groupby("id_doc_budg").json_budget.apply(
        lambda x: pd.DataFrame(x.values[0], index=[0]).reset_index()
    )
    df.drop(columns="json_budget", inplace=True)
    df_result = df.merge(temp, left_on="id_doc_budg", right_on="id_doc_budg")
    return df_result.reindex(columns=all_columns)
