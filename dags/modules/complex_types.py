import xmltodict
import pandas as pd
from pathlib import Path

PATH_TO_SCHEMA = Path("./data/schemas/")

"""
  Parse le dictionnaire des types complexes et retourne le résultat dans un dataframe et un dictionnaire
"""


def create_dict_from_xml(chemin_fichier):
    with open(chemin_fichier, encoding="utf8") as fd:
        doc = xmltodict.parse(fd.read(), dict_constructor=dict)
    return doc


def _parse_complex_type(annexe_type: dict):
    list_records = []
    all_complex_type = annexe_type["xs:schema"]["xs:complexType"]
    for complex_type in all_complex_type:
        temp_dict = dict()
        nom_type_complexe = complex_type["@name"]
        if isinstance(complex_type["xs:attribute"], dict):
            for element in complex_type["xs:attribute"]["xs:simpleType"][
                "xs:restriction"
            ]["xs:enumeration"]:
                temp_dict[element["@value"]] = element.get("xs:annotation", {}).get(
                    "xs:documentation", element["@value"]
                )

        result_dict = {"type": nom_type_complexe, "enum": temp_dict}
        list_records.append(result_dict)

    df = pd.DataFrame.from_records(list_records)

    return df


def _generate_complex_type_df(chemin: Path) -> pd.DataFrame:
    annexe_type = create_dict_from_xml(chemin)
    complexe_types_df = _parse_complex_type(annexe_type)

    return complexe_types_df


"""
  Génération d'une documentation des codes de données des annexes
"""


def _make_dict_champs(element: dict, nom_annexe: str) -> dict:
    documentation = element["xs:annotation"]["xs:documentation"]
    if isinstance(documentation, str):
        libelle = documentation
        description = documentation
    else:
        libelle = element["xs:annotation"]["xs:documentation"]["z:libelle"]
        description = element["xs:annotation"]["xs:documentation"].get("z:description")
    dict_champs = {
        "nom_annexe": nom_annexe,
        "nom_champ": element["@name"],
        "type": element["@type"],
        "libelle": libelle,
        "description": description,
    }
    return dict_champs


def _parse_annexe_fields_documentation(class_annexe: dict) -> pd.DataFrame:
    elements = class_annexe["xs:sequence"]["xs:element"]
    list_records = []
    dict_champs = dict()
    nom_annexe = class_annexe["@name"][1:]

    for element in elements:
        dict_champs = _make_dict_champs(element, nom_annexe)
        list_records.append(dict_champs)

    df = pd.DataFrame.from_records(list_records)
    df["description"] = df["description"].str.replace(r"^<[^<>]*>", "", regex=True)
    df["description"] = df["description"].str.replace(r"^\s*<ul>", "", regex=True)
    df["description"] = df["description"].str.replace(r"^\s*<li>", "", regex=True)
    df["description"] = df["description"].str.replace(r"<ul>", " : ", regex=True)
    df["description"] = df["description"].str.replace(r"<li>", " - ", regex=True)
    df["description"] = df["description"].str.replace(r"<[^<>]*>", " ", regex=True)
    df["description"] = df["description"].str.replace(r"\s\s+", " ", regex=True)

    return df


def _merge_annexe_type_and_documentation(
    chemin_annexe: Path, complex_type_df: pd.DataFrame
) -> pd.DataFrame:
    class_to_generate = create_dict_from_xml(chemin_annexe)["xs:schema"][
        "xs:complexType"
    ][1]
    init_df = _parse_annexe_fields_documentation(class_to_generate)
    init_df = init_df.merge(complex_type_df, how="left")
    return init_df


def _get_list_annexes_path():
    dict_annexe = create_dict_from_xml(
        PATH_TO_SCHEMA.joinpath("SchemaDocBudg/Class_Annexes.xsd")
    )["xs:schema"]["xs:include"]
    dict_annexe.pop(0)
    class_annexe_paths = []
    for annexe in dict_annexe:
        class_annexe_paths.append(
            PATH_TO_SCHEMA.joinpath(f"SchemaDocBudg/{annexe['@schemaLocation']}")
        )
    return class_annexe_paths


def parse_all_annexes_fields_documentation() -> pd.DataFrame:
    # Erreurs à traiter manuellement :
    # - l'annexe signatures dont la balise xs:complextype est inversée par rapport à l'habitude, il faut copier le premier bloc xs:complextype en dessous du 2ème.
    #       + suppression deux balises complexes types signataires (nested?)
    # - l'annexe emprunt "IndSousJacentDtVote" ou il y a deux balises documentation qui génère une liste (seul cas)
    df_result = pd.DataFrame()

    annexes_paths = _get_list_annexes_path()
    annexe_complexe_types = _generate_complex_type_df(
        PATH_TO_SCHEMA.joinpath("SchemaDocBudg/CommunAnnexe.xsd")
    )

    for annexe_path in annexes_paths:
        df = _merge_annexe_type_and_documentation(annexe_path, annexe_complexe_types)
        df_result = pd.concat([df, df_result])

    return df_result.set_index("nom_champ")


def parse_budget_fields_documentation() -> pd.DataFrame:
    annexe_complexe_types = _generate_complex_type_df(
        PATH_TO_SCHEMA.joinpath("SchemaDocBudg/CommunBudget.xsd")
    )
    class_to_generate = create_dict_from_xml(
        PATH_TO_SCHEMA.joinpath("SchemaDocBudg/Class_budget.xsd")
    )["xs:schema"]["xs:complexType"][1]
    init_df = _parse_annexe_fields_documentation(class_to_generate)
    init_df = init_df.merge(annexe_complexe_types, how="left")
    return init_df
