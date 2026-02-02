#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
02-ETL_ESMA_DAILY_BUILD_CSV_AUTONOME.py
======================================

Script AUTONOME (sans argument) :
- Cherche les XML déjà extraits sous :
    <DATA_ROOT>\extracted\DLTINS\<YYYYMMDD>\**\*.xml
    <DATA_ROOT>\extracted\FULINS\<YYYYMMDD>\**\*.xml

Règle d'entrée :
- DLTINS : on prend UNIQUEMENT le sous-répertoire de date MAX présent (un seul attendu).
- FULINS : on prend UNIQUEMENT le sous-répertoire de date MAX présent (un seul attendu).

Puis :
- Construit les fichiers BSV (pipe-delimited) :
    <DATA_ROOT>\csv\FULINS\<YYYYMMDD>\FULINS_WIDE_<YYYYMMDD>.bsv
    <DATA_ROOT>\csv\DLTINS\<YYYYMMDD>\dltins_wide_<YYYYMMDD>.bsv

Logging :
- Toutes les étapes sont loguées dans [log].[ESMA_Load_Log].

Aucun paramètre CLI.
"""
import sys
from pathlib import Path

def _bootstrap_sys_path() -> None:
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "config").exists() and (parent / "src" / "python").exists():
            sys.path.insert(0, str(parent / "src" / "python"))
            return
    raise RuntimeError("Repo root not found (expected 'config' and 'src/python').")

_bootstrap_sys_path()
from common.config_loader import load_config, resolve_project_root

import configparser
import csv
import hashlib
import re
import traceback
import xml.etree.ElementTree as ET
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pyodbc

SCRIPT_NAME = "02-ETL_ESMA_DAILY_BUILD_CSV_AUTONOME.py"
DELIMITER = "|"

DATA_ROOT = None
CONFIG_DIR = None

# Namespaces (identiques aux scripts historiques)
NS_FUL = {"a": "urn:iso:std:iso:20022:tech:xsd:auth.017.001.02"}
NS_DLT = {"a": "urn:iso:std:iso:20022:tech:xsd:auth.036.001.03"}

SAN_RE = re.compile(r"[\r\n\t]+")

# Colonnes FULL (alignées stg.ESMA_FULINS_WIDE)
COLUMNS_FULINS_WIDE = [
    "HeaderReportingMarketId","HeaderReportingNCA","HeaderReportingPeriodDate","SourceFileName","TechRcrdId",
    "ISIN","FullName","ShortName","CFI","CommodityDerivativeInd","NotionalCurrency","IssuerLEI","TradingVenueMIC",
    "IssuerReqAdmission","AdmissionApprvlDate","ReqForAdmissionDate","FirstTradingDate","TerminationDate",
    "TotalIssuedNominalAmount","TotalIssuedNominalAmountCcy","MaturityDate","NominalValuePerUnit","NominalValuePerUnitCcy",
    "FixedRate","FloatRefRateISIN","FloatRefRateIndex","FloatTermUnit","FloatTermValue","FloatBasisPointSpread","DebtSeniority",
    "ExpiryDate","PriceMultiplier","UnderlyingISIN","UnderlyingLEI","UnderlyingIndexRef","UnderlyingIndexTermUnit","UnderlyingIndexTermValue",
    "OptionType","OptionExerciseStyle","DeliveryType","StrikePrice","StrikePriceCcy","StrikeNoPriceCcy",
    "CmdtyBaseProduct","CmdtySubProduct","CmdtySubSubProduct","CmdtyTransactionType","CmdtyFinalPriceType",
    "ValidFromDate","ValidToDate","LatestRecordFlag",
]
COLUMNS_DLT_STG = COLUMNS_FULINS_WIDE + ["ActionType"]




# ----------------------------
# Config / SQL log
# ----------------------------




def sql_conn(cfg: configparser.ConfigParser) -> pyodbc.Connection:
    driver = cfg["SQLSERVER"].get("driver", "ODBC Driver 17 for SQL Server")
    server = cfg["SQLSERVER"]["server"]
    database = cfg["SQLSERVER"]["database_stg"]
    user = cfg["SQLSERVER"]["user"]
    password = cfg["SQLSERVER"]["password"]
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};DATABASE={database};"
        f"UID={user};PWD={password};"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str, autocommit=True)


def sql_log_line(conn: pyodbc.Connection, message: str, element: str = "", complement: str = "", file_name: str = "") -> None:
    sql = """
    INSERT INTO log.ESMA_Load_Log
        (ScriptName, LaunchTimestamp, StartTime, Message, FileName, Element, Complement)
    VALUES
        (?, SYSDATETIME(), SYSDATETIME(), ?, ?, ?, ?);
    """
    cur = conn.cursor()
    cur.execute(sql, (SCRIPT_NAME, str(message)[:4000], file_name[:260], element[:200], str(complement)[:4000]))
    cur.close()


def sql_log_long(conn: pyodbc.Connection, message: str, element: str, complement: str = "", file_name: str = "") -> None:
    if message is None:
        return
    chunk_size = 3500
    msg = str(message)
    if not msg:
        return
    total = (len(msg) + chunk_size - 1) // chunk_size
    for i in range(total):
        part = msg[i * chunk_size:(i + 1) * chunk_size]
        sql_log_line(conn, part, element=f"{element}_{i+1:02d}/{total:02d}", complement=complement, file_name=file_name)


# ----------------------------
# Helpers
# ----------------------------
def sanitize(v: str) -> str:
    if v is None:
        return ""
    s = str(v)
    s = SAN_RE.sub(" ", s)
    s = s.replace(DELIMITER, " ")
    return s.strip()


def md5_tech_id(source_file: str, isin: str, mic: str, idx: int) -> str:
    raw = f"{source_file}|{isin}|{mic}|{idx}".encode("utf-8")
    return hashlib.md5(raw).hexdigest()


def list_xmls(extracted_dir: Path) -> List[Path]:
    if not extracted_dir.exists():
        return []
    return sorted(extracted_dir.rglob("*.xml"))


def pick_max_yyyymmdd_dir(parent: Path) -> Optional[Tuple[str, Path]]:
    """
    Retourne (YYYYMMDD, path) du sous-dossier date max sous parent.
    Ignore tout ce qui n'est pas un dossier 8 chiffres.
    """
    if not parent.exists():
        return None
    best = None
    for p in parent.iterdir():
        if not p.is_dir():
            continue
        name = p.name.strip()
        if len(name) == 8 and name.isdigit():
            if best is None or name > best[0]:
                best = (name, p)
    return best


# ----------------------------
# XML helpers (FULL)
# ----------------------------
def _text(node, xpath: str, ns: Dict) -> str:
    if node is None:
        return ""
    el = node.find(xpath, ns)
    if el is None or el.text is None:
        return ""
    return el.text.strip()


def _first_text(node, xpaths: List[str], ns: Dict) -> str:
    for xp in xpaths:
        v = _text(node, xp, ns)
        if v:
            return v
    return ""


def _find_any(node, xpath: str, ns: Dict):
    if node is None:
        return None
    return node.find(xpath, ns)


def _find_first(node, xpath: str, ns: Dict):
    if node is None:
        return None
    items = node.findall(xpath, ns)
    return items[0] if items else None


def extract_record_fulins(refdata, hdr: Dict[str, str], source_file: str, record_idx: int) -> Dict[str, str]:
    row = {c: "" for c in COLUMNS_FULINS_WIDE}

    row["HeaderReportingMarketId"] = hdr.get("HeaderReportingMarketId", "")
    row["HeaderReportingNCA"] = hdr.get("HeaderReportingNCA", "")
    row["HeaderReportingPeriodDate"] = hdr.get("HeaderReportingPeriodDate", "")
    row["SourceFileName"] = source_file

    row["ISIN"] = _first_text(refdata, ["./a:FinInstrmGnlAttrbts/a:Id","./a:FinInstrmGnlAttrbts/a:ISIN"], NS_FUL)
    row["FullName"] = _text(refdata, "./a:FinInstrmGnlAttrbts/a:FullNm", NS_FUL)
    row["ShortName"] = _text(refdata, "./a:FinInstrmGnlAttrbts/a:ShrtNm", NS_FUL)
    row["CFI"] = _first_text(refdata, ["./a:FinInstrmGnlAttrbts/a:ClssfctnTp","./a:FinInstrmGnlAttrbts/a:CFI"], NS_FUL)
    row["CommodityDerivativeInd"] = _first_text(refdata, ["./a:FinInstrmGnlAttrbts/a:CmmdtyDerivInd","./a:FinInstrmGnlAttrbts/a:CommmodityDerivInd"], NS_FUL)
    row["NotionalCurrency"] = _first_text(refdata, ["./a:FinInstrmGnlAttrbts/a:NtnlCcy","./a:FinInstrmGnlAttrbts/a:NotionalCcy"], NS_FUL)

    row["IssuerLEI"] = _first_text(refdata, ["./a:Issr","./a:FinInstrmGnlAttrbts/a:Issr"], NS_FUL)

    row["TradingVenueMIC"] = _first_text(refdata, ["./a:TradgVnRltdAttrbts/a:Id","./a:TradgVnRltdAttrbts/a:MIC"], NS_FUL)
    row["IssuerReqAdmission"] = _first_text(refdata, ["./a:TradgVnRltdAttrbts/a:IssrReq","./a:TradgVnRltdAttrbts/a:IssrReqAdmssn"], NS_FUL)
    row["AdmissionApprvlDate"] = _first_text(refdata, ["./a:TradgVnRltdAttrbts/a:AdmssnApprvlDtByIssr","./a:TradgVnRltdAttrbts/a:AdmissionApprovalDate"], NS_FUL)
    row["ReqForAdmissionDate"] = _first_text(refdata, ["./a:TradgVnRltdAttrbts/a:ReqForAdmssnDt","./a:TradgVnRltdAttrbts/a:RequestForAdmissionDate"], NS_FUL)
    row["FirstTradingDate"] = _first_text(refdata, ["./a:TradgVnRltdAttrbts/a:FrstTradDt","./a:TradgVnRltdAttrbts/a:FirstTradingDate"], NS_FUL)
    row["TerminationDate"] = _first_text(refdata, ["./a:TradgVnRltdAttrbts/a:TermntnDt","./a:TradgVnRltdAttrbts/a:TerminationDate"], NS_FUL)

    debt = _find_any(refdata, "./a:DebtInstrmAttrbts", NS_FUL)
    if debt is not None:
        row["TotalIssuedNominalAmount"] = _first_text(debt, ["./a:TtlIssdNmnlAmt","./a:TotalIssuedNominalAmount"], NS_FUL)
        ccy = _text(debt, "./a:TtlIssdNmnlAmtCcy", NS_FUL)
        if not ccy:
            amt_el = debt.find("./a:TtlIssdNmnlAmt", NS_FUL)
            if amt_el is not None:
                ccy = amt_el.attrib.get("Ccy", "") or amt_el.attrib.get("CCY", "")
        row["TotalIssuedNominalAmountCcy"] = ccy

        row["MaturityDate"] = _first_text(debt, ["./a:MtrtyDt","./a:MaturityDate"], NS_FUL)
        row["NominalValuePerUnit"] = _first_text(debt, ["./a:NmnlValPerUnit","./a:NominalValuePerUnit"], NS_FUL)

        nvp_el = debt.find("./a:NmnlValPerUnit", NS_FUL)
        nvp_ccy = _text(debt, "./a:NmnlValPerUnitCcy", NS_FUL)
        if not nvp_ccy and nvp_el is not None:
            nvp_ccy = nvp_el.attrib.get("Ccy", "")
        row["NominalValuePerUnitCcy"] = nvp_ccy

        row["FixedRate"] = _first_text(debt, ["./a:FxddRate","./a:FixedRate"], NS_FUL)
        row["FloatRefRateISIN"] = _first_text(debt, [".//a:FltgRate/a:RefRate/a:Id",".//a:FloatgRate/a:RefRate/a:Id",".//a:FltgRate/a:RefRateISIN"], NS_FUL)
        row["FloatRefRateIndex"] = _first_text(debt, [".//a:FltgRate/a:RefRateIndx",".//a:FloatgRate/a:RefRateIndx",".//a:FltgRate/a:RefRateIndex"], NS_FUL)
        row["FloatTermUnit"] = _first_text(debt, [".//a:FltgRate/a:Term/a:Unit",".//a:FloatgRate/a:Term/a:Unit"], NS_FUL)
        row["FloatTermValue"] = _first_text(debt, [".//a:FltgRate/a:Term/a:Val",".//a:FloatgRate/a:Term/a:Val"], NS_FUL)
        row["FloatBasisPointSpread"] = _first_text(debt, [".//a:FltgRate/a:BssPtsSprd",".//a:FloatgRate/a:BssPtsSprd"], NS_FUL)
        row["DebtSeniority"] = _first_text(debt, ["./a:DbtSnrty","./a:DebtSeniority"], NS_FUL)

    der = _find_any(refdata, "./a:DerivInstrmAttrbts", NS_FUL)
    if der is not None:
        row["ExpiryDate"] = _first_text(der, ["./a:XpryDt","./a:ExpiryDate"], NS_FUL)
        row["PriceMultiplier"] = _first_text(der, ["./a:PricMltplr","./a:PriceMultiplier"], NS_FUL)

        und = _find_first(der, ".//a:UndrlygInstrm", NS_FUL)
        if und is not None:
            row["UnderlyingISIN"] = _first_text(und, ["./a:Id","./a:ISIN"], NS_FUL)
            row["UnderlyingLEI"] = _first_text(und, ["./a:LEI","./a:IdLEI"], NS_FUL)
            row["UnderlyingIndexRef"] = _first_text(und, ["./a:Indx/a:Id","./a:Indx/a:Nm","./a:Index/a:Id","./a:Index/a:Name"], NS_FUL)
            row["UnderlyingIndexTermUnit"] = _first_text(und, [".//a:Indx/a:Term/a:Unit",".//a:Index/a:Term/a:Unit"], NS_FUL)
            row["UnderlyingIndexTermValue"] = _first_text(und, [".//a:Indx/a:Term/a:Val",".//a:Index/a:Term/a:Val"], NS_FUL)

        row["OptionType"] = _first_text(der, [".//a:OptnTp",".//a:OptionType"], NS_FUL)
        row["OptionExerciseStyle"] = _first_text(der, [".//a:ExrcStyle",".//a:ExerciseStyle"], NS_FUL)
        row["DeliveryType"] = _first_text(der, [".//a:DlvryTp",".//a:DeliveryType"], NS_FUL)

        row["StrikePrice"] = _first_text(der, [".//a:StrkPric/a:Val",".//a:StrikePrice/a:Val",".//a:StrkPric"], NS_FUL)
        row["StrikePriceCcy"] = _first_text(der, [".//a:StrkPric/a:Ccy",".//a:StrikePrice/a:Ccy"], NS_FUL)
        row["StrikeNoPriceCcy"] = _first_text(der, [".//a:StrkNoPric/a:Ccy",".//a:StrikeNoPrice/a:Ccy"], NS_FUL)

        row["CmdtyBaseProduct"] = _first_text(der, [".//a:BasePdct"], NS_FUL)
        row["CmdtySubProduct"] = _first_text(der, [".//a:SubPdct"], NS_FUL)
        row["CmdtySubSubProduct"] = _first_text(der, [".//a:SubSubPdct"], NS_FUL)
        row["CmdtyTransactionType"] = _first_text(der, [".//a:TxTp"], NS_FUL)
        row["CmdtyFinalPriceType"] = _first_text(der, [".//a:FnlPricTp"], NS_FUL)

    row["ValidFromDate"] = row["HeaderReportingPeriodDate"]
    row["ValidToDate"] = ""
    row["LatestRecordFlag"] = "1"
    row["TechRcrdId"] = md5_tech_id(source_file, row["ISIN"], row["TradingVenueMIC"], record_idx)

    return row


def extract_fulins_xmls_to_bsv(xml_files: List[Path], out_bsv: Path, conn: pyodbc.Connection, run_ts: str) -> int:
    out_bsv.parent.mkdir(parents=True, exist_ok=True)
    with out_bsv.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter=DELIMITER, lineterminator="\n", quoting=csv.QUOTE_NONE, escapechar="\\")
        w.writerow(COLUMNS_FULINS_WIDE)

        total = 0
        for xml_path in xml_files:
            source_file = xml_path.name
            sql_log_line(conn, "Parsing XML", element="FUL_PARSE", complement=f"file={xml_path} run_ts={run_ts}")

            context = ET.iterparse(xml_path, events=("start", "end"))
            _, root = next(context)

            hdr = {"HeaderReportingMarketId": "", "HeaderReportingNCA": "", "HeaderReportingPeriodDate": ""}
            record_idx = 0

            for ev, elem in context:
                if ev == "end" and str(elem.tag).endswith("FinInstrmRptgRefDataRpt"):
                    hdr_el = elem.find("./a:RptHdr", NS_FUL)
                    if hdr_el is not None:
                        hdr["HeaderReportingMarketId"] = _first_text(hdr_el, ["./a:RptgNtty/a:MktIdCd", "./a:RptgNtty/a:MktId"], NS_FUL)
                        hdr["HeaderReportingNCA"] = _first_text(hdr_el, ["./a:RptgNtty/a:NtlCmptntAuthrty", "./a:RptgNtty/a:NCA"], NS_FUL)
                        hdr["HeaderReportingPeriodDate"] = _first_text(hdr_el, ["./a:RptgPrd/a:Dt", "./a:ReportingPeriod/a:Date"], NS_FUL)

                    for refdata in elem.findall("./a:RefData", NS_FUL):
                        record_idx += 1
                        row = extract_record_fulins(refdata, hdr, source_file, record_idx)
                        if not row.get("ISIN"):
                            continue
                        w.writerow([sanitize(row.get(c, "")) for c in COLUMNS_FULINS_WIDE])
                        total += 1

                    elem.clear()
                    root.clear()

        return total


# ----------------------------
# XML helpers (DELTA)
# ----------------------------
def _text_dlt(node, xpath: str) -> str:
    return _text(node, xpath, NS_DLT)


def _find_any_dlt(node, xpath: str):
    return _find_any(node, xpath, NS_DLT)


def _find_first_dlt(node, xpath: str):
    return _find_first(node, xpath, NS_DLT)


def get_valid_from_dlt(refdata) -> str:
    for xp in ["./a:FinInstrmGnlAttrbts/a:VldFr", ".//a:VldFr", ".//a:ValidFrom"]:
        v = _text_dlt(refdata, xp)
        if v:
            return v
    return ""


def parse_refdata_to_wide_dlt(refdata) -> Dict[str, str]:
    row = {c: "" for c in COLUMNS_FULINS_WIDE}

    row["ISIN"] = _first_text(refdata, ["./a:FinInstrmGnlAttrbts/a:Id", "./a:FinInstrmGnlAttrbts/a:ISIN"], NS_DLT)
    row["FullName"] = _text_dlt(refdata, ".//a:FullNm")
    row["ShortName"] = _text_dlt(refdata, ".//a:ShrtNm")
    row["CFI"] = _first_text(refdata, [".//a:ClssfctnTp", ".//a:CFI"], NS_DLT)
    row["CommodityDerivativeInd"] = _first_text(refdata, [".//a:CmmdtyDerivInd", ".//a:CommmodityDerivInd"], NS_DLT)
    row["NotionalCurrency"] = _first_text(refdata, [".//a:NtnlCcy", ".//a:NotionalCcy"], NS_DLT)
    row["IssuerLEI"] = _first_text(refdata, [".//a:Issr", ".//a:IssrLEI"], NS_DLT)

    row["TradingVenueMIC"] = _first_text(refdata, [".//a:TradgVnRltdAttrbts/a:Id", ".//a:TradgVnRltdAttrbts/a:MIC"], NS_DLT)

    row["IssuerReqAdmission"] = _first_text(refdata, [".//a:IssrReqAdmssn", ".//a:IssrReq"], NS_DLT)
    row["AdmissionApprvlDate"] = _first_text(refdata, [".//a:AdmssnApprvlDtByIssr", ".//a:AdmissionApprovalDate"], NS_DLT)
    row["ReqForAdmissionDate"] = _first_text(refdata, [".//a:ReqForAdmssnDt", ".//a:RequestForAdmissionDate"], NS_DLT)
    row["FirstTradingDate"] = _first_text(refdata, [".//a:FrstTradDt", ".//a:FirstTradingDate"], NS_DLT)
    row["TerminationDate"] = _first_text(refdata, [".//a:TermntnDt", ".//a:TerminationDate"], NS_DLT)

    debt = _find_any_dlt(refdata, ".//a:DebtInstrmAttrbts")
    if debt is not None:
        row["TotalIssuedNominalAmount"] = _first_text(debt, [".//a:TtlIssdNmnlAmt", ".//a:TotalIssuedNominalAmount"], NS_DLT)
        amt_el = debt.find(".//a:TtlIssdNmnlAmt", NS_DLT)
        if amt_el is not None:
            row["TotalIssuedNominalAmountCcy"] = amt_el.attrib.get("Ccy", "") or amt_el.attrib.get("CCY", "")
        row["MaturityDate"] = _first_text(debt, [".//a:MtrtyDt", ".//a:MaturityDate"], NS_DLT)
        row["NominalValuePerUnit"] = _first_text(debt, [".//a:NmnlValPerUnit", ".//a:NominalValuePerUnit"], NS_DLT)
        nvp_el = debt.find(".//a:NmnlValPerUnit", NS_DLT)
        if nvp_el is not None:
            row["NominalValuePerUnitCcy"] = nvp_el.attrib.get("Ccy", "") or nvp_el.attrib.get("CCY", "")
        row["FixedRate"] = _first_text(debt, [".//a:FxddRate", ".//a:FixedRate"], NS_DLT)
        row["FloatRefRateISIN"] = _first_text(debt, [".//a:RefRate/a:Id", ".//a:RefRateISIN"], NS_DLT)
        row["FloatRefRateIndex"] = _first_text(debt, [".//a:RefRateIndx", ".//a:RefRateIndex"], NS_DLT)
        row["FloatTermUnit"] = _first_text(debt, [".//a:Term/a:Unit"], NS_DLT)
        row["FloatTermValue"] = _first_text(debt, [".//a:Term/a:Val"], NS_DLT)
        row["FloatBasisPointSpread"] = _first_text(debt, [".//a:BssPtsSprd"], NS_DLT)
        row["DebtSeniority"] = _first_text(debt, [".//a:DbtSnrty", ".//a:DebtSeniority"], NS_DLT)

    der = _find_any_dlt(refdata, ".//a:DerivInstrmAttrbts")
    if der is not None:
        row["ExpiryDate"] = _first_text(der, [".//a:XpryDt", ".//a:ExpiryDate"], NS_DLT)
        row["PriceMultiplier"] = _first_text(der, [".//a:PricMltplr", ".//a:PriceMultiplier"], NS_DLT)

        und = _find_first_dlt(der, ".//a:UndrlygInstrm")
        if und is not None:
            row["UnderlyingISIN"] = _text_dlt(und, "./a:Id") or _text_dlt(und, "./a:ISIN")
            row["UnderlyingLEI"] = _text_dlt(und, "./a:LEI")
            idx = _find_any_dlt(und, "./a:Indx")
            if idx is not None:
                row["UnderlyingIndexRef"] = _text_dlt(idx, "./a:Id") or _text_dlt(idx, "./a:Nm")
                term = _find_any_dlt(idx, "./a:Term")
                row["UnderlyingIndexTermUnit"] = _text_dlt(term, "./a:Unit")
                row["UnderlyingIndexTermValue"] = _text_dlt(term, "./a:Val")

        row["OptionType"] = _text_dlt(der, ".//a:OptnTp")
        row["OptionExerciseStyle"] = _text_dlt(der, ".//a:ExrcStyle")
        row["DeliveryType"] = _text_dlt(der, ".//a:DlvryTp")

        sp = _find_any_dlt(der, ".//a:StrkPric")
        if sp is not None:
            row["StrikePrice"] = _text_dlt(sp, "./a:Val")
            row["StrikePriceCcy"] = _text_dlt(sp, "./a:Ccy")

        snp = _find_any_dlt(der, ".//a:StrkNoPric")
        if snp is not None:
            row["StrikeNoPriceCcy"] = _text_dlt(snp, "./a:Ccy")

        row["CmdtyBaseProduct"] = _text_dlt(der, ".//a:BasePdct")
        row["CmdtySubProduct"] = _text_dlt(der, ".//a:SubPdct")
        row["CmdtySubSubProduct"] = _text_dlt(der, ".//a:SubSubPdct")
        row["CmdtyTransactionType"] = _text_dlt(der, ".//a:TxTp")
        row["CmdtyFinalPriceType"] = _text_dlt(der, ".//a:FnlPricTp")

    return row


def extract_dltins_xmls_to_bsv(xml_files: List[Path], out_bsv: Path, conn: pyodbc.Connection, run_ts: str) -> int:
    out_bsv.parent.mkdir(parents=True, exist_ok=True)

    def _iter_refdata_nodes(record_elem):
        refdatas = record_elem.findall(".//a:RefData", NS_DLT)
        if refdatas:
            return refdatas
        if record_elem.find(".//a:FinInstrmGnlAttrbts", NS_DLT) is not None:
            return [record_elem]
        return []

    with out_bsv.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter=DELIMITER, lineterminator="\n", quoting=csv.QUOTE_NONE, escapechar="\\")
        w.writerow(COLUMNS_DLT_STG)

        total = 0
        for xml_path in xml_files:
            source_file = xml_path.name
            sql_log_line(conn, "Parsing XML", element="DLT_PARSE", complement=f"file={xml_path} run_ts={run_ts}")

            context = ET.iterparse(xml_path, events=("start", "end"))
            _, root = next(context)

            hdr_market = ""
            hdr_nca = ""
            hdr_period = ""
            record_idx = 0

            for ev, elem in context:
                if ev != "end":
                    continue
                if not str(elem.tag).endswith("FinInstrmRptgRefDataDltaRpt"):
                    continue

                hdr = elem.find("./a:RptHdr", NS_DLT)
                if hdr is not None:
                    hdr_market = _text_dlt(hdr, "./a:RptgNtty/a:MktIdCd")
                    hdr_nca = _text_dlt(hdr, "./a:RptgNtty/a:NtlCmptntAuthrty")
                    hdr_period = _text_dlt(hdr, "./a:RptgPrd/a:Dt")

                blocks = [
                    ("NEW",  ".//a:NewRcrd"),
                    ("MOD",  ".//a:ModfdRcrd"),
                    ("TERM", ".//a:TermntdRcrd"),
                    ("CANC", ".//a:CancRcrd"),
                ]

                for action, bx in blocks:
                    for record_elem in elem.findall(bx, NS_DLT):
                        for refdata in _iter_refdata_nodes(record_elem):
                            record_idx += 1
                            row = parse_refdata_to_wide_dlt(refdata)

                            row["HeaderReportingMarketId"] = hdr_market
                            row["HeaderReportingNCA"] = hdr_nca
                            row["HeaderReportingPeriodDate"] = hdr_period
                            row["SourceFileName"] = source_file

                            row["ValidFromDate"] = get_valid_from_dlt(refdata)
                            row["ValidToDate"] = ""
                            row["LatestRecordFlag"] = "1"

                            if not row.get("TechRcrdId"):
                                row["TechRcrdId"] = md5_tech_id(source_file, row.get("ISIN", ""), row.get("TradingVenueMIC", ""), record_idx)

                            if not row.get("ISIN") or not row.get("TradingVenueMIC"):
                                continue

                            out = [sanitize(row.get(c, "")) for c in COLUMNS_FULINS_WIDE]
                            out.append(action)
                            w.writerow(out)
                            total += 1

                elem.clear()
                root.clear()

        return total


# ----------------------------
# Main
# ----------------------------
def main() -> int:
    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    script_dir = Path(__file__).resolve().parent
    root_dir = resolve_project_root()
    data_root = root_dir / "data"

    cfg = load_config()
    conn = sql_conn(cfg)

    try:
        sql_log_line(conn, "BEGIN", element="BUILD_CSV", complement=f"run_ts={run_ts} data_root={data_root}")

        extracted_root = data_root / "extracted"
        csv_root = data_root / "csv"


        # FULL : max date
        ful_parent = extracted_root / "FULINS"
        ful_pick = pick_max_yyyymmdd_dir(ful_parent)
        if ful_pick is None:
            sql_log_line(conn, "FULL - No extracted folder found", element="FUL_SKIP", complement=str(ful_parent))
        else:
            ful_d, ful_dir = ful_pick
            xmls = list_xmls(ful_dir)
            out_bsv = csv_root / "FULINS" / ful_d / f"FULINS_WIDE_{ful_d}.bsv"
            sql_log_line(conn, f"FULL - picked date={ful_d} xmls={len(xmls)}", element="FUL_PLAN", complement=f"dir={ful_dir} out={out_bsv} run_ts={run_ts}")
            if not xmls:
                sql_log_line(conn, "FULL - No XML found, skip", element="FUL_SKIP", complement=f"dir={ful_dir}")
            else:
                rows = extract_fulins_xmls_to_bsv(xmls, out_bsv, conn, run_ts)
                sql_log_line(conn, f"FULL_RESULT - rows={rows}", element="FUL_RESULT", complement=str(out_bsv))

        # DELTA : max date
        dlt_parent = extracted_root / "DLTINS"
        dlt_pick = pick_max_yyyymmdd_dir(dlt_parent)
        if dlt_pick is None:
            sql_log_line(conn, "DELTA - No extracted folder found", element="DLT_SKIP", complement=str(dlt_parent))
        else:
            dlt_d, dlt_dir = dlt_pick
            xmls = list_xmls(dlt_dir)
            out_bsv = csv_root / "DLTINS" / dlt_d / f"dltins_wide_{dlt_d}.bsv"
            sql_log_line(conn, f"DELTA - picked date={dlt_d} xmls={len(xmls)}", element="DLT_PLAN", complement=f"dir={dlt_dir} out={out_bsv} run_ts={run_ts}")
            if not xmls:
                sql_log_line(conn, "DELTA - No XML found, skip", element="DLT_SKIP", complement=f"dir={dlt_dir}")
            else:
                rows = extract_dltins_xmls_to_bsv(xmls, out_bsv, conn, run_ts)
                sql_log_line(conn, f"DELTA_RESULT - rows={rows}", element="DLT_RESULT", complement=str(out_bsv))

        sql_log_line(conn, "END", element="END", complement=f"run_ts={run_ts}")
        return 0

    except Exception:
        tb = traceback.format_exc()
        sql_log_long(conn, tb, element="TRACEBACK", complement=f"run_ts={run_ts}")
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
