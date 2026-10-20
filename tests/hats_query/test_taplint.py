import json
import os
import shlex
import subprocess
from collections import Counter

import pytest

# Taplint errors, warnings and failures known to happen, as {"STAGE-CODE": count} in alphabetical order,
# please update when they change
KNOWN_FAILURES = {
    "AVV-GONO": 1,
    "CAP-CAP0": 2,
    "CAP-SVR0": 1,
    "CPV-GONM": 1,
    "EXA-EXNO": 1,
    "MDQ-NOTM": 1,
    "QAS-JREJ": 2,
    "QAS-QERR": 3,
    "QGE-DFSF": 2,
    "QGE-QERX": 5,
    "QGE-VOCT": 7,
    "QGE-YJQB": 7,
    "QPO-DFSF": 2,
    "QPO-QERX": 3,
    "QPO-VOCT": 5,
    "QPO-YJQB": 5,
    "TMC-NOTM": 1,
    "TME-CPIO": 1,
    "TME-GONE": 1,
    "TME-TBNF": 1,
    "TMS-CERR": 1,
    "TMS-CLIO": 1,
    "TMS-FKIO": 1,
    "TMS-GONE": 1,
    "TMS-KCIO": 1,
    "TMS-QERX": 6,
    "TMS-SCIO": 1,
    "TMS-TBIO": 1,
    "TMS-TST0": 5,
    "TMS-VOCT": 12,
    "TMS-YJQB": 12,
    "TMV-GONO": 1,
    "UPL-QERR": 2,
    "UUC-NOTM": 1,
    "UWS-QFAA": 3,
}


def count_failures(output: dict) -> Counter:
    """Count taplint reports per "STAGE-CODE\""""
    failures = Counter()
    for section in output["sections"]:
        for report in section["reports"]:
            # "N more reports of the same code" entries have "more" instead of "text"
            failures[f"{section['code']}-{report['code']}"] += report.get("more", 1)
    return failures


@pytest.mark.stilts
def test_taplint(live_server_url):
    """Run STILTS taplint, fail if its errors, warnings and failures differ from KNOWN_FAILURES"""
    # STILTS command, e.g. "java -jar stilts.jar"
    stilts = os.environ.get("STILTS", "stilts")
    result = subprocess.run(
        [
            *shlex.split(stilts),
            "taplint",
            f"tapurl={live_server_url}/tap",
            # All stages except optional data models: ObsCore, ObsLocTAP and EPN-TAP
            "stages=TMV TME TMS TMC UUC CPV CAP AVV QGE QPO QAS UWS MDQ EXA UPL",
            "format=json",
            # Errors, warnings and failures
            "report=EWF",
            "interface=tap1.1",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    output = json.loads(result.stdout)
    failures = count_failures(output)
    print(f"taplint: {len(failures)} failing codes, {failures.total()} reports")

    assert dict(failures) == KNOWN_FAILURES
