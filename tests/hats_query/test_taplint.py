import json
import os
import shlex
import subprocess
from collections import Counter, defaultdict

import pytest

# Taplint errors, warnings and failures known to happen, as {"STAGE-CODE": count} in alphabetical order,
# please update when they change
KNOWN_FAILURES = {
    "EXA-QERX": 2,
    "EXA-VOCT": 2,
    "EXA-YJQB": 2,
    "MDQ-QERX": 7,
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
    "UPL-NOUP": 1,
    "UWS-QFAA": 3,
}


def collect_failures(output: dict) -> tuple[Counter, dict[str, list[str]]]:
    """Count taplint reports per "STAGE-CODE" and collect their messages"""
    failures = Counter()
    messages = defaultdict(list)
    for section in output["sections"]:
        for report in section["reports"]:
            code = f"{section['code']}-{report['code']}"
            # "N more reports of the same code" entries have "more" instead of "text"
            failures[code] += report.get("more", 1)
            if "text" in report:
                messages[code].append(f"[{section['title']}] {report['level']}: {report['text']}")
    return failures, messages


def describe_changes(failures: Counter, messages: dict[str, list[str]]) -> str:
    """Describe how failures differ from KNOWN_FAILURES, with messages of the new ones"""
    lines = []
    for code in sorted(failures.keys() | KNOWN_FAILURES.keys()):
        known, actual = KNOWN_FAILURES.get(code, 0), failures[code]
        if known == actual:
            continue
        lines.append(f"{code}: {known} -> {actual}")
        if actual > known:
            lines.extend(f"    {message}" for message in messages[code])
    return "\n".join(lines)


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
    failures, messages = collect_failures(output)
    print(f"taplint: {len(failures)} failing codes, {failures.total()} reports")

    if dict(failures) != KNOWN_FAILURES:
        pytest.fail(
            f"taplint failures changed, update KNOWN_FAILURES:\n{describe_changes(failures, messages)}"
        )
