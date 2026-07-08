from dataclasses import dataclass, field
from typing import Optional, List
from enum import Enum


class Severity(str, Enum):
    CRITICAL = "CRITICAL"
    HIGH     = "HIGH"
    MEDIUM   = "MEDIUM"
    LOW      = "LOW"
    INFO     = "INFO"


class Category(str, Enum):
    SECRETS            = "secrets"
    SQL_SECURITY       = "sql_security"
    SQL_COMPLEXITY     = "sql_complexity"
    SQL_QUALITY        = "sql_quality"
    PYTHON_SECURITY    = "python_security"
    PYTHON_COMPLEXITY  = "python_complexity"
    PYTHON_QUALITY     = "python_quality"
    DEPENDENCY         = "dependency"
    DBT_QUALITY        = "dbt_quality"
    GENERIC            = "generic"


@dataclass
class Finding:
    check_id:          str
    title:             str
    severity:          Severity
    category:          Category
    file:              str
    line:              int            = 0
    column:            int            = 0
    description:       str            = ""
    suggestion:        str            = ""
    code_snippet:      str            = ""
    time_complexity:   Optional[str]  = None
    space_complexity:  Optional[str]  = None
    cwe:               Optional[str]  = None
    owasp:             Optional[str]  = None

    def to_dict(self) -> dict:
        return {
            "check_id":         self.check_id,
            "title":            self.title,
            "severity":         self.severity.value,
            "category":         self.category.value,
            "file":             self.file,
            "line":             self.line,
            "description":      self.description,
            "suggestion":       self.suggestion,
            "code_snippet":     self.code_snippet,
            "time_complexity":  self.time_complexity,
            "space_complexity": self.space_complexity,
            "cwe":              self.cwe,
            "owasp":            self.owasp,
        }


class BaseScanner:
    name: str = "base"

    def scan(self, manifest: dict) -> List[Finding]:
        raise NotImplementedError
