import hashlib

from collections.abc import Sequence
from typing import Any, Dict, List, Tuple, Union

class StringHelper:
    @staticmethod
    def build_where_clause(
        filters: Dict[str, Tuple[str, Any | Sequence[Union[int, str]]]]
    ) -> Tuple[str, List[Any]]:
        """
        Build a SQL WHERE clause from filters.
        - Single values -> `col = ?`
        - IN/NOT IN lists -> `col IN (?, ?, …)`
        """
        if not filters:
            return "", []

        conditions: List[str] = []
        params: List[Any] = []

        for column, (operator, value) in filters.items():
            # skip nulls
            if value is None:
                continue

            op = operator.strip().upper()
            match op:
                case "IN" | "NOT IN":
                    # ensure a non-string iterable
                    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
                        raise TypeError(f"Value for {op} must be a non-string sequence")
                    # empty list edge-case
                    if not value:
                        # IN () is invalid SQL; fall back to always-false/true
                        conditions.append("1=0" if op == "IN" else "1=1")
                    else:
                        placeholders = ", ".join("?" for _ in value)
                        conditions.append(f"{column} {op} ({placeholders})")
                        params.extend(value)

                case _:
                    # single-value operators shouldn’t get a list
                    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
                        raise TypeError(f"Operator {op} does not support sequence values")
                    conditions.append(f"{column} {operator} ?")
                    params.append(value)

        where_clause = f" WHERE {' AND '.join(conditions)}" if conditions else ""
        return where_clause, params

def compute_definition_hash(
    *,
    source_type: str,
    scope: str,
    s_node_id: int,
    e_node_id: int | None,
    filter_fab_no: int | None,
    filter_model_no: int | None,
    filter_phase_no: int | None,
    filter_toolset_no: int | None,
    filter_e2e_group_nos: str,
    filter_category_nos: str,
    filter_utilitie_nos: str,
    filter_references: str,
    target_data_codes: str,
    forbidden_node_ids: str,
) -> str:
    parts = [
        source_type or '',
        scope or '',
        str(s_node_id),
        str(e_node_id or ''),
        str(filter_fab_no or ''),
        str(filter_model_no or ''),
        str(filter_phase_no or ''),
        str(filter_toolset_no or ''),
        filter_e2e_group_nos or '',
        filter_category_nos or '',
        filter_utilitie_nos or '',
        filter_references or '',
        target_data_codes or '',
        forbidden_node_ids or '',
    ]
    canon = "|".join(parts)
    return hashlib.sha256(canon.encode('utf-8')).hexdigest()

# Example usage:
h = compute_definition_hash(
    source_type="RANDOM", scope="CONNECTIVITY",
    s_node_id=123, e_node_id=None,
    filter_fab_no=None, filter_model_no=2, filter_phase_no=1, filter_toolset_no=42,
    filter_e2e_group_nos="100,200", filter_category_nos="10", filter_utilitie_nos="5,6",
    filter_references="ABC", target_data_codes="15000,16000", forbidden_node_ids="999"
)
print(h)  # 64-char hex string
