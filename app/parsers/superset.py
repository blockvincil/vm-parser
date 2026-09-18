"""
Pull chart data from Apache Superset.
  request.superset = {"chart_id": 42}                              -> saved chart query
  request.superset = {"chart_id": 42, "query_context": {...}}      -> custom query (filters etc.)
  request.superset = {"chart_id": 42, "extra_filters": [{"col":"currency","op":"==","val":"USD"}]}
"""
from __future__ import annotations
import httpx
from .base import BaseParser, RawTable


class SupersetParser(BaseParser):
    engine = "superset"

    def _session(self) -> httpx.Client:
        c = self.s.superset
        cli = httpx.Client(base_url=c.base_url, timeout=c.timeout_seconds, verify=c.verify_tls)
        tok = cli.post("/api/v1/security/login", json={
            "username": c.username, "password": c.password, "provider": c.provider, "refresh": True,
        }).raise_for_status().json()["access_token"]
        cli.headers["Authorization"] = f"Bearer {tok}"
        csrf = cli.get("/api/v1/security/csrf_token/").raise_for_status().json()["result"]
        cli.headers["X-CSRFToken"] = csrf
        cli.headers["Referer"] = c.base_url
        return cli

    def tables(self, _path=None):
        spec = self.req.superset or {}
        chart_id = spec.get("chart_id")
        if not chart_id:
            raise ValueError("superset.chart_id is required")
        with self._session() as cli:
            if spec.get("query_context"):
                qc = spec["query_context"]
            else:
                chart = cli.get(f"/api/v1/chart/{chart_id}").raise_for_status().json()["result"]
                import json
                if not chart.get("query_context"):
                    # older charts have no stored query_context -> GET data endpoint
                    body = cli.get(f"/api/v1/chart/{chart_id}/data/",
                                   params={"format": "json", "type": "full"}).raise_for_status().json()
                    yield from self._emit(body, chart_id)
                    return
                qc = json.loads(chart["query_context"])
            for q in qc.get("queries", []):
                q["row_limit"] = spec.get("row_limit", self.s.superset.row_limit)
                q.setdefault("filters", []).extend(spec.get("extra_filters", []))
            qc["result_format"], qc["result_type"] = "json", "full"
            body = cli.post("/api/v1/chart/data", json=qc).raise_for_status().json()
            yield from self._emit(body, chart_id)

    def _emit(self, body, chart_id):
        for i, res in enumerate(body.get("result", [])):
            data = res.get("data") or []
            cols = res.get("colnames") or (list(data[0].keys()) if data else [])
            yield RawTable(cols, (list(r.get(c) for c in cols) for r in data), 1,
                           {"chart_id": chart_id, "query_index": i, "rowcount": res.get("rowcount")})
