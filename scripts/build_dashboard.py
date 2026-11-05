"""Gera um dashboard HTML estático (Plotly) a partir das tabelas Gold/governance REAIS.

Também salva ``docs/results/gold_snapshot.json`` (números citados no README) e
``docs/results/ledger_comparison.json`` (anomalias injetadas x detectadas).
Só tabelas agregadas (pequenas) são trazidas para o driver com ``toPandas``.

    python scripts/build_dashboard.py --env databricks
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import plotly.graph_objects as go  # noqa: E402

from loyalty_lakehouse.config import load_settings  # noqa: E402
from loyalty_lakehouse.quality import compare_with_ledger  # noqa: E402
from loyalty_lakehouse.utils import Lakehouse, get_spark  # noqa: E402

# Paleta categórica validada para daltonismo (ordem fixa) e rampa sequencial azul.
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
BLUE_RAMP = ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"]
INK, INK2, MUTED, GRID, SURFACE = "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#fcfcfb"


def layout(fig, title: str, height: int = 360):
    fig.update_layout(
        title=dict(text=title, font=dict(size=15, color=INK), x=0, xanchor="left"),
        height=height, margin=dict(l=56, r=24, t=56, b=48), paper_bgcolor=SURFACE, plot_bgcolor=SURFACE,
        font=dict(family="Inter, Segoe UI, sans-serif", size=12, color=INK2), hoverlabel=dict(bgcolor="white"),
        legend=dict(orientation="h", y=-0.18, x=0), bargap=0.25)
    fig.update_xaxes(showgrid=False, linecolor="#c3c2b7", tickfont=dict(color=MUTED))
    fig.update_yaxes(gridcolor=GRID, zeroline=False, tickfont=dict(color=MUTED))
    return fig


def bar(x, y, name, color=SERIES[0], **kw):
    return go.Bar(x=x, y=y, name=name, marker=dict(color=color, line=dict(color=SURFACE, width=2)),
                  hovertemplate="%{x}<br>%{y:,.2f}<extra>" + name + "</extra>", **kw)


def fmt_brl(v) -> str:
    return "R$ " + f"{float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def fmt_int(v) -> str:
    return f"{int(v):,}".replace(",", ".")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--env", default="databricks", choices=["local", "databricks"])
    p.add_argument("--ledger", default=None, help="anomaly_ledger.csv (padrão: data/generated)")
    p.add_argument("--output", default="docs/dashboard/index.html")
    a = p.parse_args()
    settings = load_settings(a.env)
    lh = Lakehouse(settings, get_spark(settings))
    g = lambda n: lh.read(lh.table("gold", n)).toPandas()  # noqa: E731  (tabelas agregadas, pequenas)
    gov = lambda n: lh.read(lh.table("governance", n))  # noqa: E731

    monthly = g("kpi_monthly_sales").sort_values("month")
    pts = g("kpi_points_summary").iloc[0]
    red = g("kpi_redemption_summary").iloc[0]
    active = g("kpi_active_customers_monthly").sort_values("month")
    camp = g("kpi_campaign_performance").sort_values("conversion_rate", ascending=False)
    cohort = g("kpi_cohort_retention")
    seg = g("kpi_segment_monthly")
    sla = g("kpi_credit_sla_daily")
    ptype = g("kpi_points_daily")
    backlog = g("operational_backlog")
    quality = g("kpi_quality_summary")
    run_id = quality["run_id"].iloc[0]
    q = lh.read(lh.table("silver", "quarantine")).groupBy("entity", "primary_reason").count().toPandas()
    incidents = gov("data_quality_incidents").where(f"run_id = '{run_id}'").groupBy("category", "severity").count().toPandas()
    runs = (gov("pipeline_execution_log").groupBy("run_id").agg({"duration_seconds": "sum", "rows_written": "sum"})
            .toPandas().sort_values("run_id").tail(10))
    ledger_path = Path(a.ledger) if a.ledger else settings.generated_path / "anomaly_ledger.csv"
    ledger_cmp = compare_with_ledger(lh, ledger_path, run_id)

    last = active.iloc[-1]
    total = monthly[["completed_purchases", "net_sales", "realized_revenue", "gross_sales", "discounts",
                     "returns_amount", "cancelled_purchases"]].sum()
    all_q = quality[quality["check_category"] == "ALL"].iloc[0]
    snapshot = {
        "run_id": run_id, "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "reference_timestamp": settings.rules["reference_timestamp"],
        "sales": {"completed_purchases": int(total["completed_purchases"]), "gross_sales": float(total["gross_sales"]),
                  "discounts": float(total["discounts"]), "net_sales": float(total["net_sales"]),
                  "returns_amount": float(total["returns_amount"]), "realized_revenue": float(total["realized_revenue"]),
                  "cancelled_purchases": int(total["cancelled_purchases"]),
                  "avg_ticket": round(float(total["net_sales"]) / float(total["completed_purchases"]), 2),
                  "monthly": monthly[["month", "completed_purchases", "net_sales", "avg_ticket",
                                      "mom_net_sales_change_pct"]].astype(str).to_dict("records")},
        "points": {k: (None if v != v else (float(v) if isinstance(v, float) else int(v) if hasattr(v, "__int__") else str(v)))
                   for k, v in pts.items() if k != "reference_timestamp"},
        "redemptions": {k: (float(v) if isinstance(v, float) else int(v)) for k, v in red.items()
                        if k != "reference_timestamp"},
        "active_customers_last_month": {k: str(v) for k, v in last.items()},
        "campaigns": camp[["campaign_id", "campaign_type", "exposed_customers", "converted_customers",
                           "conversion_rate", "attributed_net_sales"]].astype(str).to_dict("records"),
        "quality": {"checks": int(all_q["checks"]), "checks_passed": int(all_q["checks_passed"]),
                    "pct_checks_passed": float(all_q["pct_checks_passed"]),
                    "quarantined_by_reason": q.sort_values("count", ascending=False).astype(str).to_dict("records"),
                    "incidents": incidents.astype(str).to_dict("records")},
        "ledger_all_detected": all(r["match"] for r in ledger_cmp),
    }
    results = ROOT / "docs" / "results"
    results.mkdir(parents=True, exist_ok=True)
    (results / "gold_snapshot.json").write_text(json.dumps(snapshot, indent=2, ensure_ascii=False), encoding="utf-8")
    (results / "ledger_comparison.json").write_text(json.dumps(ledger_cmp, indent=2, ensure_ascii=False),
                                                    encoding="utf-8")

    figs: list[tuple[str, list]] = []
    months = monthly["month"].astype(str)
    # Página 1 - visão executiva
    f1 = layout(go.Figure([bar(months, monthly["net_sales"], "Faturamento líquido (R$)")]),
                "Faturamento líquido por mês (vendas concluídas: COMPLETED + RETURNED)")
    f1b = layout(go.Figure([bar(months, monthly["avg_ticket"], "Ticket médio (R$)", SERIES[0])]), "Ticket médio por mês (R$)")
    tiles = [("Compras concluídas", fmt_int(total["completed_purchases"])), ("Faturamento líquido", fmt_brl(total["net_sales"])),
             ("Ticket médio", fmt_brl(snapshot["sales"]["avg_ticket"])),
             (f"Clientes compradores ({str(last['month'])[:7]})", fmt_int(last["active_buyers"])),
             ("Pontos creditados", fmt_int(pts["points_credited"])),
             ("Créditos no prazo", f"{100 * float(pts['on_time_credit_rate']):.1f}%".replace(".", ",")),
             ("Resgates concluídos", fmt_int(red["completed"])),
             ("Verificações de qualidade aprovadas", f"{float(all_q['pct_checks_passed']):.1f}%".replace(".", ","))]
    figs.append(("1. Visão executiva", [f1, f1b]))
    # Página 2 - operação de pontos
    order = ["COMPLETED_ON_TIME", "COMPLETED_LATE", "PENDING_WITHIN_SLA", "PENDING_OVERDUE", "NOT_APPLICABLE"]
    sla_tot = sla.groupby("sla_category")["credits"].sum().reindex(order).fillna(0)
    f2 = layout(go.Figure([bar(sla_tot.index, sla_tot.values, "Créditos")]), "Créditos de pontos por categoria de prazo")
    types = ptype[ptype["status"] == "COMPLETED"].groupby("transaction_type")["points_abs"].sum()
    f2b = layout(go.Figure([bar(types.index, types.values, "Pontos (concluídos)")]),
                 "Pontos movimentados por tipo (somente concluídos)")
    bl = backlog[backlog["issue_group"].isin(["POINTS", "RECONCILIATION"])].sort_values("open_items")
    f2c = layout(go.Figure([go.Bar(x=bl["open_items"], y=bl["issue_type"], orientation="h", name="Itens abertos",
                                   marker=dict(color=SERIES[1], line=dict(color=SURFACE, width=2)),
                                   hovertemplate="%{y}: %{x}<extra></extra>")]),
                 "Pendências e divergências de pontos (itens abertos)", 380)
    figs.append(("2. Operação de pontos", [f2, f2b, f2c]))
    # Página 3 - campanhas
    f3 = layout(go.Figure([bar(camp["campaign_id"], camp["conversion_rate"] * 100, "Conversão (%)")]),
                "Taxa de conversão por campanha (clientes únicos convertidos / expostos, %)")
    f3b = layout(go.Figure([bar(camp["campaign_id"], camp["attributed_net_sales"], "Faturamento atribuído (R$)")]),
                 "Faturamento líquido atribuído por campanha (ROI não calculado: sem custos)")
    figs.append(("3. Campanhas", [f3, f3b]))
    # Página 4 - clientes
    am = active["month"].astype(str)
    f4 = go.Figure()
    for i, (col, label) in enumerate([("active_buyers", "Compradores"), ("active_points_earners", "Receberam pontos"),
                                      ("active_redeemers", "Resgataram"), ("active_campaign_interactors", "Interagiram c/ campanha")]):
        f4.add_trace(go.Scatter(x=am, y=active[col], name=label, mode="lines+markers",
                                line=dict(color=SERIES[i], width=2), marker=dict(size=8)))
    layout(f4, "Clientes ativos por tipo de atividade (cada linha é um critério diferente)")
    f4.update_layout(hovermode="x unified")
    pivot = cohort.pivot(index="cohort_month", columns="months_since_first", values="retention_rate").sort_index()
    f4b = layout(go.Figure(go.Heatmap(z=pivot.values * 100, x=[f"M+{c}" for c in pivot.columns],
                                      y=[str(i)[:7] for i in pivot.index], colorscale=[[i / 6, c] for i, c in enumerate(BLUE_RAMP)],
                                      hovertemplate="coorte %{y}, %{x}: %{z:.1f}%<extra></extra>",
                                      colorbar=dict(title="%"), xgap=2, ygap=2)),
                 "Retenção por coorte de primeira compra (%)", 380)
    segt = seg.groupby("customer_segment")[["net_sales", "completed_purchases"]].sum()
    f4c = layout(go.Figure([bar(segt.index, segt["net_sales"] / segt["completed_purchases"], "Ticket médio (R$)")]),
                 "Ticket médio por segmento (R$)")
    figs.append(("4. Clientes", [f4, f4b, f4c]))
    # Página 5 - qualidade
    qq = q.groupby("primary_reason")["count"].sum().sort_values()
    f5 = layout(go.Figure([go.Bar(x=qq.values, y=qq.index, orientation="h", name="Registros",
                                  marker=dict(color=SERIES[0], line=dict(color=SURFACE, width=2)),
                                  hovertemplate="%{y}: %{x}<extra></extra>")]),
                "Registros em quarentena por motivo principal", 460)
    inc = incidents.groupby("category")["count"].sum().sort_values()
    f5b = layout(go.Figure([go.Bar(x=inc.values, y=inc.index, orientation="h", name="Incidentes",
                                   marker=dict(color=SERIES[1], line=dict(color=SURFACE, width=2)),
                                   hovertemplate="%{y}: %{x}<extra></extra>")]), "Incidentes por categoria (última execução)")
    f5c = layout(go.Figure([bar(runs["run_id"].str[4:19], runs["sum(duration_seconds)"], "Duração (s)")]),
                 "Histórico de execuções: duração somada das etapas (s)")
    figs.append(("5. Qualidade dos dados", [f5, f5b, f5c]))

    html = [f"""<!doctype html><html lang="pt-BR"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1"><title>Loyalty Lakehouse Dashboard</title>
<style>body{{font-family:Inter,Segoe UI,sans-serif;background:#f9f9f7;color:{INK};margin:0;padding:16px}}
main{{max-width:1100px;margin:auto}} h1{{font-size:22px}} h2{{font-size:18px;margin-top:36px}}
.note{{color:{INK2};font-size:13px}} .tiles{{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px}}
.tile{{background:{SURFACE};border:1px solid {GRID};border-radius:8px;padding:12px}}
.tile .v{{font-size:24px;font-weight:600}} .tile .l{{color:{INK2};font-size:12px}}
.chart{{background:{SURFACE};border:1px solid {GRID};border-radius:8px;margin:12px 0;padding:4px}}</style></head><body><main>
<h1>Loyalty Commerce Lakehouse - dashboard</h1>
<p class="note">Gerado a partir das tabelas Gold reais (execução <code>{run_id}</code>, ambiente {settings.environment}).
Dados 100% sintéticos. Data de referência: {settings.rules['reference_timestamp']}.</p>
<h2>1. Visão executiva</h2><div class="tiles">"""]
    html += [f'<div class="tile"><div class="v">{v}</div><div class="l">{label}</div></div>' for label, v in tiles]
    html.append("</div>")
    first = True
    for section, fs in figs:
        if not section.startswith("1."):
            html.append(f"<h2>{section}</h2>")
        for f in fs:
            html.append('<div class="chart">' + f.to_html(full_html=False, include_plotlyjs="cdn" if first else False,
                                                           config={"displaylogo": False, "responsive": True}) + "</div>")
            first = False
    html.append("</main></body></html>")
    out = ROOT / a.output
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(html), encoding="utf-8")
    print("dashboard:", out, "| snapshot:", results / "gold_snapshot.json",
          "| ledger 100% detectado:", snapshot["ledger_all_detected"])


if __name__ == "__main__":
    main()
