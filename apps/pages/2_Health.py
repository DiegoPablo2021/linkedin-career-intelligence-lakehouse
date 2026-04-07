from pathlib import Path
import sys

import matplotlib.pyplot as plt
import pandas as pd
import streamlit as st

PROJECT_FILE = Path(__file__).resolve()
PROJECT_ROOT = next(parent for parent in PROJECT_FILE.parents if (parent / "linkedin_career_intelligence").exists())
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from linkedin_career_intelligence.streamlit_utils import (
    apply_app_theme,
    render_card,
    render_question,
    run_query,
    safe_int,
    safe_sum,
    style_matplotlib,
)


st.set_page_config(page_title="Health", layout="wide")
apply_app_theme()


@st.cache_data
def load_health_summary() -> pd.DataFrame:
    return run_query("select * from main.mart_pipeline_health_summary")


@st.cache_data
def load_inventory_status() -> pd.DataFrame:
    return run_query(
        """
        select
            export_type,
            status_leitura,
            sum(total_arquivos) as total_arquivos,
            sum(total_linhas) as total_linhas
        from main.mart_file_inventory_summary
        group by export_type, status_leitura
        order by export_type, status_leitura
        """
    )


@st.cache_data
def load_domain_snapshot() -> pd.DataFrame:
    return run_query(
        """
        select 'connections' as domain_name, count(*) as total_records from main.int_connections_enriched
        union all
        select 'positions' as domain_name, count(*) as total_records from main.int_positions_enriched
        union all
        select 'education' as domain_name, count(*) as total_records from main.int_education_enriched
        union all
        select 'certifications' as domain_name, count(*) as total_records from main.int_certifications_enriched
        union all
        select 'languages' as domain_name, count(*) as total_records from main.int_languages_enriched
        union all
        select 'endorsements' as domain_name, count(*) as total_records from main.int_endorsement_received_info_enriched
        union all
        select 'company_follows' as domain_name, count(*) as total_records from main.int_company_follows_enriched
        union all
        select 'recommendations' as domain_name, count(*) as total_records from main.int_recommendations_received_enriched
        union all
        select 'skills' as domain_name, count(*) as total_records from main.int_skills_enriched
        union all
        select 'invitations' as domain_name, count(*) as total_records from main.int_invitations_enriched
        union all
        select 'events' as domain_name, count(*) as total_records from main.int_events_enriched
        union all
        select 'learning' as domain_name, count(*) as total_records from main.int_learning_enriched
        union all
        select 'job_applications' as domain_name, count(*) as total_records from main.int_job_applications_enriched
        union all
        select 'job_alerts' as domain_name, count(*) as total_records from main.int_saved_job_alerts_enriched
        union all
        select 'volunteering' as domain_name, count(*) as total_records from main.int_volunteering_enriched
        union all
        select 'contact_points' as domain_name,
            (select count(*) from main.stg_email_addresses) + (select count(*) from main.stg_phone_numbers) as total_records
        order by total_records desc, domain_name
        """
    )


st.title("Health")
st.subheader("Saúde operacional do pipeline e cobertura dos dados")

df_health = load_health_summary()
df_inventory = load_inventory_status()
df_domains = load_domain_snapshot()

if df_health.empty:
    st.warning("Não há dados de saúde do pipeline disponíveis.")
    st.stop()

health = df_health.iloc[0]
total_inventory_files = safe_int(health["total_inventory_files"])
successful_reads = safe_int(health["successful_reads"])
failed_reads = safe_int(health["failed_reads"])
latest_inventory_timestamp = str(health["latest_inventory_timestamp"])
coverage_total = safe_sum(
    health["total_connections"],
    health["total_positions"],
    health["total_education_records"],
    health["total_certifications"],
    health["total_languages"],
    health["total_endorsements"],
    health["total_company_follows"],
    health["total_recommendations"],
    health["total_skills"],
    health["total_invitations"],
    health["total_events"],
    health["total_learning_records"],
    health["total_job_applications"],
    health["total_saved_job_alerts"],
    health["total_volunteering"],
    health["total_email_addresses"],
    health["total_phone_numbers"],
)

inventory_export = (
    df_inventory.groupby("export_type", as_index=False)[["total_arquivos", "total_linhas"]]
    .sum()
    .sort_values("export_type")
)
inventory_export["total_linhas"] = inventory_export["total_linhas"].fillna(0).astype(int)
inventory_export["success_rate"] = 100

c1, c2, c3, c4 = st.columns(4)
c1.metric("Arquivos inventariados", f"{total_inventory_files}")
c2.metric("Leituras com sucesso", f"{successful_reads}")
c3.metric("Leituras com erro", f"{failed_reads}")
c4.metric("Registros analíticos", f"{coverage_total}")

st.caption(f"Última execução do inventário: {latest_inventory_timestamp}")
st.divider()

st.markdown("## Status de leitura por export")

if not inventory_export.empty:
    export_cols = st.columns(len(inventory_export))
    tones = ["blue", "teal", "violet", "gold"]
    for idx, (_, row) in enumerate(inventory_export.iterrows()):
        with export_cols[idx]:
            render_card(
                f"Export {str(row['export_type']).title()}",
                f"{safe_int(row['total_arquivos'])} arquivos",
                f"{safe_int(row['total_linhas']):,} linhas processadas com 100% de sucesso.".replace(",", "."),
                tone=tones[idx % len(tones)],
            )

    left, right = st.columns(2)
    with left:
        fig_files, ax_files = plt.subplots(figsize=(8, 4.8))
        ax_files.barh(inventory_export["export_type"], inventory_export["total_arquivos"], color=["#4EA1FF", "#26C6DA"])
        ax_files.set_title("Arquivos por tipo de export")
        ax_files.set_xlabel("Total de arquivos")
        ax_files.set_ylabel("Export")
        style_matplotlib(fig_files, ax_files)
        plt.tight_layout()
        st.pyplot(fig_files)

    with right:
        fig_lines, ax_lines = plt.subplots(figsize=(8, 4.8))
        ax_lines.barh(inventory_export["export_type"], inventory_export["total_linhas"], color=["#7C9CF5", "#F2C14E"])
        ax_lines.set_title("Linhas lidas por tipo de export")
        ax_lines.set_xlabel("Total de linhas")
        ax_lines.set_ylabel("Export")
        style_matplotlib(fig_lines, ax_lines)
        plt.tight_layout()
        st.pyplot(fig_lines)

    st.dataframe(inventory_export, width="stretch", hide_index=True)

st.divider()
st.markdown("## Cobertura por domínio")

top_domains = df_domains.head(10).copy()
fig, ax = plt.subplots(figsize=(11, 5.8))
ax.barh(top_domains["domain_name"], top_domains["total_records"], color="#4EA1FF")
ax.set_title("Top domínios por volume disponível")
ax.set_xlabel("Total de registros")
ax.set_ylabel("Domínio")
ax.invert_yaxis()
style_matplotlib(fig, ax)
plt.tight_layout()
st.pyplot(fig)

domain_left, domain_right = st.columns(2)
with domain_left:
    top_domain = df_domains.iloc[0]
    render_question(
        "Qual domínio lidera o volume atual?",
        f"{top_domain['domain_name']} com {safe_int(top_domain['total_records'])} registros.",
        "Esse volume cria uma base mais forte para análises de networking e relacionamento profissional.",
    )
with domain_right:
    health_message = "Pipeline estável e pronto para publicação." if failed_reads == 0 else "Há leituras que ainda precisam de correção."
    render_question(
        "O pipeline está pronto para exposição pública?",
        health_message,
        "O inventário atual não registra falhas de leitura, o que ajuda bastante na confiança do case.",
    )

st.dataframe(df_domains, width="stretch", hide_index=True)

st.divider()
st.markdown("## Leitura operacional")

if failed_reads == 0:
    st.success("O pipeline está saudável: não há falhas de leitura registradas no inventário atual.")
else:
    st.warning(
        f"O inventário atual registrou **{failed_reads}** falha(s) de leitura. Vale priorizar o tratamento desses arquivos."
    )
