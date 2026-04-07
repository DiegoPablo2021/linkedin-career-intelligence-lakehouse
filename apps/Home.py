import pandas as pd
import streamlit as st

from linkedin_career_intelligence.streamlit_utils import (
    apply_app_theme,
    render_card,
    render_question,
    run_query,
    ui_text,
)


st.set_page_config(page_title="LinkedIn Career Intelligence", layout="wide")
apply_app_theme()


@st.cache_data
def load_home_summary() -> pd.DataFrame:
    return run_query("select * from main.mart_pipeline_health_summary")


@st.cache_data
def load_connections_overview() -> pd.DataFrame:
    return run_query(
        """
        select
            sum(total_connections) as total_connections,
            sum(connections_with_email) as connections_with_email,
            max(unique_companies) as unique_companies,
            max(unique_positions) as unique_positions
        from main.mart_connections_summary
        """
    )


@st.cache_data
def load_profile_overview() -> pd.DataFrame:
    return run_query(
        """
        select
            first_name,
            last_name,
            profile_track,
            summary_size_category,
            summary_length
        from main.mart_profile_summary
        """
    )


def format_profile_track(value: str) -> str:
    mapping = {
        "analista": "Analista",
        "engenharia": "Engenharia",
        "desenvolvimento": "Desenvolvimento",
        "outros": "Outros",
    }
    return mapping.get(str(value), str(value).title())


def format_summary_category(value: str) -> str:
    mapping = {
        "sem_summary": "Sem summary",
        "curta": "Curta",
        "media": "Média",
        "longa": "Longa",
    }
    return mapping.get(str(value), str(value).title())


df_home = load_home_summary()
df_connections = load_connections_overview()
df_profile = load_profile_overview()

st.title(ui_text("LinkedIn Career Intelligence", "LinkedIn Career Intelligence"))
st.markdown(
    f"""
    <div class="cci-hero">
        {ui_text(
            "Plataforma analítica construída sobre o export do LinkedIn para transformar histórico profissional, networking, aprendizado e candidaturas em leitura executiva com pipeline reproduzível.",
            "Analytics platform built on top of LinkedIn exports to turn professional history, networking, learning and applications into an executive view with a reproducible pipeline."
        )}
    </div>
    """,
    unsafe_allow_html=True,
)

if not df_home.empty:
    home = df_home.iloc[0]
    success_rate = 0.0
    if int(home["total_inventory_files"]) > 0:
        success_rate = (float(home["successful_reads"]) / float(home["total_inventory_files"])) * 100

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        render_card(
            ui_text("Pipeline confiável", "Reliable pipeline"),
            f"{success_rate:.0f}%",
            ui_text("Leitura operacional concluída sem falhas no inventário atual.", "Operational reading completed with no failures in the current inventory."),
            tone="blue",
        )
    with c2:
        render_card(
            ui_text("Escala da rede", "Network scale"),
            f"{int(home['total_connections']):,}".replace(",", "."),
            ui_text("Base robusta de conexões para leitura de networking e posicionamento.", "Robust connection base for networking and positioning analysis."),
            tone="teal",
        )
    with c3:
        render_card(
            ui_text("Capacidade demonstrada", "Demonstrated capability"),
            f"{int(home['total_skills'])}",
            ui_text("Skills já tratadas no pipeline para leitura de competências sinalizadas.", "Skills already processed by the pipeline for capability analysis."),
            tone="violet",
        )
    with c4:
        render_card(
            ui_text("Movimento de carreira", "Career movement"),
            f"{int(home['total_job_applications'])}",
            ui_text("Candidaturas carregadas para analisar interesse, volume e foco profissional.", "Applications loaded to analyze intent, volume and professional focus."),
            tone="gold",
        )

if not df_connections.empty and not df_profile.empty and not df_home.empty:
    connections = df_connections.iloc[0]
    profile = df_profile.iloc[0]
    home = df_home.iloc[0]

    total_connections = int(connections["total_connections"] or 0)
    email_connections = int(connections["connections_with_email"] or 0)
    unique_companies = int(connections["unique_companies"] or 0)
    unique_positions = int(connections["unique_positions"] or 0)
    email_share = round((email_connections / total_connections) * 100, 1) if total_connections else 0.0
    profile_track = format_profile_track(profile["profile_track"])
    summary_category = format_summary_category(profile["summary_size_category"])
    summary_length = int(profile["summary_length"] or 0)
    learning_total = int(home["total_learning_records"] or 0)
    invitations_total = int(home["total_invitations"] or 0)

    st.markdown(f"## {ui_text('Perguntas de negócio', 'Business questions')}")
    q1, q2 = st.columns(2)
    with q1:
        render_question(
            ui_text("Que tipo de presença profissional o perfil comunica?", "What kind of professional presence does the profile communicate?"),
            ui_text(
                f"Perfil orientado à trilha de {profile_track.lower()} com summary {summary_category.lower()}.",
                f"Profile oriented toward the {profile_track.lower()} track with a {summary_category.lower()} summary."
            ),
            ui_text(
                f"O resumo principal tem {summary_length} caracteres e sustenta um posicionamento mais completo.",
                f"The main summary has {summary_length} characters and supports a stronger positioning."
            ),
        )
        render_question(
            ui_text("A rede é ampla ou concentrada?", "Is the network broad or concentrated?"),
            ui_text(
                f"A rede combina escala com diversidade: {total_connections:,} conexões, {unique_companies} empresas e {unique_positions} cargos.".replace(",", "."),
                f"The network combines scale and diversity: {total_connections:,} connections, {unique_companies} companies and {unique_positions} roles.".replace(",", "."),
            ),
            ui_text("Isso sugere amplitude relacional e exposição a múltiplos contextos profissionais.", "This suggests relational breadth and exposure to multiple professional contexts."),
        )
    with q2:
        render_question(
            ui_text("Quanto da rede tem canal de contato direto?", "How much of the network has a direct contact channel?"),
            ui_text(f"{email_share}% das conexões contam com e-mail registrado.", f"{email_share}% of the connections have an email registered."),
            ui_text(
                f"São {email_connections:,} contatos com e-mail mapeado para possíveis ações segmentadas.".replace(",", "."),
                f"That means {email_connections:,} contacts with mapped email for possible segmented actions.".replace(",", "."),
            ),
        )
        render_question(
            ui_text("O histórico mostra intenção contínua de evolução?", "Does the history show continuous intent to evolve?"),
            ui_text(f"Sim: {learning_total} registros de learning e {invitations_total} convites processados.", f"Yes: {learning_total} learning records and {invitations_total} processed invitations."),
            ui_text("O conjunto reforça sinais de atualização constante e presença ativa no ecossistema da plataforma.", "Together they reinforce signs of continuous learning and active presence in the platform ecosystem."),
        )

st.markdown(f"## {ui_text('Cobertura analítica', 'Analytical coverage')}")
c5, c6, c7 = st.columns(3)
with c5:
    render_card(
        ui_text("Domínios entregues", "Domains delivered"),
        "14+",
        ui_text("Connections, Career, Education, Certifications, Languages, Profile, Skills, Engagement, Learning, Jobs e frentes operacionais.", "Connections, Career, Education, Certifications, Languages, Profile, Skills, Engagement, Learning, Jobs and operational layers."),
        tone="slate",
    )
with c6:
    render_card(
        ui_text("Stack do case", "Project stack"),
        "Python + dbt",
        ui_text("Ingestão, padronização, enriquecimento analítico, testes e consumo em app dark com Streamlit.", "Ingestion, standardization, analytical enrichment, tests and consumption in a Streamlit app."),
        tone="blue",
    )
with c7:
    render_card(
        ui_text("Uso do app", "App usage"),
        "Storytelling + drilldown",
        ui_text("A navegação foi desenhada para começar pelo contexto executivo e avançar para páginas por domínio.", "Navigation was designed to start from executive context and move into domain pages."),
        tone="teal",
    )

st.markdown(f"## {ui_text('Navegação recomendada', 'Recommended navigation')}")
st.markdown(
    ui_text(
        """
        - **Profile**: leitura do posicionamento principal, headline, summary e contexto profissional.
        - **Health**: saúde operacional do pipeline e cobertura dos dados processados.
        - **Connections / Career**: rede, trajetória e sinais de movimentação profissional.
        - **Education / Certifications / Languages / Skills**: capital formativo e competências.
        - **Engagement / Learning / Jobs / Recommendations**: atividade, aprendizado e prova social.
        """,
        """
        - **Profile**: main positioning, headline, summary and professional context.
        - **Health**: pipeline health and processed data coverage.
        - **Connections / Career**: network, trajectory and movement signals.
        - **Education / Certifications / Languages / Skills**: education capital and capabilities.
        - **Engagement / Learning / Jobs / Recommendations**: activity, learning and social proof.
        """
    )
)

st.markdown(f"## {ui_text('Arquitetura resumida', 'Architecture summary')}")
st.code(
    """
RAW CSV (LinkedIn)
-> Python ingestion
-> DuckDB local
-> dbt staging / intermediate / marts
-> Streamlit analytics app
-> GitHub + portfolio publishing
    """.strip(),
    language="text",
)
