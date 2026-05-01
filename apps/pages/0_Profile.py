import pandas as pd
import streamlit as st

from linkedin_career_intelligence.streamlit_utils import (
    apply_app_theme,
    is_demo_mode,
    render_card,
    render_question,
    run_query,
    ui_text,
)


st.set_page_config(page_title="Profile", layout="wide")
apply_app_theme()

LOCAL_DISPLAY_LOCATION = "RN"
DEMO_DISPLAY_LOCATION = "Brasil"
PUBLIC_PORTFOLIO_URL = "https://diego-pablo.vercel.app/"
PUBLIC_GITHUB_URL = "https://github.com/DiegoPablo2021/"
PUBLIC_LINKEDIN_URL = "https://www.linkedin.com/in/diego-pablo/"
PUBLIC_EMAIL = "diegopmenezes@hotmail.com"


@st.cache_data
def load_profile_summary() -> pd.DataFrame:
    query = """
        select
            first_name,
            last_name,
            headline,
            industry,
            geo_location,
            profile_track,
            summary_length,
            summary_size_category,
            primary_contact_url,
            primary_contact_label,
            portfolio_website
        from main.mart_profile_summary
    """
    return run_query(query)


@st.cache_data
def load_profile_detail() -> pd.DataFrame:
    query = """
        select
            first_name,
            last_name,
            maiden_name,
            address,
            birth_date,
            headline,
            summary,
            industry,
            zip_code,
            geo_location,
            twitter_handles,
            websites,
            primary_contact_url,
            primary_contact_label,
            portfolio_website,
            instant_messengers,
            profile_track,
            summary_length,
            summary_size_category
        from main.int_profile_enriched
    """
    return run_query(query)


def format_title_case(value: str) -> str:
    if value is None:
        return ""
    return str(value).strip().title()


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


st.title(ui_text("Profile", "Profile"))
st.subheader(ui_text("Leitura executiva do posicionamento principal no LinkedIn", "Executive reading of the main LinkedIn positioning"))

df_summary = load_profile_summary()
df_detail = load_profile_detail()

if not df_summary.empty:
    df_summary["first_name"] = df_summary["first_name"].fillna("").apply(format_title_case)
    df_summary["last_name"] = df_summary["last_name"].fillna("").apply(format_title_case)
    df_summary["industry"] = df_summary["industry"].fillna("").apply(format_title_case)
    df_summary["geo_location"] = df_summary["geo_location"].fillna("").apply(format_title_case)
    df_summary["profile_track"] = df_summary["profile_track"].apply(format_profile_track)
    df_summary["summary_size_category"] = df_summary["summary_size_category"].apply(format_summary_category)

if not df_detail.empty:
    df_detail["first_name"] = df_detail["first_name"].fillna("").apply(format_title_case)
    df_detail["last_name"] = df_detail["last_name"].fillna("").apply(format_title_case)
    df_detail["maiden_name"] = df_detail["maiden_name"].fillna("").apply(format_title_case)
    df_detail["industry"] = df_detail["industry"].fillna("").apply(format_title_case)
    df_detail["geo_location"] = df_detail["geo_location"].fillna("").apply(format_title_case)
    df_detail["profile_track"] = df_detail["profile_track"].apply(format_profile_track)
    df_detail["summary_size_category"] = df_detail["summary_size_category"].apply(format_summary_category)
    df_detail["birth_date"] = pd.to_datetime(df_detail["birth_date"], errors="coerce")

if not df_summary.empty:
    profile_row = df_summary.iloc[0]
    full_name = f"{profile_row['first_name']} {profile_row['last_name']}".strip()
    headline = str(profile_row["headline"]) if pd.notna(profile_row["headline"]) else ""
    industry = str(profile_row["industry"]) if pd.notna(profile_row["industry"]) else ""
    geo_location = str(profile_row["geo_location"]) if pd.notna(profile_row["geo_location"]) else ""
    profile_track = str(profile_row["profile_track"]) if pd.notna(profile_row["profile_track"]) else ""
    summary_length = int(profile_row["summary_length"]) if pd.notna(profile_row["summary_length"]) else 0
    summary_size_category = str(profile_row["summary_size_category"]) if pd.notna(profile_row["summary_size_category"]) else ""
    primary_contact_url = str(profile_row["primary_contact_url"]) if pd.notna(profile_row["primary_contact_url"]) else ""
    primary_contact_label = str(profile_row["primary_contact_label"]) if pd.notna(profile_row["primary_contact_label"]) else "Contato"
    portfolio_website = str(profile_row["portfolio_website"]) if pd.notna(profile_row["portfolio_website"]) else ""
else:
    full_name = ""
    headline = ""
    industry = ""
    geo_location = ""
    profile_track = ""
    summary_length = 0
    summary_size_category = ""
    primary_contact_url = ""
    primary_contact_label = "Contato"
    portfolio_website = ""

display_location = DEMO_DISPLAY_LOCATION if is_demo_mode() else LOCAL_DISPLAY_LOCATION
display_website = portfolio_website if portfolio_website else PUBLIC_PORTFOLIO_URL
show_primary_contact = bool(
    not is_demo_mode()
    and primary_contact_url
    and primary_contact_label.strip().lower() not in {"website"}
)

hero_name = full_name if full_name else "Perfil principal"
st.markdown(
    f"""
    <div class="cci-hero">
        <div class="cci-card-title">{hero_name}</div>
        <div class="cci-card-body">{headline if headline else "Headline principal indisponível no momento."}</div>
    </div>
    """,
    unsafe_allow_html=True,
)

c1, c2, c3, c4 = st.columns(4)
with c1:
    render_card(ui_text("Nome do perfil", "Profile name"), full_name if full_name else "-", ui_text("Identidade principal capturada no export.", "Main identity captured from the export."), tone="blue")
with c2:
    render_card(ui_text("Trilha inferida", "Inferred track"), profile_track if profile_track else "-", ui_text("Leitura resumida do eixo profissional mais evidente.", "Summarized reading of the strongest professional axis."), tone="teal")
with c3:
    render_card(ui_text("Força do summary", "Summary strength"), str(summary_length), ui_text("Quantidade de caracteres usados para sustentar a narrativa profissional.", "Character count used to support the professional narrative."), tone="violet")
with c4:
    render_card(ui_text("Categoria do summary", "Summary category"), summary_size_category if summary_size_category else "-", ui_text("Classificação textual da profundidade do resumo.", "Textual classification of summary depth."), tone="gold")

st.markdown(f"## {ui_text('Perguntas de negócio', 'Business questions')}")
q1, q2 = st.columns(2)
with q1:
    render_question(
        ui_text("Como o perfil se posiciona profissionalmente?", "How does the profile position itself professionally?"),
        ui_text(f"A trilha predominante é {profile_track.lower()}.", f"The dominant track is {profile_track.lower()}.") if profile_track else ui_text("Ainda sem trilha inferida.", "No inferred track yet."),
        ui_text("Essa leitura ajuda a conectar headline, experiência e sinais analíticos em uma narrativa única.", "This helps connect headline, experience and analytical signs into one narrative."),
    )
    render_question(
        ui_text("O summary sustenta uma proposta de valor clara?", "Does the summary sustain a clear value proposition?"),
        ui_text(f"Sim. O summary foi classificado como {summary_size_category.lower()}.", f"Yes. The summary was classified as {summary_size_category.lower()}.") if summary_size_category else ui_text("Ainda sem classificação disponível.", "No classification available yet."),
        ui_text(f"O texto principal tem {summary_length} caracteres, sugerindo espaço relevante para contexto, tecnologias e entregas.", f"The main text has {summary_length} characters, suggesting relevant room for context, technologies and outcomes."),
    )
with q2:
    render_question(
        ui_text("Qual contexto de mercado aparece no perfil?", "What market context appears in the profile?"),
        industry if industry else "Indústria não informada.",
        ui_text(f"Localização principal: {display_location}", f"Main location: {display_location}"),
    )
    render_question(
        ui_text("Existe ativo externo para aprofundar a marca pessoal?", "Is there any external asset that extends the personal brand?"),
        ui_text("Sim, há presença de canal externo registrado.", "Yes, there is an external channel registered.") if display_website or show_primary_contact else ui_text("Não há link externo destacado.", "There is no highlighted external link."),
        display_website if display_website else primary_contact_url,
    )

st.divider()
st.markdown(f"## {ui_text('Visão principal do perfil', 'Main profile view')}")

left, right = st.columns([2, 1])
with left:
    st.markdown(f"### {ui_text('Headline', 'Headline')}")
    st.info(headline if headline else ui_text("Sem headline cadastrada.", "No headline available."))

    st.markdown(f"### {ui_text('Summary', 'Summary')}")
    if not df_detail.empty and pd.notna(df_detail.iloc[0]["summary"]):
        st.write(df_detail.iloc[0]["summary"])
    else:
        st.write(ui_text("Sem summary disponível.", "No summary available."))

with right:
    st.markdown(f"### {ui_text('Contexto do perfil', 'Profile context')}")
    st.write(f"**Industry:** {industry if industry else '-'}")
    st.write(f"**{ui_text('Localização', 'Location')}:** {display_location}")
    if show_primary_contact:
        st.markdown(f"**{primary_contact_label}:** [{primary_contact_url}]({primary_contact_url})")
    if display_website:
        st.markdown(f"**Website:** [{display_website}]({display_website})")
    if is_demo_mode():
        st.markdown(f"**GitHub:** [github.com/DiegoPablo2021]({PUBLIC_GITHUB_URL})")
        st.caption(ui_text("Contatos diretos ficam ocultos na demo pública.", "Direct contact details stay hidden in the public demo."))
    else:
        st.markdown(f"**GitHub:** [github.com/DiegoPablo2021]({PUBLIC_GITHUB_URL})")
        st.markdown(f"**LinkedIn:** [linkedin.com/in/diego-pablo]({PUBLIC_LINKEDIN_URL})")
        st.markdown(f"**E-mail:** [{PUBLIC_EMAIL}](mailto:{PUBLIC_EMAIL})")

st.divider()
st.markdown(f"## {ui_text('Sinais centrais do perfil', 'Core profile signals')}")

s1, s2, s3 = st.columns(3)
with s1:
    render_card(
        ui_text("Posicionamento inferido", "Inferred positioning"),
        profile_track if profile_track else "-",
        ui_text("A leitura atual aponta para um eixo profissional analítico e técnico, coerente com a headline principal.", "The current reading points to an analytical and technical professional axis, coherent with the main headline."),
        tone="blue",
    )
with s2:
    render_card(
        ui_text("Profundidade narrativa", "Narrative depth"),
        summary_size_category if summary_size_category else "-",
        ui_text(f"O summary soma {summary_length} caracteres e sustenta uma proposta de valor mais completa.", f"The summary has {summary_length} characters and supports a more complete value proposition."),
        tone="teal",
    )
with s3:
    external_asset = ui_text("Portfólio publicado", "Published portfolio") if portfolio_website else ui_text("Sem ativo destacado", "No highlighted asset")
    render_card(
        ui_text("Ativo externo", "External asset"),
        external_asset,
        ui_text("A presença de um link externo fortalece a continuidade da marca pessoal fora do LinkedIn.", "The presence of an external link strengthens the personal brand beyond LinkedIn."),
        tone="gold",
    )

signal_left, signal_right = st.columns(2)
with signal_left:
    render_question(
        ui_text("O perfil comunica especialização ou generalismo?", "Does the profile communicate specialization or generalism?"),
        ui_text("Especialização com amplitude.", "Specialization with breadth."),
        ui_text("A combinação de trilha inferida, headline técnica e summary longo mostra foco, mas sem parecer restrito a uma única ferramenta.", "The mix of inferred track, technical headline and long summary shows focus without looking limited to a single tool."),
    )
with signal_right:
    render_question(
        ui_text("A narrativa profissional está madura para exposição pública?", "Is the professional narrative mature enough for public exposure?"),
        ui_text("Sim, com boa base para portfólio e GitHub.", "Yes, with a strong base for portfolio and GitHub."),
        ui_text("O conjunto headline + summary + website já sustenta uma apresentação profissional mais forte.", "The combination of headline, summary and website already supports a stronger professional presentation."),
    )

st.divider()
st.markdown(f"## {ui_text('Tabela detalhada', 'Detailed table')}")
st.dataframe(df_detail, width="stretch")
