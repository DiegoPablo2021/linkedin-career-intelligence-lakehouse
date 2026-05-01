select
    *,

    {{ lower_trim_text('first_name') }} as first_name_clean,
    {{ lower_trim_text('last_name') }} as last_name_clean,

    {{ lower_trim_text("coalesce(industry, '')") }} as industry_clean,

    case
        when websites like '[PERSONAL:%'
            then regexp_extract(websites, '\\[PERSONAL:(.+)\\]', 1)
        else {{ nullif_trim_text('websites') }}
    end as primary_contact_url,

    case
        when websites like '[PERSONAL:%' then 'WhatsApp'
        else 'Website'
    end as primary_contact_label,

    'https://diego-pablo.vercel.app/' as portfolio_website,

    case
        when lower(headline) like '%analista%' then 'analista'
        when lower(headline) like '%engenheiro%' then 'engenharia'
        when lower(headline) like '%developer%' then 'desenvolvimento'
        else 'outros'
    end as profile_track,

    length(summary) as summary_length,

    case
        when summary is null then 'sem_summary'
        when length(summary) < 100 then 'curta'
        when length(summary) < 500 then 'media'
        else 'longa'
    end as summary_size_category

from {{ ref('stg_profile') }}
