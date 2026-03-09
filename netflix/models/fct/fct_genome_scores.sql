{{
    config(materialized = 'table')
}}

-- Genome relevance scores: static dataset, full-refresh table
with src_scores as (
    select * from {{ ref('src_genome_score') }}
)
select
    movie_id,
    tag_id,
    round(relevance, 4)                             as relevance_score,
    case
        when relevance >= 0.8 then 'very high'
        when relevance >= 0.5 then 'high'
        when relevance >= 0.2 then 'medium'
        else 'low'
    end                                             as relevance_tier
from src_scores
where relevance > 0
