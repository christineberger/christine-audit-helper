{% macro compare_queries(a_query, b_query, primary_key=None, mode='summary', return_all=false, limit=None) -%}
  {{ return(adapter.dispatch('compare_queries', 'audit_helper')(a_query, b_query, primary_key, summarize, limit)) }}
{%- endmacro %}

{% macro default__compare_queries(a_query, b_query, primary_key=None, mode='summary', return_all=false, limit=None) %}
-- Modes:
    -- summary (same as before)
    -- details (same as before)
    -- text_summary (summary as text, not table)

with a as (

    {{ a_query }}

),

b as (

    {{ b_query }}

),

a_intersect_b as (

    select * from a
    {{ dbt.intersect() }}
    select * from b

),

a_except_b as (

    select * from a
    {{ dbt.except() }}
    select * from b

),

b_except_a as (

    select * from b
    {{ dbt.except() }}
    select * from a

),

all_records as (

    select
        *,
        true as in_a,
        true as in_b
    from a_intersect_b

    union all

    select
        *,
        true as in_a,
        false as in_b
    from a_except_b

    union all

    select
        *,
        false as in_a,
        true as in_b
    from b_except_a

),

{%- if mode in ('summary', 'text_summary') %}

summary_count as (
    select 
        in_a, 
        in_b, 
        count(*) as count 
    from all_records 
    group by 1, 2
),

summary_perc as (
    select
        *,
        round(100.0 * count / sum(count) over (), 2) as percent_of_total
    from summary_stats
    order by in_a desc, in_b desc
),

{%- if mode == 'summary' %}

final as (select * from summary_perc)

{%- else %}

final as (
    select
        'Perfect matches: ' 
        || coalesce(min(case when in_a and in_b then percent_of_total end), 0) || '% ('
        || coalesce(min(case when in_a and in_b then count end), 0) || ')\n'
        || '{{ a_alias }} diffs: '
        || coalesce(min(case when in_a and not in_b then percent_of_total end), 0) || '% ('
        || coalesce(min(case when in_a and not in_b then count end), 0) || ')\n'
        || '{{ b_alias }} diffs: '
        || coalesce(min(case when not in_a and in_b then percent_of_total end), 0) || '% ('
        || coalesce(min(case when not in_a and in_b then count end), 0) || ')'
        as text_summary
    from summary_perc
)
{%- endif %}

{%- else %}

final as (
    
    select * from all_records
    {% if return_all == false %}where not (in_a and in_b){%- endif %}
    order by {{ primary_key ~ ", " if primary_key is not none }} in_a desc, in_b desc

)

{%- endif %}

select * from final
{%- if limit and not summarize %}
limit {{ limit }}
{%- endif %}


{% endmacro %}
