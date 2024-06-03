
{% macro compare_queries_by_time(a_query, b_query, datetime_column, timepart, primary_key=primary_key, a_alias='a', b_alias='b') %}
-- Built for Databricks SQL
with

details as (
    {{ audit_helper.compare_queries(a_query, b_query, primary_key=primary_key, summarize=false, return_all=true) }}
),

timepart_agg as (
    select
        to_date(date_trunc('{{ timepart }}', datetime_column)) as date_{{ timepart }},
        in_a,
        in_b,
        count(*) as num_records
    from details
    group by 1, 2, 3
),

final as (
    select
        *,
        round(100.0 * num_records / sum(num_records) over (partition by date_{{ timepart }}), 2) as percent_of_total
    from summary_stats
    order by in_a desc, in_b desc
),

timepart_summary as (
    select
        date_{{ timepart }},
        
        concat(
            coalesce(min(case when in_a and in_b then percent_of_total end), 0), '% ('
            coalesce(min(case when in_a and in_b then count end), 0), ')'
        ) as perfect_matches,
        
        concat(
            coalesce(min(case when in_a and not in_b then percent_of_total end), 0), '% ('
            coalesce(min(case when in_a and not in_b then count end), 0), ')'
        ) as {{ a_alias }}_diffs,

        concat(
            coalesce(min(case when not in_a and in_b then percent_of_total end), 0), '% ('
            coalesce(min(case when not in_a and in_b then count end), 0), ')'
        ) as {{ b_alias }}_diffs
    from timepart_agg
)

select * from timepart_summary

{% endmacro %}