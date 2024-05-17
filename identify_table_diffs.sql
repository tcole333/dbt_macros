/*
Macro to get column by column comparison for non matching rows between two versions of a table. The total number of rows that dont match within each column are displayed in the
first row.
*/

{% macro idenitfy_table_diffs(a_relation, b_relation, exclude_columns=[], primary_key=[]) %}

{% set column_names = dbt_utils.get_filtered_columns_in_relation(from=a_relation, except=exclude_columns) %}

with a as (
    select
        {{ dbt_utils.surrogate_key(primary_key) }} as primary_key,
        {{ dbt_utils.surrogate_key(column_names) }} as column_hash,
        {% for column_name in column_names %}
        {{ adapter.quote(column_name) }} as a_{{ column_name }}
        {% if not loop.last %}, {% endif %}
        {% endfor %}
    from {{ a_relation }}
),
b as (
    select
        {{ dbt_utils.surrogate_key(primary_key) }} as primary_key,
        {{ dbt_utils.surrogate_key(column_names) }} as column_hash,
        {% for column_name in column_names %}
        {{ adapter.quote(column_name) }} as b_{{ column_name }}
        {% if not loop.last %}, {% endif %}
        {% endfor %}
    from {{ b_relation }}
),
comparison as (
    select
        a.primary_key,
        {% for column_name in column_names %}
        case when a.a_{{ column_name }} <> b.b_{{ column_name }} then a.a_{{ column_name }} else null end as a_{{ column_name }},
        case when a.a_{{ column_name }} <> b.b_{{ column_name }} then b.b_{{ column_name }} else null end as b_{{ column_name }}
        {% if not loop.last %}, {% endif %}
        {% endfor %}
    from a
    join b on a.primary_key = b.primary_key
    where a.column_hash <> b.column_hash
),
diffs as (
select
    primary_key,
    {% for column_name in column_names %}
    cast(a_{{ column_name }} as string) as a_{{ column_name }},
    cast(b_{{ column_name }} as string) as b_{{ column_name }}
    {% if not loop.last %}, {% endif %}
    {% endfor %}
from comparison
where {% for column_name in column_names %} a_{{ column_name }} is not null or b_{{ column_name }} is not null {% if not loop.last %} or {% endif %} {% endfor %}
),
row_counts as (
    select
        'primary_key' as primary_key,
        {% for column_name in column_names %}
        cast(count(a_{{ column_name }}) as string) as  a_{{ column_name }},
        cast(count(b_{{ column_name }}) as string) as  b_{{ column_name }}
        {% if not loop.last %}, {% endif %}
        {% endfor %}
    from diffs
    group by 1
),
final as (
    select 
    *,
    1 as sort_key
    from row_counts
    union all 
    select 
    *,
    2 as sort_key 
    from diffs
)
select * from final order by sort_key
{% endmacro %}
