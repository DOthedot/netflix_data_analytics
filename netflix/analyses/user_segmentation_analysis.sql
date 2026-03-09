-- User segment breakdown — how many users in each engagement tier?
select
    user_segment,
    count(*)                            as user_count,
    round(avg(total_ratings), 1)        as avg_ratings_per_user,
    round(avg(avg_rating_given), 3)     as avg_rating_tendency,
    round(avg(total_tags_applied), 1)   as avg_tags_per_user
from {{ ref('mart_user_activity') }}
group by user_segment
order by
    case user_segment
        when 'power'   then 1
        when 'regular' then 2
        when 'casual'  then 3
        when 'light'   then 4
    end
