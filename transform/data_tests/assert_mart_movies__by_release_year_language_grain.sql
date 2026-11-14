-- Grain du mart : au plus une ligne par (release_year, original_language),
-- la valeur nulle formant un groupe (group by la traite comme une valeur).
select release_year, original_language, count(*) as rows_per_group
from {{ ref('mart_movies__by_release_year_language') }}
group by release_year, original_language
having count(*) > 1
