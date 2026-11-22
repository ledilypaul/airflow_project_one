-- Exemple de mart : films de l'état courant par année de sortie et langue
-- originale. Grain : (release_year, original_language), valeurs nulles
-- comprises (un groupe chacune). movie_rows compte les lignes de l'état
-- courant (un même film peut figurer dans plusieurs fichiers),
-- distinct_movies les id distincts.
select
    release_year,
    original_language,
    count(*) as movie_rows,
    count(distinct id) as distinct_movies,
    avg(vote_average) as avg_vote_average,
    sum(vote_count)::bigint as total_vote_count
from {{ ref('stg_movies__current') }}
group by release_year, original_language
