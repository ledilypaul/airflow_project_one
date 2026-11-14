{#
  Un état courant ne garde qu'un instantané par fichier source, et un
  instantané (lake_file_id = file_list.id) n'appartient qu'à un fichier.
  Renvoie les fichiers ou instantanés qui violent l'une des deux règles.
#}
{% test one_snapshot_per_source_file(model) %}
select 'lake_source_file' as key_column, lake_source_file::varchar as key_value
from {{ model }}
group by lake_source_file
having count(distinct lake_file_id) > 1
union all
select 'lake_file_id', lake_file_id::varchar
from {{ model }}
group by lake_file_id
having count(distinct lake_source_file) > 1
{% endtest %}
