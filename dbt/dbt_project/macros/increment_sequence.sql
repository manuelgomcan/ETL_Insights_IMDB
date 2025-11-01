/*
  MACRO OBSOLETA: Esta macro ya no se utiliza porque hemos reemplazado el uso de secuencias
  por la función generate_surrogate_key() de dbt_utils, que genera claves subrogadas
  deterministas basadas en valores de columnas naturales.
*/

{%- macro increment_sequence() -%}
  
  -- Esta macro ya no se usa, pero se mantiene para compatibilidad con código antiguo
  -- Reemplazar con: {{ dbt_utils.generate_surrogate_key(['columna1', 'columna2']) }}
  
  -- nextval('{{ this.schema }}.{{ this.name }}_seq')
  NULL -- Devuelve NULL para evitar errores si se usa accidentalmente

{%- endmacro -%}

-- ejemplo antiguo: {{ increment_sequence() }} as movie_performance_sk
-- ejemplo nuevo: {{ dbt_utils.generate_surrogate_key(['id', 'nombre']) }} as movie_performance_sk