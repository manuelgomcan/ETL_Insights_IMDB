/*
  MACRO OBSOLETA: Esta macro ya no se utiliza porque hemos reemplazado el uso de secuencias
  por la función generate_surrogate_key() de dbt_utils, que genera claves subrogadas
  deterministas basadas en valores de columnas naturales.
*/

{% macro generate_sequences() %}

    -- Esta macro está desactivada y se mantiene solo como referencia
    -- El código original estaba aquí pero ha sido eliminado para evitar problemas de compilación
    
    -- No hacer nada, simplemente retornar
    {% do return(none) %}
  
{% endmacro %}