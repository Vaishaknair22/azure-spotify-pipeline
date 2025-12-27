# Databricks notebook source
parameters = [

    {
        "table": "spotify_catalog.silver.factstream",
        "alias" : "factstream",
        "cols": "factstream.stream_id,factstream.listen_duration"     
    },

        {
        "table": "spotify_catalog.silver.dimuser",
        "alias" : "dimuser",
        "cols": "dimuser.user_id, dimuser.user_name"  ,
        "condition": "dimuser.user_id = factstream.user_id  "     
    },
    
        {
        "table": "spotify_catalog.silver.dimtrack",
        "alias" : "dimtrack",
        "cols": "dimtrack.track_id , dimtrack.track_name",
        "condition":  "dimtrack.track_id = factstream.track_id "  
    }
]

# COMMAND ----------

pip install jinja2

# COMMAND ----------

from jinja2 import Template

# COMMAND ----------

query = """
    SELECT 
    {% for param in parameters %}
        {{param.cols}}
        {% if not loop.last %}
        ,
        {% endif %}
    {% endfor %}
    FROM
    {% for param in parameters %}
      {% if loop.first %}
        {{param['table']}} AS {{param.alias}}
      {% else %}
    LEFT JOIN
        {{param['table']}} AS {{param.alias}}
    ON
        {{ param['condition'] }}
      {% endif %}
    {% endfor %}
"""

# COMMAND ----------

jinja_sql = Template(query)
query_sql=jinja_sql.render(parameters=parameters)


# COMMAND ----------

display(spark.sql(query_sql))

# COMMAND ----------

