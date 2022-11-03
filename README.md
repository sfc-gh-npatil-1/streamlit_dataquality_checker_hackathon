# streamlit_dataquality_checker_hackathon


## Goal: Check your snowflake tables for data quality

### Implementation: 

Checks snowflake table for missing data. Current threshold for percent of null values in table is 30 percent. Clicking Apply Tags button creates a quality_score tag in snowflake and assigns to columns that have > 30 percent null. (Note tags cannot be applied to datashare objects)


### Prerequistes:

1. Update .streamlit/secrets.toml file with your snowflake connection details. App requires a role with priveleges to create a javascript procedure and add tags in the selected schema if you want to apply tags.
2. pip install -r requirements.txt
3. streamlit run dataquality.py


### Demo

https://user-images.githubusercontent.com/100793807/199849856-c5774aed-d413-4ff5-ad49-f866fc4706c5.mov

