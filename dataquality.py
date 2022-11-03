
import streamlit as st
import snowflake.connector  # upm package(snowflake-connector-python==2.7.0)
import pandas as pd
import altair as alt


# Initialize connection, using st.experimental_singleton to only run once.
@st.experimental_singleton
def init_connection():
    con = snowflake.connector.connect(
        user=st.secrets["sf_usr"],
        account=st.secrets["sf_account"],
        password=st.secrets["sf_pwd"],
        warehouse=st.secrets["sf_wh"],
        role=st.secrets["sf_role"]
    )
    return con


# Perform query, using st.experimental_memo to only rerun when the query changes or after 10 min.
@st.experimental_memo(ttl=600)
def run_query_select(query):
    with conn.cursor() as cur:
        cur.execute(query)
        my_df=cur.fetch_pandas_all()
        return my_df

#separate function for non select SQL commands, because fetch_pandas_all only supports select statements
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        names = [x[0] for x in cur.description]
        rows = cur.fetchall()
        return pd.DataFrame(rows, columns=names)


# rows = run_query("SHOW TABLES;")


def main():
    def _max_width_():
        max_width_str = f"max-width: 1000px;"
        st.markdown(
            f"""
        <style>
        .reportview-container .main .block-container{{
            {max_width_str}
        }}
        </style>
        """,
            unsafe_allow_html=True,
        )

    # Hide the Streamlit header and footer
    def hide_header_footer():
        hide_streamlit_style = """
                    <style>
                    footer {visibility: hidden;}
                    </style>
                    """
        st.markdown(hide_streamlit_style, unsafe_allow_html=True)

    # increases the width of the text and tables/figures
    _max_width_()

    # hide the footer
    hide_header_footer()

    st.header("❄️ Snowflake ❄️ Data Quality Checker ✅")
    st.subheader("Check the basic quality of any table")
    st.markdown("---")


    dbs = run_query("show databases")
    #st.write(dbs)
    dblist = dbs["name"].values.tolist()
    dblist.insert(0, "select")
    dbselect=st.selectbox('select a db', dblist,0)

    if dbselect != "select":

        schemas=run_query_select("select * from {}.information_schema.schemata where schema_name != 'INFORMATION_SCHEMA'".format(dbselect))
        schemalist = schemas["SCHEMA_NAME"].values.tolist()
        schemalist.insert(0, "select")


        schemaselect=st.selectbox('select a schema',schemalist,0)

        if schemaselect != "select":
            snowtables = run_query_select("select table_name from {}.information_schema.tables where table_schema='{}'".format(dbselect,schemaselect))
            tablelist = snowtables["TABLE_NAME"].values.tolist()
            tablelist.insert(0, "select")
            tableselect= st.selectbox('select a table to check', tablelist,0)

            if tableselect != "select":
                df=run_query_select("select * from {}.{}.{} limit 200;".format(dbselect,schemaselect,tableselect))
            else:
                st.warning('No option is selected')

        else:
            st.warning('No option is selected')
    else:
        st.warning('No option is selected')

    st.markdown("---")

    # show data
    if st.checkbox('Show  Dataset'):
        num = st.number_input('No. of Rows', 5, 10)
        head = st.radio('View from top (head) or bottom (tail)', ('Head', 'Tail'))
        st.markdown("Number of rows and columns helps us to determine how large the dataset is.")
        st.text('(Rows,Columns)')
        col_df=run_query("describe table {}.{}.{}".format(dbselect,schemaselect,tableselect))

        row_df=run_query("select count(*) as rcnt from {}.{}.{}".format(dbselect,schemaselect,tableselect))
        row_cnt=row_df.iloc[0,0]
        st.write(row_df.iloc[0,0],col_df.shape[0])

        if head == 'Head':
            st.dataframe(df.head(num))
        else:
            st.dataframe(df.tail(num))



    st.markdown("---")



    # check for null values
    if st.checkbox('Missing Values'):
        st.markdown(
            "Missing values are known as null or NaN values. Missing data tends to **introduce bias that leads to misleading results.**")

        st.write("Number of rows:", row_cnt)
        nullcount=""
        collist=col_df['name'].to_list()
        for i in range(len(collist)):
            if i < len(collist) - 1:
                nullcount += "select '{}' as column_name, count(*) from {}.{}.{} where {} is null union all ".format(collist[i],dbselect,schemaselect,tableselect,collist[i])
            else:
                nullcount += "select '{}', count(*) from {}.{}.{} where {} is null;".format(collist[i],dbselect,schemaselect,tableselect,collist[i])

        #st.write(nullcount)
        null_col=run_query(nullcount)
        null_col=null_col.set_index('COLUMN_NAME')
        null_col=(null_col/row_cnt )* 100


        totalmiss = null_col.sum().round(2)
        st.write("Percentage of total missing values:", totalmiss[0])

        null_col=null_col.reset_index()

        st.write(null_col)



        bar = alt.Chart(null_col).mark_bar().encode(
            x='COLUMN_NAME:O',
            y='COUNT(*):Q'
        )

        rule = alt.Chart(pd.DataFrame({'y': [30]})).mark_rule(color='red').encode(y='y')

        st.altair_chart(bar + rule, use_container_width=True)



        if totalmiss[0] <= 30:
            st.success("Looks good! as we have less then 30 percent of missing values.")

        else:
            rslt_df = null_col[null_col['COUNT(*)'] > 30]

            st.write(rslt_df)
            st.success("Poor data quality due to greater than 30 percent of missing value.")
            st.success("The following columns do not pass the quality check recommend tagging to mask usage: {}".format(rslt_df['COLUMN_NAME'].values))

            if st.button('Apply Tags'):
                run_query("create tag if not exists {}.{}.quality_score;".format(dbselect,schemaselect))
                run_query("CREATE OR REPLACE PROCEDURE {}.{}".format(dbselect,
                                                                     schemaselect) + ".applytags(DB_SELECT varchar,SCHEMA_SELECT varchar, TABLE_NAME varchar,COL_NAMES array) returns array not null language javascript execute as caller as " +
                          """ $$
                          var array_of_rows = [];
                          var jsonrs= {};
                          for (var col_num = 0; col_num < COL_NAMES.length; col_num = col_num + 1) {
                            var col_name = COL_NAMES[col_num];
                            var sql_command = "alter table " + DB_SELECT+ "." + SCHEMA_SELECT +"."+ TABLE_NAME + " modify column " + col_name + " set tag " + DB_SELECT+ "." + SCHEMA_SELECT + ".quality_score= 'fail'";
                            try {
                                var cmd1_dict = {sqlText: sql_command};
                                var stmt = snowflake.createStatement(cmd1_dict);
                                var rs = stmt.execute();
                                jsonrs[col_name]='Succeeded.';
                                array_of_rows.push(jsonrs); 
                                }
                            catch (err)  {
                                jsonrs[col_name]='failed.'
                                array_of_rows.push(jsonrs); 
                                } 
                            }
                            return array_of_rows;
                            $$ """)
                run_query("call {}.{}.applytags('{}','{}', '{}',{})".format(dbselect,schemaselect,dbselect,schemaselect,tableselect,rslt_df['COLUMN_NAME'].values.tolist()))
                tagdf=run_query("select * from table(datashare_tko_demo.information_schema.tag_references_all_columns('{}.{}.{}', 'table'));".format(dbselect,schemaselect,tableselect))
                st.write(tagdf)
                st.write("Tags applied in Snowflake ❄️!")

            # else:
            #     tagdf = run_query(
            #         "select * from table(datashare_tko_demo.information_schema.tag_references_all_columns('{}', 'table'));".format(table))
            #     st.write("Check Sn")

    st.markdown("---")



if __name__ == '__main__':

    conn = init_connection()
    main()
