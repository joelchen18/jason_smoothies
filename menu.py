import streamlit as st
#from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col


# Write directly to the app
st.title(":cup_with_straw: 来定制您的果蔬吧！:cup_with_straw:")
st.write(
    """小朋友们，来选择你喜欢的水果，吃吃吃!
    """
)

cnx=st.connection("snowflake")
session=cnx.session()
name_on_order = st.text_input('小朋友的名字是？')
st.write('请填写您的可爱名字:', name_on_order)

#session = get_active_session()
#my_dataframe = session.table("smoothies.public.fruit_options").select(col('FRUIT_NAME'))
#st.dataframe(data=my_dataframe, use_container_width=True)

session = cnx
my_dataframe = session.table("smoothies.public.fruit_options").select(col('FRUIT_NAME'))
#st.dataframe(data=my_dataframe, use_container_width=True)
ingredients_list=st.multiselect("最多选择五个哦，不然会吃不下晚饭呢:", my_dataframe);

#st.multiselect("Choose up to 5 ingredients:", my_dataframe);

ingredients_string=''
for fruit_chosen in ingredients_list:
    ingredients_string+=fruit_chosen+" "

#st.write(ingredients_string)
#st.text(ingredients_string);

#my_insert_stmt = """ insert into smoothies.public.orders(ingredients) values ('""" + ingredients_string + """')"""
#st.write(my_insert_stmt);
my_insert_stmt = """ insert into smoothies.public.orders(ingredients,name_on_order) 
values ('""" + ingredients_string + """','"""+ name_on_order +"""')"""

time_to_insert=st.button("赶快点我，提交订单吧！")
if time_to_insert:
    session.sql(my_insert_stmt).collect()
    st.success('亲爱的小朋友们，好的，您的订单已收到！', icon="✅")
