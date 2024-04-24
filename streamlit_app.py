import streamlit as st
#from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col
from snowflake.snowpark.functions import when_matched

# Write directly to the app
st.title(":cup_with_straw: 待处理的订单 :cup_with_straw:")
st.write(
    """请帮忙准备订单里面的水果哦！
    """
)

#session = get_active_session()
cnx=st.connection("snowflake")
session=cnx.session()
my_dataframe = session.table("smoothies.public.orders")
#editable_df = st.experimental_data_editor(my_dataframe);
#st.dataframe(data=my_dataframe, use_container_width=True);
if my_dataframe:
    editable_df=st.data_editor(
        my_dataframe,
        column_config={
        "ORDER_FILLED": st.column_config.CheckboxColumn(
            "ORDER_FILLED",
            help="现在准备了吗?",
            default=True,
        )
    });

submitted= st.button("订单完成！")
try:
    if submitted:
            st.success("您的水果准备好了！",icon="👍")
            og_dataset = session.table("smoothies.public.orders")
            edited_dataset = session.create_dataframe(editable_df)
            og_dataset.merge(edited_dataset
                     , (og_dataset['name_on_order'] == edited_dataset['name_on_order'])
                     , [when_matched().update({'ORDER_FILLED': edited_dataset['ORDER_FILLED']})]
                    );
except:
        st.write(" 很抱歉，程序出错!")
else:
        st.success('订单没有更新!');
    
