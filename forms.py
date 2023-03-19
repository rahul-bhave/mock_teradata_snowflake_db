from wtforms import Form, StringField, SelectField

class QueryForm(Form):
    select_keyword = SelectField('Select Keyword', choices=[('SELECT', 'SELECT')])
    select_column = StringField('Select Column(s)')
    from_keyword = SelectField('From Keyword', choices=[('FROM', 'FROM')])
    from_table = StringField('Table Name(s)')
    join_type = SelectField('Join Type', choices=[('',''),('INNER JOIN', 'INNER JOIN'), ('LEFT JOIN', 'LEFT JOIN'), ('RIGHT JOIN', 'RIGHT JOIN'), ('FULL OUTER JOIN', 'FULL OUTER JOIN')])
    join_table = StringField('Join Table Name')
    join_condition = StringField('Join Condition')
    where_keyword = SelectField('Where Keyword', choices=[('',''),('WHERE', 'WHERE')])
    where_condition = StringField('Where Condition')
    group_by_keyword = SelectField('Group By Keyword', choices=[('',''),('GROUP BY', 'GROUP BY')])
    group_by_column = StringField('Group By Column(s)')
    order_by_keyword = SelectField('Order By Keyword', choices=[('',''),('ORDER BY', 'ORDER BY')])
    order_by_column = StringField('Order By Column(s)')
    limit_keyword = SelectField('Limit Keyword', choices=[('',''),('LIMIT', 'LIMIT')])
    limit_value = StringField('Limit Value')