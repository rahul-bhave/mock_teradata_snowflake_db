# app.py
from flask import Flask, render_template, redirect, url_for, flash
from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField, SelectField
from wtforms.validators import DataRequired
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from query_builder import QueryBuilder
from config import Config

import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

app = Flask(__name__)
#app.config['SECRET_KEY'] = 'mysecretkey'
#app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///test.db'
#app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config.from_object('config.DevelopmentConfig')
db = SQLAlchemy(app)

class Config(db.Model):
    __tablename__ = 'config'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(50))
    value = db.Column(db.String(255))

class QueryForm(FlaskForm):
    select = StringField('Select', validators=[DataRequired()])
    from_table = SelectField('From', choices=[], validators=[DataRequired()])
    where = StringField('Where')
    submit = SubmitField('Execute')

def get_tables():
    engine = create_engine(app.config['SQLALCHEMY_DATABASE_URI'])
    Base = automap_base()
    Base.prepare(engine, reflect=True)
    tables = []
    for table in Base.classes:
        tables.append(table.__table__.name)
    return tables

def get_table_columns(table):
    engine = create_engine(app.config['SQLALCHEMY_DATABASE_URI'])
    Base = automap_base()
    Base.prepare(engine, reflect=True)
    table_class = Base.classes[table]
    columns = []
    for column in table_class.__table__.columns:
        columns.append(column.name)
    return columns

def create_db():
    db.create_all()
    db.session.commit()
    config = Config(name='db_version', value='1')
    db.session.add(config)
    db.session.commit()

def upgrade_db():
    config = Config.query.filter_by(name='db_version').first()
    if not config:
        create_db()

@app.route('/', methods=['GET', 'POST'])
def index():
    form = QueryForm()
    form.from_table.choices = [(table, table) for table in get_tables()]
    if form.validate_on_submit():
        query_builder = QueryBuilder(form.select.data, form.from_table.data, form.where.data)
        query = query_builder.build_query()
        try:
            engine = create_engine(app.config['SQLALCHEMY_DATABASE_URI'])
            conn = engine.connect()
            result = conn.execute(query).fetchall()
            conn.close()
        except Exception as e:
            flash(str(e))
            return redirect(url_for('index'))
        return render_template('query_result.html', result=result, columns=query_builder.get_columns())
    return render_template('index.html', form=form)

if __name__ == '__main__':
    upgrade_db()
    app.run(debug=True)