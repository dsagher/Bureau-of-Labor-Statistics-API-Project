from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String, Boolean, Float, ForeignKey
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import URL
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from sqlalchemy.schema import PrimaryKeyConstraint, ForeignKeyConstraint

#----------------------------------create_dummy_data-------------------------------------------
state_results_lst = []
state_series_lst = []

state_series1 = {'seriesID': 'ABC125', 'series':'Always Be Cool', 'state':'Michigan','survey':'ABC', 'is_adjusted': True}
state_series2 = {'seriesID': 'ABC126', 'series':'Always Be Cool', 'state':'Michigan','survey':'ABC', 'is_adjusted': True}
state_series_lst.append(state_series1)
state_series_lst.append(state_series2)

stateresults1 = {'seriesID': "ABC125", 'year':2025, 'period':"M1", 'period_name':"January", 'value':1.5, 'footnotes':None}
stateresults2 = {'seriesID': "ABC125", 'year':2026, 'period':"M2", 'period_name':"February", 'value':2.2, 'footnotes':None}
state_results_lst.append(stateresults1)
state_results_lst.append(stateresults2)
#---------------------------------------------------------------------------------------------
url_object = URL.create('postgresql+psycopg2', 
                        username='danielsagher',
                        password='dsagher',
                        host='localhost',
                        database='danielsagher')
engine = create_engine(url_object, logging_name='SQLAlchemy')
metadata = MetaData()

state_series = Table('state_series', 
                     metadata, 
                     Column('seriesID', String, primary_key=True),
                     Column('series', String, nullable=False),
                     Column('state', String, nullable=False),
                     Column('survey', String),
                     Column('is_adjusted', Boolean))
national_series = Table('national_series', 
                     metadata, 
                     Column('seriesID', String, primary_key=True),
                     Column('series', String, nullable=False),
                     Column('survey', String),
                     Column('is_adjusted', Boolean))
state_results = Table('state_results', 
                     metadata, 
                     Column('seriesID', String, ForeignKey('state_series.seriesID'), primary_key=True),
                     Column('year', Integer, nullable=False, primary_key=True),
                     Column('period', String, nullable=False, primary_key=True),
                     Column('period_name',String, nullable=False),
                     Column('value', Float),
                     Column('footnotes', String))
national_results = Table('national_results', 
                     metadata, 
                     Column('seriesID', String, ForeignKey('national_series.seriesID'), primary_key=True),
                     Column('year', Integer, nullable=False, primary_key=True),
                     Column('period', String, nullable=False, primary_key=True),
                     Column('period_name',String, nullable=False),
                     Column('value', Float),
                     Column('footnotes', String))

metadata.create_all(bind=engine, checkfirst=True)
state_results_stmt = insert(state_results).values(state_results_lst).on_conflict_do_nothing()
state_series_stmt = insert(state_series).values(state_series_lst).on_conflict_do_nothing()

with engine.connect() as connect:
    connect.execute(state_series_stmt)
    connect.execute(state_results_stmt)
    connect.commit()

    