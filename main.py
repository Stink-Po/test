import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, MetaData, ForeignKey, Float
from sqlalchemy.orm import relationship, declarative_base, sessionmaker
# from bs4 import BeautifulSoup
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
# from requests.exceptions import SSLError, ConnectionError
# from pymysql.err import ProgrammingError
from create_db import CreateDb

create_database = CreateDb(user='root', password='tool789', database_name='iran_geo')

engine = create_engine("mysql+pymysql://root:tool789@localhost/iran_geo", echo=True)
base = declarative_base()
session = sessionmaker(bind=engine)()
meta = MetaData()
req_session = requests.Session()
retry = Retry(connect=3, backoff_factor=0.5)
adapter = HTTPAdapter(max_retries=retry)
req_session.mount('https://', adapter)
req_session.mount('https://', adapter)


class Province(base):
    __tablename__ = "province"
    id = Column(Integer, primary_key=True)
    province_name = Column(String(250), nullable=False, unique=True)
    population = Column(Integer, nullable=True)
    population_ratio = Column(Float, nullable=True)
    population_rank = Column(Integer, nullable=True)
    state = relationship("State", backref="province")


class State(base):
    __tablename__ = "state"
    id = Column(Integer, primary_key=True)
    state_name = Column(String(250), nullable=False)
    province_id = Column(Integer, ForeignKey("province.id"))
    city = relationship("City", backref="state")


class City(base):
    __tablename__ = "city"
    id = Column(Integer, primary_key=True)
    city_name = Column(String(250), nullable=False)
    state_id = Column(Integer, ForeignKey("state.id"))


base.metadata.create_all(engine)


def create_insert_city_states_provinces():
    """Insert City Names States Province To Mysql tables with one-to-many relationships."""
    province = []
    state = []

    # Create Pandas Dataframe From official data from Json file

    df = pd.read_json('Iran96-97.json')

    # Dropping unnecessary Column from Dataframe
    df.drop(['latitude', 'longitude'], axis=1, inplace=True)

    for (key, val) in df.iterrows():
        if val['province'] not in province:
            province.append(val['province'])
            new_prov = Province(province_name=val['province'])
            session.add(new_prov)
            session.commit()

    for (key, val) in df.iterrows():
        if val['state'] not in state:
            state.append(val['state'])
            prov_id = session.query(Province).filter_by(province_name=val['province']).first().id
            new_state = State(state_name=val['state'],
                              province_id=prov_id)
            session.add(new_state)
            session.commit()

    for (key, val) in df.iterrows():
        state_id = session.query(State).filter_by(state_name=val['state']).first().id
        new_city = City(city_name=val['city'],
                        state_id=state_id)
        session.add(new_city)
        session.commit()

    session.close()


create_insert_city_states_provinces()

population_df = pd.read_csv('iod-02211-provincial-population-distribution-percent-f.csv')

# Remove Old Data

population_df.drop(['۱۳۹۵-توزیع', '۱۳۹۵-رتبه',
                    '۱۳۹۶-جمعیت', '۱۳۹۶-رتبه',
                    '۱۳۹۷-جمعیت', '۱۳۹۷-رتبه',
                    '۱۳۹۸-جمعیت', '۱۳۹۸-رتبه'], axis=1, inplace=True)

total_population_province = pd.read_csv('population-country-by-province-1398-fa.csv')

province_data = session.query(Province).all()

for index, prov in enumerate(province_data):
    test = population_df.loc[population_df['استان'] == prov.province_name]
    print(test['۱۳۹۹ -جمعیت'])
    for item in test['۱۳۹۹ -رتبه']:
        print(item)
    prov.population_rank = test['۱۳۹۹ -رتبه']
    prov.population_ratio = test['۱۳۹۹ -رتبه']
    session.commit()

for (index, value) in total_population_province.iterrows():
    this_province = value['استان‌ها']
    find_province = session.query(Province).filter_by(province_name=this_province).first()
    if find_province:
        find_province.population = int(value['نفر'])
        session.commit()

session.close()

