from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import hash

# Construct the ORM for the SQLite database.
Base = declarative_base()
class Datum(Base):
    __tablename__ = 'data'
    
    # Our table has three columns:
    key = Column(String, primary_key=True)
    value = Column(String) # TODO could specify string lengths by String(100) or similar
    refCount = Column(Integer)
    

# Backend that uses a local SQLite database with one table. The table has
# three columns: key, value, and refcount.
class SQLiteBackend:
    # filename is the local file for the SQLite database.
    def __init__(self, filename):
        # Open a connection to the database.
        engine = create_engine('sqlite:///:memory:', echo=True)
        Session = sessionmaker(bind=engine)
        self.session = Session()

    def __del__(self):
        # TODO
        pass

    def put(self, value):
        key = hash.generateKey(value)
        
        # TODO
        
        self.session.commit()
        pass

    def get(self, key):
        datum = self.session.query(Datum).filter_by(key=key).first()
        return datum.value

    def incRefCount(self, key):
        # TODO
        self.session.commit()
        pass
        
    def decRefCount(self, key):
        # TODO
        self.session.commit()
        pass