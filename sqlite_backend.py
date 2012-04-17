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
    
    def __init__(self, key, value):
        self.key = key
        self.value = value
        self.refCount = 1
    

# Backend that uses a local SQLite database with one table. The table has
# three columns: key, value, and refCount. We assume single-threaded use
# of this backend.
class SQLiteBackend:
    # filename is the local file for the SQLite database.
    def __init__(self, filename):
        # Open a connection to the database.
        engine = create_engine('sqlite:///:memory:', echo=True)
        Base.metadata.create_all(engine)
        
        Session = sessionmaker(bind=engine)
        self.session = Session()

    def __del__(self):
        self.session.close()
    
    # Returns None if there is no datum with the given key.
    def getDatum(self, key):
        return self.session.query(Datum).filter_by(key=key).first()

    def put(self, value):
        key = hash.generateKey(value)
        
        # Query the datum, if it already exists.
        datum = self.getDatum(key)
        if datum is None:
            # This key isn't in use yet. Set the refCount to 1.
            self.session.add(Datum(key, value))
        else:
            # This key has already been written to, so just increment the
            # reference count.
            datum.refCount += 1
        
        self.session.commit()
        return key

    def get(self, key):
        datum = self.getDatum(key)
        if datum is None:
            return None
        else:
            return datum.value

    def incRefCount(self, key):
        self.getDatum(key).refCount += 1
        self.session.commit()
        
    def decRefCount(self, key):
        datum = self.getDatum(key)
        datum.refCount -= 1
        
        # Garbage collection.
        if datum.refCount == 0:
            self.session.delete(datum)
        
        self.session.commit()
