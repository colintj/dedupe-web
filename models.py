import os
from sqlalchemy import Column, Integer, String, Text, DateTime, Table,\
    ForeignKey, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref

Base = declarative_base()

class DedupeSession(Base):
    __tablename__ = 'dedupe_session'
    id = Column(Integer, primary_key=True)
    file_path = Column(String)
    training_file_path = Column(String)
    config_file_path = Column(String)
    field_definitions = Column(String)
    human_filename = Column(String)
    destructive = Column(Boolean)
    user_agent = Column(String)
    ip_address = Column(String)
    uploaded_date = Column(DateTime)
    status = Column(String)
    csv_header = Column(String)

    def __repr__(self):
        return '<DedupeSession %r>' % self.id

if __name__ == '__main__':
    import os
    from sqlalchemy import create_engine
    engine = create_engine('sqlite:///deduper.db')
    Base.metadata.create_all(engine)
