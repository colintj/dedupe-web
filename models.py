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
    human_filename = Column(String)
    destructive = Column(Boolean)
    user_agent = Column(String)
    ip_address = Column(String)
    uploaded_date = Column(DateTime)
    status = Column(String)
    csv_header = Column(String)
    fields = relationship('Field', backref='dedupe_session')
    training_pairs = relationship('Training', backref='dedupe_session')
    statuses = relationship('Status', backref='dedupe_session')

    def __repr__(self):
        return '<DedupeSession %r>' % self.id

class Status(Base):
    __tablename__ = 'dedupe_status'
    id = Column(Integer, primary_key=True)
    session_id = Column(Integer, ForeignKey('dedupe_session.id'))
    status = Column(String)
    message = Column(String)

    def __repr__(self):
        return '<Status %r: %r>' % (self.status, self.message)

class Field(Base):
    __tablename__ = 'dedupe_field'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    session_id = Column(Integer, ForeignKey('dedupe_session.id'))

    def __repr__(self):
        return '<Field %r>' % self.name

class Training(Base):
    __tablename__ = 'dedupe_training'
    id = Column(Integer, primary_key=True)
    session_id = Column(Integer, ForeignKey('dedupe_session.id'))
    pair_left = Column(Text)
    pair_right = Column(Text)
    match = Column(Boolean)
    timestamp = Column(DateTime)

    def __repr__(self):
        return '<Training %r>' % self.id

if __name__ == '__main__':
    import os
    from sqlalchemy import create_engine
    engine = create_engine('sqlite:///deduper.db')
    Base.metadata.create_all(engine)
