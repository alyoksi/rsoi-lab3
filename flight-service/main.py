import os
from datetime import datetime, timezone

from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey, text
from sqlalchemy.orm import declarative_base, sessionmaker, relationship, Session

DB_URL = os.getenv("DB_URL", "postgresql://program:test@localhost:5432/flights")
engine = create_engine(DB_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class Airport(Base):
    __tablename__ = "airport"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255))
    city = Column(String(255))
    country = Column(String(255))

class Flight(Base):
    __tablename__ = "flight"
    id = Column(Integer, primary_key=True, index=True)
    flight_number = Column(String(20), nullable=False)
    datetime = Column(DateTime(timezone=True), nullable=False)
    from_airport_id = Column(Integer, ForeignKey("airport.id"))
    to_airport_id = Column(Integer, ForeignKey("airport.id"))
    price = Column(Integer, nullable=False)

    from_airport = relationship("Airport", foreign_keys=[from_airport_id])
    to_airport = relationship("Airport", foreign_keys=[to_airport_id])

Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

app = FastAPI(title="Flight Service")

# The Postman collection uses AFL031 from the first page.
AIRPORTS = {
    "SVO": ("Шереметьево", "Москва"),
    "LED": ("Пулково", "Санкт-Петербург"),
    "KZN": ("Казань", "Казань"),
    "SVX": ("Кольцово", "Екатеринбург"),
    "OVB": ("Толмачёво", "Новосибирск"),
    "AER": ("Сочи", "Сочи"),
}
FLIGHTS = (
    ("AFL031", "LED", "SVO", "2021-10-08 20:00", 1500),
    ("DEMO101", "SVO", "LED", "2027-01-15 07:10", 3900),
    ("DEMO102", "SVO", "KZN", "2027-01-15 08:30", 4500),
    ("DEMO103", "KZN", "SVO", "2027-01-15 10:15", 4400),
    ("DEMO104", "SVO", "SVX", "2027-01-15 09:00", 6200),
    ("DEMO105", "SVX", "SVO", "2027-01-15 13:20", 6500),
    ("DEMO106", "SVO", "OVB", "2027-01-15 06:50", 8900),
    ("DEMO107", "OVB", "SVO", "2027-01-15 15:15", 9200),
    ("DEMO108", "LED", "AER", "2027-01-15 11:40", 7100),
    ("DEMO109", "AER", "LED", "2027-01-15 16:55", 7400),
    ("DEMO110", "KZN", "SVX", "2027-01-16 05:30", 5100),
    ("DEMO111", "SVX", "OVB", "2027-01-16 12:10", 5700),
    ("DEMO112", "OVB", "AER", "2027-01-16 09:25", 10500),
    ("DEMO113", "AER", "SVO", "2027-01-16 17:00", 6600),
)


def advance_id_sequence(db: Session, model):
    # Lab 2 inserted explicit IDs without advancing PostgreSQL's sequences.
    table = model.__tablename__
    db.execute(text(f"""
        SELECT setval(pg_get_serial_sequence('{table}', 'id'), GREATEST(
            (SELECT MAX(id) FROM {table}),
            nextval(pg_get_serial_sequence('{table}', 'id'))
        ))
    """))


@app.on_event("startup")
def seed_data():
    with SessionLocal.begin() as db:
        postgres = engine.dialect.name == "postgresql"
        if postgres:
            db.execute(text("LOCK TABLE airport, flight IN SHARE ROW EXCLUSIVE MODE"))

        existing_airports = {
            (airport.name, airport.city, airport.country): airport
            for airport in db.query(Airport).all()
        }
        airports = {}
        for code, (name, city) in AIRPORTS.items():
            airport = existing_airports.get((name, city, "Россия"))
            if airport is None:
                airport = Airport(name=name, city=city, country="Россия")
                db.add(airport)
            airports[code] = airport

        if postgres and existing_airports and db.new:
            advance_id_sequence(db, Airport)
        db.flush()

        existing_flights = {number for (number,) in db.query(Flight.flight_number)}
        if postgres and existing_flights and any(number not in existing_flights for number, *_ in FLIGHTS):
            advance_id_sequence(db, Flight)
        for number, origin, destination, departure, price in FLIGHTS:
            if number not in existing_flights:
                db.add(Flight(
                    flight_number=number,
                    datetime=datetime.fromisoformat(departure).replace(tzinfo=timezone.utc),
                    from_airport_id=airports[origin].id,
                    to_airport_id=airports[destination].id,
                    price=price,
                ))


@app.get("/manage/health")
def health():
    return {"status": "OK"}

@app.get("/api/v1/flights")
def get_flights(page: int = Query(1, ge=1), size: int = Query(10, ge=1, le=100), db: Session = Depends(get_db)):
    query = db.query(Flight)
    total = query.count()
    offset = (page - 1) * size
    flights = query.order_by(Flight.id).offset(offset).limit(size).all()

    items = []
    for f in flights:
        items.append({
            "flightNumber": f.flight_number,
            "fromAirport": f"{f.from_airport.city} {f.from_airport.name}",
            "toAirport": f"{f.to_airport.city} {f.to_airport.name}",
            "date": f.datetime.strftime("%Y-%m-%d %H:%M"),
            "price": f.price
        })
    return {
        "page": page,
        "pageSize": size,
        "totalElements": total,
        "items": items
    }

@app.get("/api/v1/flights/{flight_number}")
def get_flight(flight_number: str, db: Session = Depends(get_db)):
    f = db.query(Flight).filter(Flight.flight_number == flight_number).first()
    if not f:
        raise HTTPException(status_code=404, detail="Flight not found")
    return {
        "flightNumber": f.flight_number,
        "fromAirport": f"{f.from_airport.city} {f.from_airport.name}",
        "toAirport": f"{f.to_airport.city} {f.to_airport.name}",
        "date": f.datetime.strftime("%Y-%m-%d %H:%M"),
        "price": f.price
    }