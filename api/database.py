import os
import logging
from typing import Generator
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase, Session
from dotenv import load_dotenv

# 1. Systematic Logging instead of silent failure
logger = logging.getLogger(__name__)

load_dotenv()

# 2. Validation: Ensure production resilience by checking variables before connecting
def get_url():
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASS')
    host = os.getenv('DB_HOST')
    name = os.getenv('DB_NAME')
    
    if not all([user, password, host, name]):
        logger.error("Database environment variables are missing!")
        raise EnvironmentError("Missing DB credentials in .env file")
        
    return f"postgresql://{user}:{password}@{host}/{name}"

try:
    DATABASE_URL = get_url()
    # 3. Connection Pooling: Adding 'pool_pre_ping' ensures stale connections are cleared
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
except Exception as e:
    logger.critical(f"Could not initialize database engine: {e}")
    raise

# 4. Modern SQLAlchemy 2.0 Base Class
class Base(DeclarativeBase):
    pass

# 5. Type Hinting for better PR-based collaboration/readability
def get_db() -> Generator[Session, None, None]:
    """Dependency for FastAPI to provide a database session per request."""
    db = SessionLocal()
    try:
        yield db
    except Exception as e:
        logger.error(f"Database session error: {e}")
        raise
    finally:
        db.close()