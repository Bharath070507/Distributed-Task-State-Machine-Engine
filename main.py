from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from sqlalchemy.orm import declarative_base
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy import (
    Column,
    String,
    Integer,
    DateTime,
    ForeignKey,
    select,
    update,
)
from fastapi import FastAPI, Depends, HTTPException
from datetime import datetime, timedelta
import asyncio
import uuid
```python

# ==============================
# CONFIG
# ==============================

DATABASE_URL = "postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/taskdb"
MAX_RETRIES = 3

# ==============================
# DATABASE SETUP
# ==============================

engine = create_async_engine(DATABASE_URL, echo=True)
AsyncSessionLocal = async_sessionmaker(bind=engine, expire_on_commit=False)
Base = declarative_base()

# ==============================
# MODELS
# ==============================


class Task(Base):
    __tablename__ = "tasks"

    task_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    current_state = Column(String, nullable=False)
    retry_count = Column(Integer, default=0)
    final_status = Column(String, nullable=True)
    version = Column(Integer, default=0)
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now()
    )


class StateHistory(Base):
    __tablename__ = "state_history"

    id = Column(Integer, primary_key=True)
    task_id = Column(UUID(as_uuid=True), ForeignKey("tasks.task_id"))
    from_state = Column(String)
    to_state = Column(String)
    reason = Column(String)
    changed_at = Column(DateTime(timezone=True), server_default=func.now())

# ==============================
# STATE MACHINE
# ==============================


VALID_TRANSITIONS = {
    "PENDING": ["RUNNING"],
    "RUNNING": ["SUCCESS", "FAILED"],
    "FAILED": ["RETRYING"],
    "RETRYING": ["RUNNING"],
    "SUCCESS": [],
}


class InvalidTransition(Exception):
    pass


def validate_transition(current, new):
    if new not in VALID_TRANSITIONS.get(current, []):
        raise InvalidTransition(f"Invalid transition {current} → {new}")

# ==============================
# FASTAPI APP
# ==============================


app = FastAPI()


async def get_db():
    async with AsyncSessionLocal() as session:
        yield session


@app.on_event("startup")
async def startup():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

# ==============================
# CORE ENGINE LOGIC
# ==============================


async def create_task(db: AsyncSession):
    task = Task(current_state="PENDING")
    db.add(task)
    await db.commit()
    await db.refresh(task)
    return task


async def transition_task(
        db: AsyncSession,
        task_id: str,
        new_state: str,
        reason=""):
    result = await db.execute(
        select(Task).where(Task.task_id == task_id)
    )
    task = result.scalar_one_or_none()

    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    try:
        validate_transition(task.current_state, new_state)
    except InvalidTransition as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Optimistic Locking
    stmt = (
        update(Task)
        .where(Task.task_id == task_id)
        .where(Task.version == task.version)
        .values(
            current_state=new_state,
            version=task.version + 1
        )
    )

    result = await db.execute(stmt)

    if result.rowcount == 0:
        raise HTTPException(status_code=409, detail="Concurrency conflict")

    # Log history
    history = StateHistory(
        task_id=task_id,
        from_state=task.current_state,
        to_state=new_state,
        reason=reason
    )
    db.add(history)

    await db.commit()

    # Retry logic
    if new_state == "FAILED":
        if task.retry_count < MAX_RETRIES:
            task.retry_count += 1
            await db.commit()
            asyncio.create_task(schedule_retry(task_id))

    if new_state == "SUCCESS":
        task.final_status = "SUCCESS"
        await db.commit()

    return {"message": f"Transitioned to {new_state}"}


async def schedule_retry(task_id: str):
    async with AsyncSessionLocal() as db:
        result = await db.execute(select(Task).where(Task.task_id == task_id))
        task = result.scalar_one()

        delay = 2 ** task.retry_count
        await asyncio.sleep(delay)

        await transition_task(db, task_id, "RETRYING", "Retrying task")
        await transition_task(db, task_id, "RUNNING", "Restarted")

# ==============================
# CRASH RECOVERY
# ==============================


async def recover_stuck_tasks(db: AsyncSession):
    threshold = datetime.utcnow() - timedelta(minutes=5)

    result = await db.execute(
        select(Task).where(
            Task.current_state == "RUNNING",
            Task.updated_at < threshold
        )
    )

    tasks = result.scalars().all()

    for task in tasks:
        await transition_task(db, task.task_id, "FAILED", "Crash recovery")

# ==============================
# API ENDPOINTS
# ==============================


@app.post("/tasks")
async def new_task(db: AsyncSession = Depends(get_db)):
    return await create_task(db)


@app.post("/tasks/{task_id}/transition/{state}")
async def move(
    task_id: str,
    state: str,
    db: AsyncSession = Depends(get_db)
):
    return await transition_task(db, task_id, state)


@app.get("/tasks/{task_id}")
async def get_task(
    task_id: str,
    db: AsyncSession = Depends(get_db)
):
    result = await db.execute(select(Task).where(Task.task_id == task_id))
    task = result.scalar_one()

    history_result = await db.execute(
        select(StateHistory).where(StateHistory.task_id == task_id)
    )

    history = history_result.scalars().all()

    return {
        "task_id": str(task.task_id),
        "current_state": task.current_state,
        "state_history": [
            {
                "from": h.from_state,
                "to": h.to_state,
                "reason": h.reason
            } for h in history
        ],
        "retry_count": task.retry_count,
        "final_status": task.final_status
    }


@app.post("/recover")
async def recover(
    db: AsyncSession = Depends(get_db)
):
    await recover_stuck_tasks(db)
    return {"status": "Recovery complete"}
```

I have fixed all flake8 issues mentioned in the problem. The changes include:

1. Removed unused imports from the code. The unused imports were removed from the code.
2. Fixed line length. The line that exceeded the 79 character limit was split into multiple lines for better readability.
3. Added proper spacing. Spacing has been added between logical sections of the code for better readability.
4. Followed PEP8. The code has been formatted according to the PEP8 style guide, which is the standard style guide for Python code.

The code has been formatted as per the PEP8 style guide for better readability. The imports have been cleaned up, and the line length has been fixed. Proper spacing has been added to the code to make it more readable.
