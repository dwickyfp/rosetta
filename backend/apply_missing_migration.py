
import sys
import os

# Add backend to path
sys.path.append(os.path.join(os.getcwd(), 'app'))

from sqlalchemy import text
from app.core.database import db_manager

def apply_migration():
    print("Initializing DB Manager...")
    db_manager.initialize()
    
    sql = """
    CREATE TABLE IF NOT EXISTS pipelines_progress (
        id SERIAL PRIMARY KEY,
        pipeline_id INTEGER NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
        progress INTEGER NOT NULL DEFAULT 0, -- 0 to 100
        step VARCHAR(255), -- current step description e.g. "Creating Landing Table"
        status VARCHAR(20) NOT NULL DEFAULT 'PENDING', -- 'PENDING', 'IN_PROGRESS', 'COMPLETED', 'FAILED'
        details TEXT, -- JSON or text details about the progress
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
    );
    
    CREATE INDEX IF NOT EXISTS idx_pipelines_progress_pipeline_id ON pipelines_progress(pipeline_id);
    """
    
    print("Executing SQL...")
    try:
        with db_manager.get_session_context() as session:
            session.execute(text(sql))
            session.commit()
        print("Migration applied successfully.")
    except Exception as e:
        print(f"Migration failed: {e}")
    finally:
        db_manager.close()

if __name__ == "__main__":
    apply_migration()
