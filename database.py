from sqlalchemy import create_engine
db_string = "postgresql://postgres:sjvnfi_LFMR740@internal.cfnsbden5utu.us-east-2.rds.amazonaws.com:5432/"
con = create_engine(db_string,pool_size=10, max_overflow=20)