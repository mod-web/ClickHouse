from dotenv import load_dotenv
from pydantic import Field, BaseSettings

load_dotenv()


class CHSettings(BaseSettings):
    host: str = Field(..., env='CLICKHOUSE__HOST')
    port: str = Field(..., env='CLICKHOUSE__PORT')
    db: str = Field(..., env='CLICKHOUSE__DB')
    username: str = Field(..., env='CLICKHOUSE__USER')
    password: str = Field(..., env='CLICKHOUSE__PASSWORD')

class Settings(BaseSettings):
    ch = CHSettings()

    class Config:
        env_file = '../.env'
        env_file_encoding = 'utf-8'


settings = Settings()
